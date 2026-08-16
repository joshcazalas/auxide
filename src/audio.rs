use std::{
    collections::BTreeMap,
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult, SeekFrom},
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, TryStreamExt, stream};
use reqwest::{
    Client as HttpClient, StatusCode,
    header::{CONTENT_RANGE, HeaderMap, HeaderName, HeaderValue, RANGE},
};
use songbird::input::{
    AsyncAdapterStream, AsyncMediaSource, AudioStream, AudioStreamError, Compose, HlsRequest,
    Input, core::io::MediaSource,
};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use tokio_util::io::StreamReader;

use crate::source::{SourceResolver, TrackMetadata};

/// Largest span a single ranged request may cover.
///
/// `YouTube` refuses a media request that carries no `Range`, an open-ended
/// `bytes=N-`, or a bounded range wider than this, answering each with 403
/// Forbidden. Measured repeatedly against one freshly resolved URL: spans of
/// 1 KiB, 512 KiB, 768 KiB, and 1 MiB were served, while 1.25 MiB, 1.5 MiB,
/// 2 MiB, and the whole 3.9 MiB file were refused.
const MAX_RANGE_SPAN: u64 = 1024 * 1024;

/// Converts stable source metadata into a fresh, streaming Songbird input.
///
/// The resolver is called immediately before playback so temporary media URLs are never queued or
/// persisted. The HTTP client accepts only credential-free HTTPS redirects.
#[derive(Clone)]
pub struct AudioPipeline {
    resolver: Arc<dyn SourceResolver>,
    http: HttpClient,
    output_volume: f32,
}

impl std::fmt::Debug for AudioPipeline {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AudioPipeline")
            .finish_non_exhaustive()
    }
}

impl AudioPipeline {
    /// Builds a bounded HTTP pipeline for resolved media streams.
    ///
    /// # Errors
    ///
    /// Returns an error when the HTTP client cannot be configured.
    pub fn new(resolver: Arc<dyn SourceResolver>, output_volume: f32) -> Result<Self> {
        let http = HttpClient::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                let target = attempt.url();
                if attempt.previous().len() >= 5 {
                    attempt.error("media redirect limit exceeded")
                } else if target.scheme() != "https"
                    || !target.username().is_empty()
                    || target.password().is_some()
                {
                    attempt.error("media redirect was not credential-free HTTPS")
                } else {
                    attempt.follow()
                }
            }))
            .user_agent(concat!("auxide/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build the media HTTP client")?;
        Ok(Self {
            resolver,
            http,
            output_volume,
        })
    }

    /// Level every track starts at, from `playback.output_volume`.
    #[must_use]
    pub const fn output_volume(&self) -> f32 {
        self.output_volume
    }

    /// Resolves a queued track and creates its streaming input.
    ///
    /// # Errors
    ///
    /// Returns an error when the source cannot be refreshed, supplies invalid headers, or selects
    /// a protocol outside Auxide's allowlist.
    pub async fn prepare(&self, track: &TrackMetadata) -> Result<Input> {
        let audio = self.resolver.resolve(track).await?;
        let headers = convert_headers(&audio.headers)?;
        match audio.protocol.as_deref() {
            None | Some("https") => Ok(Input::Lazy(Box::new(ChunkedHttpRequest {
                client: self.http.clone(),
                url: audio.stream_url.to_string(),
                headers,
            }))),
            Some("m3u8" | "m3u8_native") => Ok(HlsRequest::new_with_headers(
                self.http.clone(),
                audio.stream_url.to_string(),
                headers,
            )
            .into()),
            Some(protocol) => bail!("source selected unsupported media protocol {protocol:?}"),
        }
    }
}

/// A media stream fetched as a sequence of bounded ranged requests.
///
/// Songbird's own [`songbird::input::HttpRequest`] issues one request for the
/// whole resource, which is precisely what `YouTube` refuses. It cannot be made
/// to chunk either: its adapter only retries after a read *error*, so a
/// deliberately short response would read as a clean end of track and truncate
/// playback rather than continue.
#[derive(Clone, Debug)]
struct ChunkedHttpRequest {
    client: HttpClient,
    url: String,
    headers: HeaderMap,
}

impl ChunkedHttpRequest {
    /// Streams the resource from `offset`, one [`MAX_RANGE_SPAN`] request at a time.
    ///
    /// The total length comes from the first response's `Content-Range` rather
    /// than from the caller, so it is whatever the origin actually serves. A
    /// server that ignores the range header and answers 200 is handled by
    /// taking its single response as the complete body.
    fn body(&self, offset: u64) -> impl Stream<Item = IoResult<Bytes>> + Send + Sync + use<> {
        let request = self.clone();
        stream::try_unfold(Some((offset, None)), move |state| {
            let request = request.clone();
            async move {
                let Some((offset, total)) = state else {
                    return Ok(None);
                };
                if total.is_some_and(|total| offset >= total) {
                    return Ok(None);
                }

                let last = offset.saturating_add(MAX_RANGE_SPAN - 1);
                let response = request
                    .client
                    .get(&request.url)
                    .headers(request.headers.clone())
                    .header(RANGE, format!("bytes={offset}-{last}"))
                    .send()
                    .await
                    .map_err(IoError::other)?;
                let status = response.status();
                if !status.is_success() {
                    return Err(IoError::other(format!(
                        "media request for bytes {offset}-{last} failed with status {status}"
                    )));
                }

                // A 200 means the range was ignored and the whole resource is
                // in this one response, so there is nothing left to ask for.
                let served = response.content_length();
                let next = if status == StatusCode::OK {
                    None
                } else {
                    let total = total.or_else(|| content_range_total(response.headers()));
                    match served {
                        // No progress would loop forever asking for the same bytes.
                        None | Some(0) => None,
                        Some(served) => {
                            let next = offset.saturating_add(served);
                            if total.is_some_and(|total| next >= total) {
                                None
                            } else {
                                Some((next, total))
                            }
                        }
                    }
                };

                Ok(Some((
                    response.bytes_stream().map_err(IoError::other),
                    next,
                )))
            }
        })
        .try_flatten()
    }

    async fn open(&self, offset: u64) -> Result<ChunkedHttpStream, AudioStreamError> {
        // Ask for the first chunk immediately so an unplayable source fails
        // here, where the error reaches the requester, instead of part-way
        // through a track.
        let probe = self
            .client
            .get(&self.url)
            .headers(self.headers.clone())
            .header(RANGE, format!("bytes={offset}-{offset}"))
            .send()
            .await
            .map_err(|error| AudioStreamError::Fail(Box::new(error)))?;
        if !probe.status().is_success() {
            let message: Box<dyn std::error::Error + Send + Sync + 'static> =
                format!("media request failed with status {}", probe.status()).into();
            return Err(AudioStreamError::Fail(message));
        }
        let total = content_range_total(probe.headers());

        Ok(ChunkedHttpStream {
            stream: Box::pin(StreamReader::new(self.body(offset))),
            request: self.clone(),
            total,
        })
    }
}

/// Reads the total resource length out of a `Content-Range: bytes A-B/TOTAL` header.
fn content_range_total(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(CONTENT_RANGE)?
        .to_str()
        .ok()?
        .rsplit_once('/')
        .and_then(|(_, total)| total.trim().parse().ok())
}

struct ChunkedHttpStream {
    stream: Pin<Box<dyn AsyncRead + Send + Sync>>,
    request: ChunkedHttpRequest,
    total: Option<u64>,
}

impl AsyncRead for ChunkedHttpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        AsyncRead::poll_read(self.stream.as_mut(), context, buffer)
    }
}

impl AsyncSeek for ChunkedHttpStream {
    fn start_seek(self: Pin<&mut Self>, _position: SeekFrom) -> IoResult<()> {
        Err(IoErrorKind::Unsupported.into())
    }

    fn poll_complete(self: Pin<&mut Self>, _context: &mut TaskContext<'_>) -> Poll<IoResult<u64>> {
        Poll::Ready(Err(IoErrorKind::Unsupported.into()))
    }
}

#[async_trait]
impl AsyncMediaSource for ChunkedHttpStream {
    fn is_seekable(&self) -> bool {
        false
    }

    async fn byte_len(&self) -> Option<u64> {
        self.total
    }

    async fn try_resume(
        &mut self,
        offset: u64,
    ) -> Result<Box<dyn AsyncMediaSource>, AudioStreamError> {
        self.request
            .open(offset)
            .await
            .map(|stream| Box::new(stream) as Box<dyn AsyncMediaSource>)
    }
}

#[async_trait]
impl Compose for ChunkedHttpRequest {
    fn create(&mut self) -> Result<AudioStream<Box<dyn MediaSource>>, AudioStreamError> {
        Err(AudioStreamError::Unsupported)
    }

    async fn create_async(
        &mut self,
    ) -> Result<AudioStream<Box<dyn MediaSource>>, AudioStreamError> {
        let stream = self.open(0).await?;
        Ok(AudioStream {
            input: Box::new(AsyncAdapterStream::new(Box::new(stream), 64 * 1024))
                as Box<dyn MediaSource>,
        })
    }

    fn should_create_async(&self) -> bool {
        true
    }
}

fn convert_headers(headers: &BTreeMap<String, String>) -> Result<HeaderMap> {
    let mut converted = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("source returned an invalid HTTP header name: {name:?}"))?;
        let value = HeaderValue::from_str(value)
            .with_context(|| format!("source returned an invalid value for header {name}"))?;
        converted.insert(name, value);
    }
    Ok(converted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_valid_resolver_headers() {
        let headers = BTreeMap::from([
            ("User-Agent".to_owned(), "test-agent".to_owned()),
            ("Referer".to_owned(), "https://www.youtube.com/".to_owned()),
        ]);
        let converted = convert_headers(&headers).unwrap();
        assert_eq!(converted["user-agent"], "test-agent");
    }

    /// Guards the dependency wiring that makes playback possible at all.
    ///
    /// Songbird registers only DCA and raw PCM itself and takes Symphonia with
    /// `default-features = false`, so without the direct Symphonia dependency
    /// in `Cargo.toml` these registries are empty, every track fails the
    /// instant it is probed, and nothing in the type system notices.
    #[test]
    fn the_codecs_youtube_audio_needs_are_registered() {
        use songbird::input::codecs::get_codec_registry;
        use symphonia::core::codecs::{CODEC_TYPE_AAC, CODEC_TYPE_OPUS};

        let registry = get_codec_registry();
        assert!(
            registry.get_codec(CODEC_TYPE_OPUS).is_some(),
            "Opus is what WebM and Ogg streams carry"
        );
        assert!(
            registry.get_codec(CODEC_TYPE_AAC).is_some(),
            "AAC is what YouTube's MP4 audio carries"
        );
    }

    /// The total length decides when chunking stops, so a misread header would
    /// either truncate a track or loop asking for bytes past its end.
    #[test]
    fn reads_the_total_length_from_a_content_range_header() {
        let total = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_RANGE, HeaderValue::from_str(value).unwrap());
            content_range_total(&headers)
        };

        assert_eq!(total("bytes 0-1048575/4092844"), Some(4_092_844));
        assert_eq!(total("bytes 1048576-2097151/4092844"), Some(4_092_844));
        // An origin that will not state the total leaves chunking to stop on a
        // short response instead of on a byte count.
        assert_eq!(total("bytes 0-1023/*"), None);
        assert_eq!(total("nonsense"), None);
    }

    #[test]
    fn rejects_header_injection() {
        let headers = BTreeMap::from([("X-Test".to_owned(), "ok\r\nevil: true".to_owned())]);
        assert!(convert_headers(&headers).is_err());
    }
}
