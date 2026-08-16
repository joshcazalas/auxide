use std::{
    collections::BTreeMap,
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult, SeekFrom},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
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
use tokio::{
    io::{AsyncRead, AsyncSeek, ReadBuf},
    time,
};
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

/// Deadline for a response's headers, and deliberately not for its body.
///
/// The client's connect and read timeouts do not bound a request whose
/// response simply never arrives, which showed up as a track stuck before it
/// ever became playable, with nothing logged at all.
///
/// It must not cover the body. Songbird pulls from this stream at playback
/// rate, so a one-mebibyte chunk takes about a minute of listening to drain
/// even though it arrives from the network in under a second. A deadline
/// spanning the body cancels healthy chunks mid-song, once every thirty
/// seconds of audio.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// How many times one stream may resolve a fresh media URL before giving up.
///
/// A signed URL can be refused from the moment it is issued, and asking the
/// source again reliably produces one that is not, so a refusal is treated as a
/// stale URL rather than as an unplayable track.
const MAX_URL_REFRESHES: u32 = 3;

/// Bytes a stream must deliver before a later failure stops counting against
/// [`MAX_URL_REFRESHES`].
///
/// Roughly half a minute of audio, which is enough to distinguish a URL that
/// never worked from one that carried the track for a while.
const PROGRESS_RESETS_REFRESHES: u64 = 512 * 1024;

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
            // Deliberately no read timeout. Songbird consumes this body at
            // playback rate, and reqwest resets that timer only when a frame
            // arrives, so a gap caused by our own pacing counts against it and
            // kills a healthy chunk roughly every thirty seconds of audio. The
            // wait for headers is bounded below instead.
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
                resolver: Arc::clone(&self.resolver),
                track: track.clone(),
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
#[derive(Clone)]
struct ChunkedHttpRequest {
    client: HttpClient,
    resolver: Arc<dyn SourceResolver>,
    track: TrackMetadata,
    url: String,
    headers: HeaderMap,
}

impl std::fmt::Debug for ChunkedHttpRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The media URL is a signed, short-lived credential of sorts and has no
        // place in a log line or a panic message.
        formatter
            .debug_struct("ChunkedHttpRequest")
            .field("source_id", &self.track.source_id)
            .finish_non_exhaustive()
    }
}

impl ChunkedHttpRequest {
    /// Streams the resource from `offset`, one [`MAX_RANGE_SPAN`] request at a time.
    ///
    /// The total length comes from the first response's `Content-Range` rather
    /// than from the caller, so it is whatever the origin actually serves. A
    /// server that ignores the range header and answers 200 is handled by
    /// taking its single response as the complete body.
    fn body(
        &self,
        start: u64,
        delivered: Arc<AtomicU64>,
    ) -> impl Stream<Item = IoResult<Bytes>> + Send + Sync + use<> {
        let request = self.clone();

        stream::try_unfold(Some((None::<u64>, None::<u64>)), move |state| {
            let request = request.clone();
            let delivered = Arc::clone(&delivered);
            async move {
                let Some((total, previous)) = state else {
                    return Ok(None);
                };
                let consumed = delivered.load(Ordering::Relaxed);
                let position = start.saturating_add(consumed);

                if total.is_some_and(|total| position >= total) {
                    return Ok(None);
                }
                // A response that delivered nothing would otherwise have this
                // ask for the same bytes forever.
                if previous == Some(consumed) {
                    tracing::warn!(
                        position,
                        "media request delivered no bytes; ending the stream"
                    );
                    return Ok(None);
                }

                let last = position.saturating_add(MAX_RANGE_SPAN - 1);
                // Only the wait for headers is bounded. `send` resolves once
                // they arrive, leaving the body to stream at whatever rate it
                // is consumed.
                let sent = request
                    .client
                    .get(&request.url)
                    .headers(request.headers.clone())
                    .header(RANGE, format!("bytes={position}-{last}"))
                    .send();
                let response = match time::timeout(RESPONSE_TIMEOUT, sent).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(error)) => {
                        tracing::warn!(%error, position, last, "media request failed");
                        return Err(IoError::other(error));
                    }
                    Err(_) => {
                        tracing::warn!(position, last, "media request timed out awaiting headers");
                        return Err(IoError::new(
                            IoErrorKind::TimedOut,
                            "media request timed out awaiting response headers",
                        ));
                    }
                };

                let status = response.status();
                if !status.is_success() {
                    // Reported as an error so Songbird's adapter calls
                    // try_resume, which is where a fresh URL is obtained.
                    tracing::warn!(
                        position,
                        last,
                        status = status.as_u16(),
                        "media request was refused"
                    );
                    return Err(IoError::other(format!(
                        "media request for bytes {position}-{last} failed with status {status}"
                    )));
                }

                let total = total.or_else(|| content_range_total(response.headers()));
                tracing::debug!(
                    position,
                    last,
                    status = status.as_u16(),
                    served = response.content_length(),
                    total,
                    "fetched a media chunk"
                );

                // A 200 means the range was ignored and the whole resource is
                // in this one response, so there is nothing left to ask for.
                let next = if status == StatusCode::OK {
                    None
                } else {
                    Some((total, Some(consumed)))
                };
                let counter = Arc::clone(&delivered);
                let body = response
                    .bytes_stream()
                    .map_ok(move |chunk| {
                        counter.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                        chunk
                    })
                    .map_err(move |error| {
                        tracing::warn!(%error, position, "media body read failed");
                        IoError::other(error)
                    });
                Ok(Some((body, next)))
            }
        })
        .try_flatten()
    }

    /// Builds the stream without first probing the URL.
    ///
    /// An earlier version asked for a single byte here to fail fast. That was
    /// worse than useless: a one-byte range is accepted even by a URL that
    /// refuses every real chunk, so it reported success and left the failure to
    /// surface later as a container that would not parse.
    fn open(&self, offset: u64) -> ChunkedHttpStream {
        // Where the next request resumes is decided by the bytes that actually
        // arrived, never by the length a response advertised. A body cut short
        // mid-chunk would otherwise leave a gap the container never recovers
        // from, which reads as audio that simply stops.
        let delivered = Arc::new(AtomicU64::new(0));
        ChunkedHttpStream {
            stream: Box::pin(StreamReader::new(self.body(offset, Arc::clone(&delivered)))),
            request: self.clone(),
            delivered,
            refreshes: 0,
        }
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
    /// Bytes this stream has delivered since it was opened.
    delivered: Arc<AtomicU64>,
    refreshes: u32,
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
        None
    }

    /// Continues the track after a read error, on a freshly resolved URL.
    ///
    /// A signed media URL can be refused from the moment it is issued, and
    /// asking the source again yields one that is not. Resolution is already
    /// just-in-time for exactly this reason, so a refusal is treated as a stale
    /// URL rather than as an unplayable track. Songbird calls this on any read
    /// error, which is what makes it the right place to do it.
    async fn try_resume(
        &mut self,
        offset: u64,
    ) -> Result<Box<dyn AsyncMediaSource>, AudioStreamError> {
        // A stream that played for a while before faltering is not a bad URL,
        // so it does not spend the budget meant for one. Without this a long
        // track exhausts its refreshes on unrelated hiccups and stops early.
        let delivered = self.delivered.load(Ordering::Relaxed);
        let progressed = delivered >= PROGRESS_RESETS_REFRESHES;
        if !progressed && self.refreshes >= MAX_URL_REFRESHES {
            tracing::warn!(
                offset,
                delivered,
                "giving up after refreshing the media URL repeatedly"
            );
            return Err(AudioStreamError::Unsupported);
        }
        let refreshes = if progressed { 1 } else { self.refreshes + 1 };
        tracing::warn!(offset, refreshes, "resolving a fresh media URL mid-track");

        let audio = self
            .request
            .resolver
            .resolve(&self.request.track)
            .await
            .map_err(|error| AudioStreamError::Fail(Box::new(error)))?;
        if audio.metadata.source_id != self.request.track.source_id {
            let message: Box<dyn std::error::Error + Send + Sync + 'static> =
                "resolved media identity changed mid-track".into();
            return Err(AudioStreamError::Fail(message));
        }

        let mut request = self.request.clone();
        request.url = audio.stream_url.to_string();
        request.headers = convert_headers(&audio.headers)
            .map_err(|error| AudioStreamError::Fail(error.into()))?;
        let mut resumed = request.open(offset);
        resumed.refreshes = refreshes;
        Ok(Box::new(resumed) as Box<dyn AsyncMediaSource>)
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
        let stream = self.open(0);
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
