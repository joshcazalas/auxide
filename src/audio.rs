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
use futures_util::{Stream, stream};
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

/// Span a single ranged request asks for.
///
/// What `YouTube` insists on is that a `Range` header be present at all, not
/// that it be narrow. Measured against one freshly resolved URL, spans of
/// 1 MiB, 2 MiB, an open-ended `bytes=N-`, and the whole 4 MiB file were each
/// served in under a fifth of a second; the same URL with no `Range` header was
/// answered 200 and then paced at about 30 KiB/s, near the track's own bitrate
/// and far too slow to stay ahead of playback.
///
/// So this span is a memory budget, not a limit the origin imposes. A mebibyte
/// is roughly a minute of Opus, which puts an ordinary song at four requests.
const CHUNK_SPAN: u64 = 1024 * 1024;

/// Deadline for one chunk: connection, headers, and body together.
///
/// A single deadline can cover the body now that a chunk is drained as fast as
/// the network delivers it rather than as fast as the song is listened to. A
/// mebibyte inside thirty seconds needs 280 kbit/s, well under what the voice
/// connection this feeds already requires.
const CHUNK_TIMEOUT: Duration = Duration::from_secs(30);

/// Attempts one URL gets at a chunk before it is treated as stale.
const CHUNK_ATTEMPTS: u32 = 3;

/// Pause before retrying a chunk, multiplied by the attempt that just failed.
///
/// `YouTube` answers 403 when it is rate-limiting the requester, not only when
/// a URL has gone bad, and it counts resolutions too. Backing off is what
/// clears that; asking the source for another URL only adds to it.
const RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// Audio buffered between the network and the decoder.
///
/// Songbird's adapter pulls from this stream only while its ring buffer has
/// room, so this figure is the entire read-ahead. The 64 KiB used before left
/// about four seconds of slack, which a single retry outlasts; a mebibyte is
/// roughly a minute of Opus.
const READ_AHEAD: usize = 1024 * 1024;

/// How many times one stream may resolve a fresh media URL before giving up.
///
/// A signed URL can be refused from the moment it is issued, and asking the
/// source again reliably produces one that is not, so a refusal is treated as a
/// stale URL rather than as an unplayable track.
const MAX_URL_REFRESHES: u32 = 3;

/// Pause before resolving a fresh URL, multiplied by the refresh being made.
const REFRESH_BACKOFF: Duration = Duration::from_secs(2);

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
            // No read timeout here: reqwest resets that timer per frame, which
            // says nothing about whether a chunk as a whole is making progress.
            // [`CHUNK_TIMEOUT`] bounds the entire request instead.
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
/// Songbird's own [`songbird::input::HttpRequest`] issues one unranged request
/// for the whole resource and then reads it as the song is listened to, which
/// is the pair of things `YouTube` will not tolerate: it paces an unranged
/// response to about the track's bitrate, and it closes any response the client
/// falls behind on. Each chunk here is asked for by range and read to the end
/// before the next is asked for, so the network is never waiting on playback.
///
/// It cannot be made to chunk either: its adapter only retries after a read
/// *error*, so a deliberately short response would read as a clean end of track
/// and truncate playback rather than continue.
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

/// What one ranged request came back with.
enum Chunk {
    /// Bytes covering the requested range, and the resource length when the
    /// origin stated one.
    Range { bytes: Bytes, total: Option<u64> },
    /// The origin ignored the range and answered with the whole resource, so
    /// there is nothing left to ask for.
    Whole(Bytes),
    /// The range starts past the end of the resource.
    Exhausted,
}

impl ChunkedHttpRequest {
    /// Streams the resource from `offset`, one [`CHUNK_SPAN`] request at a time.
    ///
    /// The total length comes from the first response's `Content-Range` rather
    /// than from the caller, so it is whatever the origin actually serves.
    fn body(&self, start: u64) -> impl Stream<Item = IoResult<Bytes>> + Send + Sync + use<> {
        let request = self.clone();

        stream::try_unfold(Some((start, None::<u64>)), move |state| {
            let request = request.clone();
            async move {
                let Some((position, total)) = state else {
                    return Ok(None);
                };
                if total.is_some_and(|total| position >= total) {
                    return Ok(None);
                }

                match request.chunk(position, total).await? {
                    Chunk::Exhausted => Ok(None),
                    // A 200 means the range was ignored and this one response
                    // carries the resource from its very start, so whatever has
                    // already played has to come off the front of it.
                    Chunk::Whole(bytes) => {
                        let played = usize::try_from(position).unwrap_or(usize::MAX);
                        Ok((played < bytes.len()).then(|| (bytes.slice(played..), None)))
                    }
                    // The next request resumes from the bytes that actually
                    // arrived, never from the length a response advertised. A
                    // body cut short mid-chunk would otherwise leave a gap the
                    // container never recovers from, which reads as audio that
                    // simply stops.
                    Chunk::Range {
                        bytes,
                        total: stated,
                    } if !bytes.is_empty() => {
                        let next = position.saturating_add(bytes.len() as u64);
                        // A length, once stated, stays known even if a later
                        // response omits it.
                        Ok(Some((bytes, Some((next, stated.or(total))))))
                    }
                    // A response that delivered nothing would otherwise have
                    // this ask for the same bytes forever.
                    Chunk::Range { .. } => {
                        tracing::warn!(
                            position,
                            "media request delivered no bytes; ending the stream"
                        );
                        Ok(None)
                    }
                }
            }
        })
    }

    /// Fetches one chunk, giving the URL in hand a few spaced attempts first.
    ///
    /// A refusal is more often `YouTube` rate-limiting this host than a URL
    /// that has gone bad, and the caller's remedy for a bad URL — resolving a
    /// fresh one — spends another request against that same limit. Waiting is
    /// what clears it, so the URL is only declared stale once pausing has not
    /// helped.
    async fn chunk(&self, position: u64, total: Option<u64>) -> IoResult<Chunk> {
        let last = range_end(position, total);
        let mut attempt = 1;
        loop {
            let error = match self.fetch(position, last).await {
                Ok(chunk) => return Ok(chunk),
                Err(error) => error,
            };
            if attempt >= CHUNK_ATTEMPTS {
                tracing::warn!(%error, position, last, attempts = attempt, "media chunk failed on every attempt");
                return Err(error);
            }
            let pause = RETRY_BACKOFF * attempt;
            tracing::warn!(%error, position, last, attempt, "media chunk failed; retrying the same URL");
            time::sleep(pause).await;
            attempt += 1;
        }
    }

    /// Issues one ranged request and drains its body at network speed.
    ///
    /// Draining promptly is the point of the whole arrangement. `YouTube` cuts
    /// off a response the client is not keeping up with: reading a mebibyte
    /// chunk at the ~16 KiB/s a song is listened to got 800 KiB of it before
    /// the connection was closed, reproducibly and at around fifty seconds
    /// every time. That reached the decoder as a truncated container and ended
    /// the track two thirds of the way through.
    async fn fetch(&self, position: u64, last: u64) -> IoResult<Chunk> {
        let sent = self
            .client
            .get(&self.url)
            .headers(self.headers.clone())
            .header(RANGE, format!("bytes={position}-{last}"))
            .send();

        let chunk = async {
            let response = sent.await.map_err(IoError::other)?;
            let status = response.status();
            // Past the end of the resource is not a failure, just the end.
            if status == StatusCode::RANGE_NOT_SATISFIABLE {
                return Ok(Chunk::Exhausted);
            }
            if !status.is_success() {
                // Reported as an error so the retry above, and then Songbird's
                // call to try_resume, both get their turn.
                return Err(IoError::other(format!(
                    "media request for bytes {position}-{last} was refused with status {status}"
                )));
            }
            let total = content_range_total(response.headers());
            let bytes = response.bytes().await.map_err(IoError::other)?;
            tracing::debug!(
                position,
                last,
                status = status.as_u16(),
                served = bytes.len(),
                total,
                "fetched a media chunk"
            );
            Ok(if status == StatusCode::OK {
                Chunk::Whole(bytes)
            } else {
                Chunk::Range { bytes, total }
            })
        };

        match time::timeout(CHUNK_TIMEOUT, chunk).await {
            Ok(chunk) => chunk,
            Err(_) => Err(IoError::new(
                IoErrorKind::TimedOut,
                format!("media request for bytes {position}-{last} timed out"),
            )),
        }
    }

    /// Builds the stream without first probing the URL.
    ///
    /// An earlier version asked for a single byte here to fail fast. That was
    /// worse than useless: a one-byte range is accepted even by a URL that
    /// refuses every real chunk, so it reported success and left the failure to
    /// surface later as a container that would not parse.
    fn open(&self, offset: u64, total: Option<u64>) -> ChunkedHttpStream {
        ChunkedHttpStream {
            stream: Box::pin(StreamReader::new(self.body(offset))),
            request: self.clone(),
            start: offset,
            position: offset,
            total,
            pending_seek: None,
            refreshes: 0,
        }
    }

    /// Asks the origin how long the resource is, before reading any of it.
    ///
    /// One byte is enough: a ranged response states the total in its
    /// `Content-Range` regardless of how little was asked for. An origin that
    /// answers `200` ignored the range and is telling us it cannot serve parts
    /// of the file, which is the same thing as saying it cannot be seeked in.
    ///
    /// Returning `None` is not a failure. It costs seeking and nothing else,
    /// which is exactly the behaviour every version before this one had.
    async fn probe_length(&self) -> Option<u64> {
        let response = self
            .client
            .get(&self.url)
            .headers(self.headers.clone())
            .header(RANGE, "bytes=0-0")
            .timeout(CHUNK_TIMEOUT)
            .send()
            .await
            .ok()?;
        if response.status() != StatusCode::PARTIAL_CONTENT {
            tracing::debug!(status = %response.status(), "origin ignored a range request; seeking is off");
            return None;
        }
        let total = content_range_total(response.headers());
        if total.is_none() {
            tracing::debug!("origin stated no length; seeking is off");
        }
        total
    }
}

/// Last byte a chunk starting at `position` should ask for.
///
/// Clamping to a known total keeps the final chunk of a track from reaching
/// past the end of the resource, which an origin is free to answer with a
/// refusal rather than with the bytes that do exist.
fn range_end(position: u64, total: Option<u64>) -> u64 {
    let span = position.saturating_add(CHUNK_SPAN - 1);
    match total {
        Some(total) => span.min(total.saturating_sub(1)),
        None => span,
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
    /// Offset in the resource this stream was opened at.
    start: u64,
    /// Where reading has reached, counted from the start of the resource.
    position: u64,
    /// The resource's length, when the origin was willing to state one.
    ///
    /// Its presence is what makes the stream seekable: Symphonia turns a
    /// timestamp into a byte offset, and it cannot do that without knowing how
    /// many bytes there are.
    total: Option<u64>,
    /// Where a seek asked to go, until [`AsyncSeek::poll_complete`] takes it.
    pending_seek: Option<u64>,
    refreshes: u32,
}

impl AsyncRead for ChunkedHttpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        let before = buffer.filled().len();
        let polled = AsyncRead::poll_read(self.stream.as_mut(), context, buffer);
        if polled.is_ready() {
            let read = buffer.filled().len().saturating_sub(before) as u64;
            self.position = self.position.saturating_add(read);
        }
        polled
    }
}

impl AsyncSeek for ChunkedHttpStream {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> IoResult<()> {
        let target = match position {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(delta) => offset_by(self.position, delta)?,
            SeekFrom::End(delta) => {
                let total = self
                    .total
                    .ok_or_else(|| IoError::from(IoErrorKind::Unsupported))?;
                offset_by(total, delta)?
            }
        };
        if let Some(total) = self.total {
            if target > total {
                return Err(IoError::new(
                    IoErrorKind::InvalidInput,
                    "seek past the end of the media",
                ));
            }
        }
        self.pending_seek = Some(target);
        Ok(())
    }

    /// Reopens the resource at the requested offset.
    ///
    /// Nothing is fetched here: the body is a lazy stream of ranged requests,
    /// so replacing it costs only the request the next read will make. That is
    /// what lets a seek complete synchronously.
    fn poll_complete(
        mut self: Pin<&mut Self>,
        _context: &mut TaskContext<'_>,
    ) -> Poll<IoResult<u64>> {
        if let Some(target) = self.pending_seek.take() {
            self.stream = Box::pin(StreamReader::new(self.request.body(target)));
            self.position = target;
            // A seek is not a stream that faltered, so what it delivers from
            // here counts as progress from here rather than from wherever the
            // stream happened to open.
            self.start = target;
        }
        Poll::Ready(Ok(self.position))
    }
}

/// Applies a signed delta to an offset, refusing to go before the start.
fn offset_by(from: u64, delta: i64) -> IoResult<u64> {
    let target = i64::try_from(from)
        .ok()
        .and_then(|from| from.checked_add(delta))
        .ok_or_else(|| IoError::new(IoErrorKind::InvalidInput, "seek offset overflowed"))?;
    u64::try_from(target).map_err(|_| {
        IoError::new(
            IoErrorKind::InvalidInput,
            "seek before the start of the media",
        )
    })
}

#[async_trait]
impl AsyncMediaSource for ChunkedHttpStream {
    /// Reports whether the origin gave enough to seek within.
    ///
    /// Read once when the stream is adapted rather than per seek, so it has to
    /// be right before anything has been read — which is why the length is
    /// probed when the request is created rather than learned on the way past.
    fn is_seekable(&self) -> bool {
        self.total.is_some()
    }

    async fn byte_len(&self) -> Option<u64> {
        self.total
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
        // Songbird counts bytes from the start of the input, so `offset` is an
        // absolute position and the distance from where this stream opened is
        // what it managed to deliver.
        //
        // A stream that played for a while before faltering is not a bad URL,
        // so it does not spend the budget meant for one. Without this a long
        // track exhausts its refreshes on unrelated hiccups and stops early.
        let delivered = offset.saturating_sub(self.start);
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
        // Resolving is itself a request against whatever limit just refused
        // this one, so the refreshes are spaced rather than fired back to back.
        // The read-ahead buffer covers about a minute, which is room enough.
        time::sleep(REFRESH_BACKOFF * refreshes).await;

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
        let mut resumed = request.open(offset, self.total);
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
        // Asked for before the stream is adapted, because the adapter reads
        // seekability once and keeps the answer.
        let total = self.probe_length().await;
        let stream = self.open(0, total);
        Ok(AudioStream {
            input: Box::new(AsyncAdapterStream::new(Box::new(stream), READ_AHEAD))
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
    use std::sync::Mutex;

    use tokio::{
        io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
        net::TcpListener,
    };
    use url::Url;

    use super::*;
    use crate::source::{ResolvedAudio, SourceError};

    /// How a fake origin should answer one request.
    #[derive(Clone, Copy)]
    enum Reply {
        /// Serve the requested range in full.
        Full,
        /// Serve only this many of the requested bytes, and say so honestly.
        Short(usize),
        /// Refuse, the way `YouTube` does while it is rate-limiting.
        Refuse(u16),
    }

    struct Origin {
        url: String,
        /// Every range asked for, in order.
        ranges: Arc<Mutex<Vec<(u64, u64)>>>,
    }

    /// Serves ranges of `body` over HTTP/1.1, one `replies` entry per request.
    async fn origin(body: Arc<Vec<u8>>, replies: Vec<Reply>) -> Origin {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/media", listener.local_addr().unwrap());
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&ranges);

        tokio::spawn(async move {
            let total = body.len();
            for reply in replies {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    if socket.read_exact(&mut byte).await.is_err() {
                        return;
                    }
                    head.push(byte[0]);
                }

                let head = String::from_utf8_lossy(&head).to_ascii_lowercase();
                let range = head
                    .split("range: bytes=")
                    .nth(1)
                    .and_then(|rest| rest.split("\r\n").next())
                    .expect("every media request must carry a bounded range");
                let (start, end) = range.split_once('-').unwrap();
                let (start, end): (u64, u64) = (start.parse().unwrap(), end.parse().unwrap());
                recorded.lock().unwrap().push((start, end));

                let start = usize::try_from(start).unwrap();
                let asked = usize::try_from(end).unwrap().min(total - 1) - start + 1;
                let served = match reply {
                    Reply::Refuse(status) => {
                        let refusal = format!(
                            "HTTP/1.1 {status} Refused\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        );
                        let _ = socket.write_all(refusal.as_bytes()).await;
                        let _ = socket.shutdown().await;
                        continue;
                    }
                    Reply::Full => asked,
                    Reply::Short(bytes) => bytes.min(asked),
                };

                let last = start + served - 1;
                let head = format!(
                    "HTTP/1.1 206 Partial Content\r\n\
                     Content-Range: bytes {start}-{last}/{total}\r\n\
                     Content-Length: {served}\r\n\
                     Connection: close\r\n\r\n"
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&body[start..=last]).await;
                let _ = socket.shutdown().await;
            }
        });

        Origin { url, ranges }
    }

    /// Stands in for the source, and fails the test if a stream reaches for it.
    struct UnusedResolver;

    #[async_trait]
    impl SourceResolver for UnusedResolver {
        async fn search(&self, _query: &str) -> Result<Vec<TrackMetadata>, SourceError> {
            unreachable!()
        }
        async fn inspect(&self, _url: &Url) -> Result<TrackMetadata, SourceError> {
            unreachable!()
        }
        async fn resolve(&self, _track: &TrackMetadata) -> Result<ResolvedAudio, SourceError> {
            panic!("a chunk the origin went on to serve must not cost a fresh media URL")
        }
        fn accepts(&self, _url: &Url) -> Result<(), SourceError> {
            unreachable!()
        }
        async fn playlist(
            &self,
            _url: &Url,
        ) -> Result<Option<crate::source::Playlist>, SourceError> {
            unreachable!()
        }
    }

    fn request(url: &str) -> ChunkedHttpRequest {
        ChunkedHttpRequest {
            client: HttpClient::new(),
            resolver: Arc::new(UnusedResolver),
            track: TrackMetadata {
                source_id: "source-id".to_owned(),
                canonical_url: Url::parse("https://www.youtube.com/watch?v=source-id").unwrap(),
                title: "Example".to_owned(),
                channel: None,
                duration: Duration::from_secs(60),
                thumbnail_url: None,
            },
            url: url.to_owned(),
            headers: HeaderMap::new(),
        }
    }

    /// Bytes a repeat or a misordered chunk cannot hide in.
    fn media(len: usize) -> Arc<Vec<u8>> {
        let mut value = 1u32;
        Arc::new(
            (0..len)
                .map(|_| {
                    value = value.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    u8::try_from(value >> 24).unwrap()
                })
                .collect(),
        )
    }

    /// An origin that answers `200` and sends the whole file, range or not.
    async fn origin_ignoring_ranges(body: Arc<Vec<u8>>) -> Origin {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/media", listener.local_addr().unwrap());
        let ranges = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let body = Arc::clone(&body);
                tokio::spawn(async move {
                    let mut request = [0_u8; 1024];
                    let _ = stream.read(&mut request).await;
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(head.as_bytes()).await;
                    let _ = stream.write_all(&body).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        Origin { url, ranges }
    }

    async fn play(request: &ChunkedHttpRequest) -> Vec<u8> {
        let mut played = Vec::new();
        request
            .open(0, None)
            .read_to_end(&mut played)
            .await
            .expect("the stream must reach the end of the resource");
        played
    }

    /// Seeking hangs entirely on whether the origin will state a length, and
    /// the answer is read once before anything is played — so getting it wrong
    /// is silent in both directions.
    #[tokio::test]
    async fn a_stated_length_is_what_makes_a_stream_seekable() {
        let body = media(8192);
        let origin = origin(Arc::clone(&body), vec![Reply::Full; 2]).await;
        let request = request(&origin.url);

        let total = request.probe_length().await;
        assert_eq!(total, Some(8192));
        let stream = request.open(0, total);
        assert!(stream.is_seekable());
        assert_eq!(stream.byte_len().await, Some(8192));

        // A probe costs one request, and it asks for a single byte rather than
        // pulling the resource down to find out how long it is.
        assert_eq!(origin.ranges.lock().unwrap()[0], (0, 0));
    }

    /// An origin that ignores ranges is telling us it cannot serve parts of the
    /// file, which is the same thing as saying it cannot be seeked in.
    #[tokio::test]
    async fn an_origin_that_ignores_ranges_is_not_seekable() {
        let body = media(4096);
        let origin = origin_ignoring_ranges(Arc::clone(&body)).await;
        let request = request(&origin.url);

        assert_eq!(request.probe_length().await, None);
        let stream = request.open(0, None);
        assert!(!stream.is_seekable());
        assert_eq!(stream.byte_len().await, None);
    }

    #[tokio::test]
    async fn seeking_reopens_the_resource_where_it_was_asked_to() {
        let body = media(8192);
        let origin = origin(Arc::clone(&body), vec![Reply::Full; 4]).await;
        let request = request(&origin.url);
        let mut stream = request.open(0, Some(8192));

        let mut opening = vec![0_u8; 16];
        stream.read_exact(&mut opening).await.unwrap();
        assert_eq!(opening, body[..16]);

        // Nothing is fetched by the seek itself; the request it needs is made
        // by the read that follows it.
        let landed = stream.seek(SeekFrom::Start(4096)).await.unwrap();
        assert_eq!(landed, 4096);
        let mut after = vec![0_u8; 16];
        stream.read_exact(&mut after).await.unwrap();
        assert_eq!(after, body[4096..4112]);

        // Relative seeks count from where reading has actually reached.
        assert_eq!(stream.seek(SeekFrom::Current(-112)).await.unwrap(), 4000);
        assert_eq!(stream.seek(SeekFrom::End(-96)).await.unwrap(), 8096);
        assert!(stream.seek(SeekFrom::Start(9000)).await.is_err());
        assert!(stream.seek(SeekFrom::Current(-99_999)).await.is_err());
    }

    /// The point of chunking at all: a resource larger than one request comes
    /// back exactly, in order, with no gap and no repeat.
    #[tokio::test]
    async fn reassembles_a_resource_from_bounded_ranges() {
        let span = usize::try_from(CHUNK_SPAN).unwrap();
        let body = media(2 * span + 4096);
        let origin = origin(Arc::clone(&body), vec![Reply::Full; 4]).await;

        assert_eq!(play(&request(&origin.url)).await, *body);
        assert_eq!(
            *origin.ranges.lock().unwrap(),
            [
                (0, CHUNK_SPAN - 1),
                (CHUNK_SPAN, 2 * CHUNK_SPAN - 1),
                // Clamped: the last request must not reach past the resource.
                (2 * CHUNK_SPAN, 2 * CHUNK_SPAN + 4095),
            ]
        );
    }

    /// A response carrying fewer bytes than were asked for is resumed from
    /// where it actually stopped. Trusting the span that was requested instead
    /// leaves a hole in the container, which reaches a listener as a track that
    /// stops partway through.
    #[tokio::test]
    async fn resumes_from_the_bytes_that_arrived() {
        let span = usize::try_from(CHUNK_SPAN).unwrap();
        let body = media(span + 4096);
        let origin = origin(
            Arc::clone(&body),
            vec![Reply::Short(1024), Reply::Full, Reply::Full],
        )
        .await;

        assert_eq!(play(&request(&origin.url)).await, *body);
        let ranges = origin.ranges.lock().unwrap().clone();
        assert_eq!(ranges[0], (0, CHUNK_SPAN - 1));
        assert_eq!(
            ranges[1].0, 1024,
            "the next request must pick up where the last response stopped"
        );
    }

    /// A refusal spends the URL in hand before it spends a resolution.
    /// `YouTube` refuses while it is rate-limiting this host, and asking the
    /// source for another URL is one more request against that same limit.
    #[tokio::test]
    async fn retries_a_refused_chunk_before_replacing_the_url() {
        let body = media(4096);
        let origin = origin(Arc::clone(&body), vec![Reply::Refuse(403), Reply::Full]).await;

        assert_eq!(play(&request(&origin.url)).await, *body);
        let ranges = origin.ranges.lock().unwrap().clone();
        assert_eq!(ranges.len(), 2, "the same range must be asked for twice");
        assert_eq!(ranges[0], ranges[1]);
    }

    /// A range reaching past the end of a track is a range an origin may refuse
    /// outright rather than clamp, which would end the track one chunk early.
    #[test]
    fn clamps_the_last_range_to_the_resource_length() {
        assert_eq!(range_end(0, None), CHUNK_SPAN - 1);
        assert_eq!(range_end(0, Some(4_164_515)), CHUNK_SPAN - 1);
        assert_eq!(range_end(4_000_000, Some(4_164_515)), 4_164_514);
        assert_eq!(range_end(0, Some(0)), 0);
    }

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
