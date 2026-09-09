use std::{
    collections::BTreeMap,
    io::{Error as IoError, ErrorKind as IoErrorKind, Read, Result as IoResult, Seek, SeekFrom},
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt as _, stream};
use reqwest::{
    Client as HttpClient, StatusCode,
    header::{CONTENT_RANGE, HeaderMap, HeaderName, HeaderValue, RANGE},
};
use songbird::input::{
    AsyncAdapterStream, AsyncMediaSource, AudioStream, AudioStreamError, Compose, HlsRequest,
    Input, LiveInput,
    codecs::{get_codec_registry, get_probe},
    core::io::MediaSource,
};
use tokio::{
    io::{AsyncRead, AsyncSeek, ReadBuf},
    time,
};
use tokio_util::{io::StreamReader, sync::CancellationToken};

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

/// Attempts one URL gets at a chunk that never arrived, before it is stale.
///
/// Only for a chunk that failed on the way — a refused one is not retried at
/// all, because a status is the origin's answer and it will give the same one.
const CHUNK_ATTEMPTS: u32 = 3;

/// Pause before retrying a chunk, multiplied by the attempt that just failed.
///
/// This covers a dropped connection, a reset, a request that ran out of time —
/// the failures where nothing was said and trying again is the whole remedy. It
/// once covered refusals too, on the belief that a 403 meant `YouTube` was
/// rate-limiting and waiting would clear it. Measurement said otherwise: the
/// identical range requested twice in a row was answered 206 both times while a
/// chunk past a ceiling was refused every time, however long the wait.
const RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// Audio buffered between the network and the decoder.
///
/// Songbird's adapter pulls from this stream only while its ring buffer has
/// room, so this figure is the entire read-ahead. The 64 KiB used before left
/// about four seconds of slack, which a single retry outlasts; a mebibyte is
/// roughly a minute of Opus.
const READ_AHEAD: usize = 1024 * 1024;

/// Includes resolution, fetching the headers, and opening the decoder.
const PREPARE_TIMEOUT: Duration = Duration::from_secs(60);

/// A diagnostic reads at network speed, with no Discord connection or clocked playback.
const PROBE_TIMEOUT: Duration = Duration::from_secs(60);

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
            // Also bounds reads made by Songbird's HLS input. Ranged requests
            // have an additional deadline covering the entire chunk.
            .read_timeout(CHUNK_TIMEOUT)
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

    /// Resolves a queued track and opens its container and decoder before playback.
    ///
    /// # Errors
    ///
    /// Returns an error when the source cannot be refreshed, supplies invalid headers, or selects
    /// a protocol outside Auxide's allowlist, or the audio cannot be parsed in time.
    pub async fn prepare(&self, track: &TrackMetadata) -> Result<Input> {
        let cancellation = CancellationToken::new();
        let guard = cancellation.clone().drop_guard();
        let input = self.prepare_bounded(track, cancellation).await?;
        // A successful input belongs to playback now. A skipped or timed-out
        // preparation instead cancels its network reads, including the ones
        // owned by the blocking parser task.
        guard.disarm();
        Ok(input)
    }

    async fn prepare_bounded(
        &self,
        track: &TrackMetadata,
        cancellation: CancellationToken,
    ) -> Result<Input> {
        time::timeout(PREPARE_TIMEOUT, self.prepare_input(track, cancellation))
            .await
            .context("audio preparation timed out")?
    }

    async fn prepare_input(
        &self,
        track: &TrackMetadata,
        cancellation: CancellationToken,
    ) -> Result<Input> {
        let audio = self.resolver.resolve(track).await?;
        let headers = convert_headers(&audio.headers)?;
        let input = match audio.protocol.as_deref() {
            None | Some("https") => Input::Lazy(Box::new(ChunkedHttpRequest {
                client: self.http.clone(),
                resolver: Arc::clone(&self.resolver),
                track: track.clone(),
                url: audio.stream_url.to_string(),
                headers,
                cancellation: cancellation.clone(),
            })),
            Some("m3u8" | "m3u8_native") => HlsRequest::new_with_headers(
                self.http.clone(),
                audio.stream_url.to_string(),
                headers,
            )
            .into(),
            Some(protocol) => bail!("source selected unsupported media protocol {protocol:?}"),
        };
        let Input::Live(LiveInput::Raw(stream), recipe) = input.make_live_async().await? else {
            bail!("audio source did not provide a raw stream");
        };
        let input = Input::Live(
            LiveInput::Raw(AudioStream {
                input: Box::new(SequentialInput {
                    inner: stream.input,
                    cancellation,
                }),
            }),
            recipe,
        );
        input
            .make_playable_async(get_codec_registry(), get_probe())
            .await
            .context("failed to open audio stream")
    }

    /// Fetches, parses, and decodes a bounded sample through the playback pipeline.
    ///
    /// No Discord token, gateway, voice connection, or audio output is used.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero packet limit, unplayable audio, or a timeout.
    pub async fn probe(&self, track: &TrackMetadata, packets: u32) -> Result<AudioProbe> {
        if packets == 0 {
            bail!("the audio probe needs at least one packet");
        }
        let cancellation = CancellationToken::new();
        let _guard = cancellation.clone().drop_guard();
        let input = self.prepare_bounded(track, cancellation.clone()).await?;
        let decoding =
            tokio::task::spawn_blocking(move || decode_sample(input, packets, &cancellation));
        time::timeout(PROBE_TIMEOUT, decoding)
            .await
            .context("audio probe timed out")?
            .context("audio probe task failed")?
    }

    /// Reads the first `wanted` bytes of a track, through the real media path.
    ///
    /// The daily probe checks that `YouTube` still answers questions about a
    /// track — its title, its length, what a playlist holds. Answering those is
    /// not the same as handing the track over, and the difference is not
    /// academic: through the whole outage that stopped every song a minute in,
    /// every one of those questions was answered correctly. Resolution had
    /// never broken. Only fetching had, and nothing looked at fetching.
    ///
    /// So this asks for bytes, over the same chunked ranged reader playback
    /// uses, and reports how many arrived. Asking for more than the ceiling
    /// that outage imposed is what makes it a check rather than a formality:
    /// the first megabyte came back fine throughout.
    ///
    /// # Errors
    ///
    /// Returns an error when the track cannot be resolved, the source supplies
    /// headers that are not headers, or the media is served over a protocol
    /// this does not read.
    pub async fn reach(&self, track: &TrackMetadata, wanted: u64) -> Result<MediaReach> {
        let audio = self.resolver.resolve(track).await?;
        let headers = convert_headers(&audio.headers)?;
        match audio.protocol.as_deref() {
            None | Some("https") => {}
            Some(protocol) => bail!("media is served over {protocol:?}, which this cannot read"),
        }
        let request = ChunkedHttpRequest {
            client: self.http.clone(),
            resolver: Arc::clone(&self.resolver),
            track: track.clone(),
            url: audio.stream_url.to_string(),
            headers,
            cancellation: CancellationToken::new(),
        };
        let total = request.probe_length().await;
        let mut stream = request.open(0);
        let mut fetched = 0;
        let mut buffer = vec![0_u8; 64 * 1024];
        while fetched < wanted {
            let read = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer).await?;
            if read == 0 {
                break;
            }
            fetched += read as u64;
        }
        Ok(MediaReach { fetched, total })
    }
}

/// How much audio a diagnostic actually decoded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AudioProbe {
    pub packets: u32,
    pub frames: u64,
    pub reached_end: bool,
}

/// Also checks cancellation during parsing HLS inputs, whose network reads are
/// owned by Songbird. The client's read timeout bounds an in-flight HLS read.
struct SequentialInput {
    inner: Box<dyn MediaSource>,
    cancellation: CancellationToken,
}

impl Read for SequentialInput {
    fn read(&mut self, buffer: &mut [u8]) -> IoResult<usize> {
        if self.cancellation.is_cancelled() {
            return Err(IoErrorKind::ConnectionAborted.into());
        }
        self.inner.read(buffer)
    }
}

impl Seek for SequentialInput {
    fn seek(&mut self, _position: SeekFrom) -> IoResult<u64> {
        Err(IoErrorKind::Unsupported.into())
    }
}

impl MediaSource for SequentialInput {
    fn is_seekable(&self) -> bool {
        false
    }
    fn byte_len(&self) -> Option<u64> {
        None
    }
}

fn decode_sample(
    mut input: Input,
    wanted: u32,
    cancellation: &CancellationToken,
) -> Result<AudioProbe> {
    let parsed = input.parsed_mut().context("audio was not prepared")?;
    let mut probe = AudioProbe {
        packets: 0,
        frames: 0,
        reached_end: false,
    };
    while probe.packets < wanted {
        if cancellation.is_cancelled() {
            bail!("audio probe was cancelled");
        }
        let packet = match parsed.format.next_packet() {
            Ok(packet) => packet,
            Err(symphonia::core::errors::Error::IoError(error))
                if error.kind() == IoErrorKind::UnexpectedEof =>
            {
                probe.reached_end = true;
                break;
            }
            Err(error) => return Err(error).context("failed to read an audio packet"),
        };
        if packet.track_id() != parsed.track_id {
            continue;
        }
        let decoded = parsed
            .decoder
            .decode(&packet)
            .context("failed to decode an audio packet")?;
        probe.frames += decoded.frames() as u64;
        probe.packets += 1;
    }
    if probe.frames == 0 {
        bail!("the stream produced no decoded audio");
    }
    Ok(probe)
}

/// What a media probe managed to get, and how much there was to get.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MediaReach {
    /// Bytes that actually arrived.
    pub fetched: u64,
    /// The whole resource's length, when the origin stated one.
    pub total: Option<u64>,
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
    cancellation: CancellationToken,
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
        .take_until(self.cancellation.clone().cancelled_owned())
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
            // A status is an answer, not a hiccup. The origin has said what it
            // will do with this request, and asking again in a second gets the
            // same sentence — only a different URL can be answered differently,
            // and that is the caller's move rather than this loop's.
            //
            // Against the megabyte ceiling YouTube spent a while enforcing,
            // waiting it out cost twelve requests and three quarters of a
            // minute per track to arrive at the refusal the first one gave, and
            // made a wall look like rate limiting for a day.
            if error.kind() == IoErrorKind::PermissionDenied {
                tracing::warn!(%error, position, last, "media chunk was refused; the same URL will not answer differently");
                return Err(error);
            }
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
            let response = sent
                .await
                .map_err(|error| IoError::other(error.without_url()))?;
            let status = response.status();
            // Past the end of the resource is not a failure, just the end.
            if status == StatusCode::RANGE_NOT_SATISFIABLE {
                return Ok(Chunk::Exhausted);
            }
            if !status.is_success() {
                // `PermissionDenied` marks this as the origin's answer rather
                // than something that went wrong on the way, which is what
                // stops the caller asking again. Songbird's call to try_resume
                // still gets its turn, and a fresh URL is the one thing that
                // could be answered differently.
                return Err(IoError::new(
                    IoErrorKind::PermissionDenied,
                    format!(
                        "media request for bytes {position}-{last} was refused with status {status}"
                    ),
                ));
            }
            let total = content_range_total(response.headers());
            let bytes = response
                .bytes()
                .await
                .map_err(|error| IoError::other(error.without_url()))?;
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
    fn open(&self, offset: u64) -> ChunkedHttpStream {
        ChunkedHttpStream {
            stream: Box::pin(StreamReader::new(self.body(offset))),
            request: self.clone(),
            start: offset,
            refreshes: 0,
        }
    }

    /// Asks the origin for the resource length for the byte-fetch diagnostic.
    /// Playback learns this from its first chunk and does not make a length probe.
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
            tracing::debug!(status = %response.status(), "origin ignored the length probe");
            return None;
        }
        let total = content_range_total(response.headers());
        if total.is_none() {
            tracing::debug!("origin stated no length");
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
    // Songbird 0.6 retains pending read-ahead bytes across a seek. Symphonia
    // can seek while opening a WebM, even without a user seek command, and then
    // parse bytes from the wrong position. Network inputs are sequential only.
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
        let cancellation = self.request.cancellation.clone();
        cancellation
            .run_until_cancelled(self.resume_at(offset))
            .await
            .unwrap_or(Err(AudioStreamError::Unsupported))
    }
}

impl ChunkedHttpStream {
    async fn resume_at(
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
        /// Refuse, with a status. An answer, however unwelcome.
        Refuse(u16),
        /// Answer nothing at all and hang up, the way a dropped connection does.
        Drop,
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
                    Reply::Drop => {
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

    struct StaticResolver {
        url: Url,
    }

    #[async_trait]
    impl SourceResolver for StaticResolver {
        async fn search(&self, _query: &str) -> Result<Vec<TrackMetadata>, SourceError> {
            Err(SourceError::Disabled)
        }
        async fn inspect(&self, _url: &Url) -> Result<TrackMetadata, SourceError> {
            Err(SourceError::Disabled)
        }
        async fn resolve(&self, track: &TrackMetadata) -> Result<ResolvedAudio, SourceError> {
            Ok(ResolvedAudio {
                metadata: track.clone(),
                stream_url: self.url.clone(),
                headers: BTreeMap::new(),
                protocol: Some("https".to_owned()),
            })
        }
        fn accepts(&self, _url: &Url) -> Result<(), SourceError> {
            Ok(())
        }
        async fn playlist(
            &self,
            _url: &Url,
        ) -> Result<Option<crate::source::Playlist>, SourceError> {
            Err(SourceError::Disabled)
        }
    }

    fn pipeline(url: &str) -> AudioPipeline {
        AudioPipeline::new(
            Arc::new(StaticResolver {
                url: Url::parse(url).unwrap(),
            }),
            0.5,
        )
        .unwrap()
    }

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
            cancellation: CancellationToken::new(),
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
            .open(0)
            .read_to_end(&mut played)
            .await
            .expect("the stream must reach the end of the resource");
        played
    }

    #[tokio::test]
    async fn a_known_length_never_enables_seeking() {
        let body = media(8192);
        let origin = origin(Arc::clone(&body), vec![Reply::Full; 2]).await;
        let request = request(&origin.url);
        assert_eq!(request.probe_length().await, Some(8192));
        let mut stream = request.open(0);
        assert!(!stream.is_seekable());
        for position in [SeekFrom::Start(0), SeekFrom::Current(0), SeekFrom::End(-1)] {
            assert_eq!(
                stream.seek(position).await.unwrap_err().kind(),
                IoErrorKind::Unsupported
            );
        }
        let mut actual = Vec::new();
        stream.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual, *body, "refusing a seek must not disturb the stream");
        assert_eq!(
            *origin.ranges.lock().unwrap(),
            [(0, 0), (0, CHUNK_SPAN - 1)]
        );
    }

    #[tokio::test]
    async fn the_songbird_adapter_preserves_bytes_across_multiple_chunks() {
        let body = media(2 * READ_AHEAD + 4096);
        let origin = origin(Arc::clone(&body), vec![Reply::Full; 3]).await;
        let mut input = request(&origin.url).create_async().await.unwrap().input;
        assert!(!input.is_seekable());
        let actual = tokio::task::spawn_blocking(move || {
            assert_eq!(
                input.seek(SeekFrom::Start(0)).unwrap_err().kind(),
                IoErrorKind::Unsupported
            );
            let mut bytes = Vec::new();
            input.read_to_end(&mut bytes).unwrap();
            bytes
        })
        .await
        .unwrap();
        assert_eq!(actual, *body);
    }

    #[tokio::test]
    async fn network_audio_opens_and_decodes_without_seeking() {
        let fixtures: [&[u8]; 3] = [
            include_bytes!("../tests/fixtures/tone.webm"),
            include_bytes!("../tests/fixtures/tone.m4a"),
            include_bytes!("../tests/fixtures/tone-fragmented.m4a"),
        ];
        for fixture in fixtures {
            let origin = origin(Arc::new(fixture.to_vec()), vec![Reply::Full; 2]).await;
            let probe = pipeline(&origin.url)
                .probe(&request(&origin.url).track, 250)
                .await
                .unwrap();
            assert!(probe.packets > 0);
            assert!(
                probe.frames >= 10_000,
                "a quarter-second tone must actually decode"
            );
            assert!(probe.reached_end);
            assert_eq!(
                *origin.ranges.lock().unwrap(),
                [(0, CHUNK_SPAN - 1)],
                "no length probe or container seeks"
            );
        }
    }

    #[tokio::test]
    async fn an_origin_ignoring_ranges_still_plays_sequentially() {
        let body = Arc::new(include_bytes!("../tests/fixtures/tone.webm").to_vec());
        let origin = origin_ignoring_ranges(body).await;
        let probe = pipeline(&origin.url)
            .probe(&request(&origin.url).track, 250)
            .await
            .unwrap();
        assert!(probe.frames >= 10_000);
        assert!(probe.reached_end);
    }

    #[tokio::test]
    async fn malformed_audio_is_rejected_during_preparation() {
        let origin = origin(
            Arc::new(b"not an audio container".to_vec()),
            vec![Reply::Full; 2],
        )
        .await;
        let error = pipeline(&origin.url)
            .prepare(&request(&origin.url).track)
            .await
            .err()
            .expect("malformed audio was accepted");
        assert!(format!("{error:#}").contains("failed to open audio stream"));
    }

    #[tokio::test]
    async fn cancelling_preparation_closes_its_pending_network_read() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/media", listener.local_addr().unwrap());
        let (requested, request_seen) = tokio::sync::oneshot::channel();
        let origin = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut byte = [0; 1];
            while !head.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
            }
            requested.send(()).unwrap();
            // Withhold the response. Cancellation must close this connection
            // instead of leaving the blocking parser waiting for a chunk timeout.
            socket.read(&mut byte).await.unwrap()
        });
        let prepare =
            tokio::spawn(async move { pipeline(&url).prepare(&request(&url).track).await });
        time::timeout(Duration::from_secs(2), request_seen)
            .await
            .unwrap()
            .unwrap();
        prepare.abort();
        assert!(
            prepare
                .await
                .err()
                .expect("preparation was not cancelled")
                .is_cancelled()
        );
        let read = time::timeout(Duration::from_secs(2), origin)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, 0);
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
    async fn a_refused_chunk_is_never_asked_for_twice() {
        let body = media(4096);
        // The origin would serve it on a second ask, and is never given one.
        let origin = origin(Arc::clone(&body), vec![Reply::Refuse(403), Reply::Full]).await;

        let mut played = Vec::new();
        let refused = request(&origin.url)
            .open(0)
            .read_to_end(&mut played)
            .await
            .expect_err("a refused chunk must reach the reader as an error");
        assert_eq!(refused.kind(), IoErrorKind::PermissionDenied);

        // A status is the origin's answer. Repeating the identical request only
        // spends time arriving at it again — against the ceiling YouTube
        // enforced for a day, three attempts and three fresh URLs came to
        // twelve requests and forty-five seconds of silence per track, and made
        // a wall look like rate limiting.
        let ranges = origin.ranges.lock().unwrap().clone();
        assert_eq!(ranges.len(), 1, "the refused range was asked for again");
    }

    /// A chunk that never arrived said nothing, so trying again is the remedy.
    #[tokio::test]
    async fn a_chunk_that_never_arrived_is_asked_for_again() {
        let body = media(4096);
        let origin = origin(Arc::clone(&body), vec![Reply::Drop, Reply::Full]).await;

        assert_eq!(play(&request(&origin.url)).await, *body);
        let ranges = origin.ranges.lock().unwrap().clone();
        assert_eq!(ranges.len(), 2, "a dropped connection ended the track");
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
