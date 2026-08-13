use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context as _, Result, anyhow, bail};
use reqwest::{
    Client as HttpClient,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use secrecy::ExposeSecret;
use serenity::{
    Client,
    all::{ChannelId, GatewayIntents, GuildId, Ready},
    async_trait,
    client::{Context, EventHandler},
};
use songbird::{
    Event, EventContext, EventHandler as VoiceEventHandler, SerenityInit, Songbird, TrackEvent,
    input::{File, HlsRequest, HttpRequest, Input},
    tracks::PlayMode,
};
use tokio::{sync::oneshot, task::JoinHandle, time};
use url::Url;

use crate::{
    config::Config,
    source::{SourceResolver, YouTubeResolver},
};

const GATEWAY_READY_TIMEOUT: Duration = Duration::from_secs(30);
const PLAYBACK_GRACE: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub enum VoiceSpikeSource {
    File(PathBuf),
    YouTube(Url),
}

#[derive(Debug)]
struct ReadyHandler {
    ready: tokio::sync::Mutex<Option<oneshot::Sender<()>>>,
}

#[async_trait]
impl EventHandler for ReadyHandler {
    async fn ready(&self, _ctx: Context, ready: Ready) {
        tracing::info!(bot_user = %ready.user.name, "Discord gateway is ready");
        if let Some(ready) = self.ready.lock().await.take() {
            let _ = ready.send(());
        }
    }
}

#[derive(Debug)]
struct TrackFinished {
    finished: tokio::sync::Mutex<Option<oneshot::Sender<Result<(), String>>>>,
}

#[async_trait]
impl VoiceEventHandler for TrackFinished {
    async fn act(&self, context: &EventContext<'_>) -> Option<Event> {
        let result = if let EventContext::Track(tracks) = context {
            match *tracks {
                [(state, _)] => match &state.playing {
                    PlayMode::Errored(error) => Err(format!("{error}")),
                    _ => Ok(()),
                },
                _ => Err("Songbird returned an unexpected track count".to_owned()),
            }
        } else {
            Err("Songbird returned an unexpected track event".to_owned())
        };
        if let Some(finished) = self.finished.lock().await.take() {
            let _ = finished.send(result);
        }
        Some(Event::Cancel)
    }
}

/// Runs a one-track, non-interactive Discord voice validation.
///
/// The caller must provide an already-created guild and voice channel. This function never
/// registers application commands or creates Discord resources.
///
/// # Errors
///
/// Returns an error if the guild is not allowlisted, source preparation fails, Discord cannot
/// connect, voice playback fails, or a safety timeout expires.
pub async fn run_voice_spike(
    config: &Config,
    guild_id: u64,
    channel_id: u64,
    source: VoiceSpikeSource,
) -> Result<()> {
    if guild_id == 0 || channel_id == 0 {
        bail!("guild and channel IDs must be non-zero Discord snowflakes");
    }
    if config.guild(guild_id).is_none() {
        bail!("guild {guild_id} is not in the configuration allowlist");
    }

    let token = config
        .load_discord_token()
        .context("failed to load the Discord token")?;
    let input = prepare_input(config, source).await?;
    let voice = Songbird::serenity();
    let (ready_tx, ready_rx) = oneshot::channel();
    let handler = ReadyHandler {
        ready: tokio::sync::Mutex::new(Some(ready_tx)),
    };
    let intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES;
    let mut client = Client::builder(token.expose_secret(), intents)
        .event_handler(handler)
        .register_songbird_with(Arc::clone(&voice))
        .await
        .context("failed to construct the Discord client")?;
    let shard_manager = Arc::clone(&client.shard_manager);
    let mut client_task = tokio::spawn(async move { client.start().await });

    let result = run_after_gateway_ready(
        config,
        GuildId::new(guild_id),
        ChannelId::new(channel_id),
        input,
        Arc::clone(&voice),
        ready_rx,
        &mut client_task,
    )
    .await;

    if let Err(error) = voice.remove(GuildId::new(guild_id)).await {
        tracing::warn!(%error, guild_id, "failed to leave voice cleanly");
    }
    shard_manager.shutdown_all().await;
    match time::timeout(Duration::from_secs(5), client_task).await {
        Ok(Ok(Err(error))) if result.is_ok() => {
            return Err(error).context("Discord client stopped unexpectedly");
        }
        Ok(Err(error)) if result.is_ok() => {
            return Err(error).context("Discord client task failed");
        }
        Err(_) => tracing::warn!("Discord client did not stop within the cleanup timeout"),
        _ => {}
    }

    result
}

async fn run_after_gateway_ready(
    config: &Config,
    guild_id: GuildId,
    channel_id: ChannelId,
    input: Input,
    voice: Arc<Songbird>,
    ready: oneshot::Receiver<()>,
    client_task: &mut JoinHandle<serenity::Result<()>>,
) -> Result<()> {
    tokio::select! {
        ready = ready => ready.context("Discord gateway stopped before becoming ready")?,
        client = &mut *client_task => {
            return match client {
                Ok(Ok(())) => Err(anyhow!("Discord client stopped before becoming ready")),
                Ok(Err(error)) => Err(error).context("Discord client failed before becoming ready"),
                Err(error) => Err(error).context("Discord client task failed before becoming ready"),
            };
        }
        () = time::sleep(GATEWAY_READY_TIMEOUT) => {
            bail!("Discord gateway did not become ready within {GATEWAY_READY_TIMEOUT:?}");
        }
        signal = shutdown_signal() => {
            bail!("received {signal} before Discord gateway became ready");
        }
    }

    tracing::info!(
        guild_id = guild_id.get(),
        channel_id = channel_id.get(),
        "joining voice"
    );
    let call = voice
        .join(guild_id, channel_id)
        .await
        .context("failed to join the Discord voice channel")?;
    tracing::info!(
        guild_id = guild_id.get(),
        channel_id = channel_id.get(),
        "voice connection established"
    );

    let handle = call.lock().await.play_only_input(input);
    let (finished_tx, finished_rx) = oneshot::channel();
    handle
        .add_event(
            Event::Track(TrackEvent::End),
            TrackFinished {
                finished: tokio::sync::Mutex::new(Some(finished_tx)),
            },
        )
        .context("failed to subscribe to track completion")?;

    let playback_timeout = Duration::from_secs(config.playback.max_track_duration_seconds)
        .saturating_add(PLAYBACK_GRACE);
    tracing::info!(?playback_timeout, "voice playback started");
    let result = tokio::select! {
        finished = finished_rx => match finished {
            Ok(Ok(())) => {
                tracing::info!("voice playback completed");
                Ok(())
            }
            Ok(Err(error)) => Err(anyhow!("voice playback failed: {error}")),
            Err(_) => Err(anyhow!("voice playback completion event was dropped")),
        },
        client = &mut *client_task => match client {
            Ok(Ok(())) => Err(anyhow!("Discord client stopped during voice playback")),
            Ok(Err(error)) => Err(error).context("Discord client failed during voice playback"),
            Err(error) => Err(error).context("Discord client task failed during voice playback"),
        },
        () = time::sleep(playback_timeout) => {
            Err(anyhow!("voice playback exceeded its {playback_timeout:?} safety timeout"))
        }
        signal = shutdown_signal() => {
            tracing::info!(signal, "shutdown requested during voice playback");
            Ok(())
        }
    };
    let _ = handle.stop();
    result
}

async fn prepare_input(config: &Config, source: VoiceSpikeSource) -> Result<Input> {
    match source {
        VoiceSpikeSource::File(path) => {
            let path = tokio::fs::canonicalize(&path).await.with_context(|| {
                format!("failed to resolve local audio file {}", path.display())
            })?;
            let metadata = tokio::fs::metadata(&path).await.with_context(|| {
                format!("failed to inspect local audio file {}", path.display())
            })?;
            if !metadata.is_file() {
                bail!(
                    "local audio source is not a regular file: {}",
                    path.display()
                );
            }
            Ok(File::new(path).into())
        }
        VoiceSpikeSource::YouTube(url) => {
            let resolver = YouTubeResolver::new(config.youtube.clone(), &config.playback);
            let metadata = resolver.inspect(&url).await?;
            tracing::info!(
                source_id = %metadata.source_id,
                duration_seconds = metadata.duration.as_secs(),
                title = %metadata.title,
                "YouTube source inspected"
            );
            let audio = resolver.resolve(&metadata).await?;
            let headers = convert_headers(&audio.headers)?;
            let client = HttpClient::builder()
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
                .user_agent("discord-music-bot/0.1")
                .build()
                .context("failed to build the media HTTP client")?;
            match audio.protocol.as_deref() {
                None | Some("https") => Ok(HttpRequest::new_with_headers(
                    client,
                    audio.stream_url.to_string(),
                    headers,
                )
                .into()),
                Some("m3u8" | "m3u8_native") => {
                    Ok(
                        HlsRequest::new_with_headers(client, audio.stream_url.to_string(), headers)
                            .into(),
                    )
                }
                Some(protocol) => bail!("yt-dlp selected unsupported media protocol {protocol:?}"),
            }
        }
    }
}

fn convert_headers(headers: &std::collections::BTreeMap<String, String>) -> Result<HeaderMap> {
    let mut converted = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("yt-dlp returned an invalid HTTP header name: {name:?}"))?;
        let value = HeaderValue::from_str(value)
            .with_context(|| format!("yt-dlp returned an invalid value for header {name}"))?;
        converted.insert(name, value);
    }
    Ok(converted)
}

#[cfg(unix)]
async fn shutdown_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).expect("SIGTERM handler is available");
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.expect("SIGINT handler is available");
            "SIGINT"
        }
        _ = terminate.recv() => "SIGTERM",
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> &'static str {
    tokio::signal::ctrl_c()
        .await
        .expect("interrupt handler is available");
    "interrupt"
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

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

    #[test]
    fn rejects_header_injection() {
        let headers = BTreeMap::from([("X-Test".to_owned(), "ok\r\nevil: true".to_owned())]);
        assert!(convert_headers(&headers).is_err());
    }
}
