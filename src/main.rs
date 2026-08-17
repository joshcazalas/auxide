use std::path::PathBuf;

use anyhow::{Context, Result};
use auxide::{
    config::{Config, ObservabilityConfig},
    discord::{register_commands, run},
    source::{SourceResolver, YouTubeResolver},
    spike::{VoiceSpikeSource, run_voice_spike},
};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    #[arg(long, env = "AUXIDE_CONFIG", default_value = "config.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::doc_markdown)] // These comments are rendered verbatim as CLI help.
enum Command {
    /// Run the long-lived Discord bot and private observability server.
    Run,
    /// Replace this application's slash commands, for every server it is installed in.
    RegisterCommands {
        /// Also clear leftover per-server commands from this server.
        #[arg(long)]
        guild_id: Option<u64>,
    },
    /// Parse and validate configuration without loading the Discord token.
    CheckConfig,
    /// Exercise public YouTube metadata resolution without joining Discord.
    #[command(name = "youtube-inspect")]
    YouTubeInspect {
        #[arg(value_parser = parse_url)]
        url: Url,
    },
    /// Exercise public YouTube search without joining Discord.
    #[command(name = "youtube-search")]
    YouTubeSearch { query: String },
    /// Expand a public YouTube playlist without joining Discord.
    #[command(name = "youtube-playlist")]
    YouTubePlaylist {
        #[arg(value_parser = parse_url)]
        url: Url,
    },
    /// Join an existing voice channel and play one source without registering commands.
    VoiceSpike {
        #[arg(long)]
        guild_id: u64,
        #[arg(long)]
        channel_id: u64,
        #[command(subcommand)]
        source: VoiceSpikeSourceArgs,
    },
}

#[derive(Debug, Subcommand)]
#[allow(clippy::doc_markdown)] // These comments are rendered verbatim as CLI help.
enum VoiceSpikeSourceArgs {
    /// Play a regular local audio file as the control case.
    File { path: PathBuf },
    /// Resolve and stream a single non-live YouTube video.
    #[command(name = "youtube")]
    YouTube {
        #[arg(value_parser = parse_url)]
        url: Url,
    },
}

fn parse_url(value: &str) -> Result<Url, String> {
    Url::parse(value).map_err(|error| error.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(&cli.config)
        .with_context(|| format!("failed to load {}", cli.config.display()))?;
    init_logging(&config.observability)?;

    match cli.command {
        Command::Run => run(config).await?,
        Command::RegisterCommands { guild_id } => register_commands(&config, guild_id).await?,
        Command::CheckConfig => {
            tracing::info!(path = %cli.config.display(), "configuration is valid");
        }
        Command::YouTubeInspect { url } => {
            let resolver = YouTubeResolver::new(config.youtube.clone(), &config.playback);
            let track = resolver.inspect(&url).await?;
            println!(
                "{}\t{}\t{}",
                track.source_id,
                track.duration.as_secs(),
                track.title
            );
        }
        Command::YouTubeSearch { query } => {
            let resolver = YouTubeResolver::new(config.youtube.clone(), &config.playback);
            for track in resolver.search(&query).await? {
                println!(
                    "{}\t{}\t{}",
                    track.source_id,
                    track.duration.as_secs(),
                    track.title
                );
            }
        }
        Command::YouTubePlaylist { url } => {
            let resolver = YouTubeResolver::new(config.youtube.clone(), &config.playback);
            let playlist = resolver
                .playlist(&url)
                .await?
                .context("that URL is not a playlist")?;
            println!(
                "{}\t{} of {} playable",
                playlist.title.as_deref().unwrap_or("(untitled)"),
                playlist.tracks.len(),
                playlist.total
            );
            for track in playlist.tracks {
                println!(
                    "{}\t{}\t{}",
                    track.source_id,
                    track.duration.as_secs(),
                    track.title
                );
            }
        }
        Command::VoiceSpike {
            guild_id,
            channel_id,
            source,
        } => {
            let source = match source {
                VoiceSpikeSourceArgs::File { path } => VoiceSpikeSource::File(path),
                VoiceSpikeSourceArgs::YouTube { url } => VoiceSpikeSource::YouTube(url),
            };
            run_voice_spike(&config, guild_id, channel_id, source).await?;
        }
    }

    Ok(())
}

fn init_logging(config: &ObservabilityConfig) -> Result<()> {
    let filter = EnvFilter::try_new(&config.log_filter)
        .with_context(|| format!("invalid log filter: {}", config.log_filter))?;
    if config.json_logs {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .try_init()
            .map_err(|error| anyhow::anyhow!("failed to initialize JSON logging: {error}"))?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .map_err(|error| anyhow::anyhow!("failed to initialize logging: {error}"))?;
    }
    Ok(())
}
