use std::{
    collections::BTreeSet,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use secrecy::SecretString;
use serde::Deserialize;
use thiserror::Error;

const MAX_SECRET_BYTES: u64 = 4 * 1024;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub discord: DiscordConfig,
    #[serde(default)]
    pub playback: PlaybackConfig,
    #[serde(default)]
    pub youtube: YouTubeConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscordConfig {
    pub token_file: PathBuf,
    pub guilds: Vec<GuildConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuildConfig {
    pub guild_id: u64,
    #[serde(default)]
    pub command_channel_ids: BTreeSet<u64>,
    #[serde(default)]
    pub authorized_role_ids: BTreeSet<u64>,
    #[serde(default)]
    pub authorized_user_ids: BTreeSet<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PlaybackConfig {
    pub idle_timeout_seconds: u64,
    pub max_track_duration_seconds: u64,
    pub max_queue_length: usize,
    pub actor_mailbox_capacity: usize,
    pub max_concurrent_resolutions: usize,
}

impl Default for PlaybackConfig {
    fn default() -> Self {
        Self {
            idle_timeout_seconds: 10 * 60,
            max_track_duration_seconds: 4 * 60 * 60,
            max_queue_length: 100,
            actor_mailbox_capacity: 128,
            max_concurrent_resolutions: 2,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct YouTubeConfig {
    pub enabled: bool,
    pub yt_dlp_path: PathBuf,
    pub deno_path: PathBuf,
    pub search_results: usize,
    pub resolution_timeout_seconds: u64,
    pub max_output_bytes: usize,
}

impl Default for YouTubeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            yt_dlp_path: PathBuf::from("yt-dlp"),
            deno_path: PathBuf::from("deno"),
            search_results: 5,
            resolution_timeout_seconds: 20,
            max_output_bytes: 1024 * 1024,
        }
    }
}

impl YouTubeConfig {
    #[must_use]
    pub const fn resolution_timeout(&self) -> Duration {
        Duration::from_secs(self.resolution_timeout_seconds)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ObservabilityConfig {
    pub listen_address: SocketAddr,
    pub json_logs: bool,
    pub log_filter: String,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            listen_address: "0.0.0.0:9090".parse().expect("valid default address"),
            json_logs: true,
            log_filter: "discord_music_bot=info,serenity=info,songbird=info".to_owned(),
        }
    }
}

impl Config {
    /// Loads and validates configuration from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, TOML cannot be decoded, or a configured
    /// value violates a validation rule.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let source = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let config: Self = toml::from_str(&source).map_err(ConfigError::Parse)?;
        config.validate()?;
        Ok(config)
    }

    /// Checks fail-closed allowlists and all configured resource bounds.
    ///
    /// # Errors
    ///
    /// Returns a validation error for duplicate or invalid Discord IDs and for zero or
    /// out-of-range resource limits.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.discord.guilds.is_empty() {
            return Err(ConfigError::Validation(
                "discord.guilds must contain at least one allowlisted guild".to_owned(),
            ));
        }

        let mut guild_ids = BTreeSet::new();
        for guild in &self.discord.guilds {
            if guild.guild_id == 0 {
                return Err(ConfigError::Validation(
                    "guild IDs must be non-zero Discord snowflakes".to_owned(),
                ));
            }
            if !guild_ids.insert(guild.guild_id) {
                return Err(ConfigError::Validation(format!(
                    "guild {} is configured more than once",
                    guild.guild_id
                )));
            }
            if guild.command_channel_ids.contains(&0)
                || guild.authorized_role_ids.contains(&0)
                || guild.authorized_user_ids.contains(&0)
            {
                return Err(ConfigError::Validation(format!(
                    "guild {} contains a zero-valued Discord ID",
                    guild.guild_id
                )));
            }
        }

        if self.playback.max_queue_length == 0
            || self.playback.actor_mailbox_capacity == 0
            || self.playback.max_concurrent_resolutions == 0
            || self.playback.max_track_duration_seconds == 0
            || self.playback.idle_timeout_seconds == 0
        {
            return Err(ConfigError::Validation(
                "all playback bounds and timeouts must be greater than zero".to_owned(),
            ));
        }

        if self.youtube.enabled {
            if !(1..=10).contains(&self.youtube.search_results) {
                return Err(ConfigError::Validation(
                    "youtube.search_results must be between 1 and 10".to_owned(),
                ));
            }
            if self.youtube.resolution_timeout_seconds == 0 {
                return Err(ConfigError::Validation(
                    "youtube.resolution_timeout_seconds must be greater than zero".to_owned(),
                ));
            }
            if self.youtube.max_output_bytes < 16 * 1024 {
                return Err(ConfigError::Validation(
                    "youtube.max_output_bytes must be at least 16384".to_owned(),
                ));
            }
        }

        Ok(())
    }

    /// Loads the Discord token from the configured runtime secret file.
    ///
    /// # Errors
    ///
    /// Returns an error when the secret file cannot be read, is empty, or exceeds the size cap.
    pub fn load_discord_token(&self) -> Result<SecretString, ConfigError> {
        read_secret(&self.discord.token_file)
    }

    #[must_use]
    pub fn guild(&self, guild_id: u64) -> Option<&GuildConfig> {
        self.discord
            .guilds
            .iter()
            .find(|guild| guild.guild_id == guild_id)
    }
}

fn read_secret(path: &Path) -> Result<SecretString, ConfigError> {
    let metadata = fs::metadata(path).map_err(|source| ConfigError::SecretRead {
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.len() > MAX_SECRET_BYTES {
        return Err(ConfigError::Validation(format!(
            "secret file {} exceeds {MAX_SECRET_BYTES} bytes",
            path.display()
        )));
    }

    let value = fs::read_to_string(path).map_err(|source| ConfigError::SecretRead {
        path: path.to_path_buf(),
        source,
    })?;
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Err(ConfigError::Validation(format!(
            "secret file {} is empty",
            path.display()
        )));
    }

    Ok(SecretString::new(value))
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read configuration {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid TOML configuration: {0}")]
    Parse(#[source] toml::de::Error),
    #[error("failed to read secret file {path}: {source}")]
    SecretRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid configuration: {0}")]
    Validation(String),
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use secrecy::ExposeSecret;
    use tempfile::NamedTempFile;

    use super::*;

    fn valid_config(token_path: &Path) -> String {
        format!(
            r#"
[discord]
token_file = "{}"

[[discord.guilds]]
guild_id = 123
command_channel_ids = [456]

[youtube]
enabled = true
"#,
            token_path.display()
        )
    }

    #[test]
    fn loads_valid_config_and_secret_without_exposing_it_in_config() {
        let mut token = NamedTempFile::new().unwrap();
        writeln!(token, "not-a-real-token").unwrap();
        let mut config = NamedTempFile::new().unwrap();
        write!(config, "{}", valid_config(token.path())).unwrap();

        let loaded = Config::load(config.path()).unwrap();
        assert_eq!(loaded.discord.guilds[0].guild_id, 123);
        assert_eq!(
            loaded.load_discord_token().unwrap().expose_secret(),
            "not-a-real-token"
        );
        assert!(!format!("{loaded:?}").contains("not-a-real-token"));
    }

    #[test]
    fn rejects_duplicate_guilds() {
        let source = r#"
[discord]
token_file = "/run/secrets/discord-token"

[[discord.guilds]]
guild_id = 123

[[discord.guilds]]
guild_id = 123
"#;
        let config: Config = toml::from_str(source).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_unknown_fields() {
        let source = r#"
[discord]
token_file = "/run/secrets/discord-token"
surprise = true

[[discord.guilds]]
guild_id = 123
"#;
        assert!(toml::from_str::<Config>(source).is_err());
    }
}
