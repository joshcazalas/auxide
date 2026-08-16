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

    /// Serve every server this application is installed in.
    ///
    /// Installation is then the authorization boundary: with **Public Bot**
    /// disabled in the Discord Developer Portal, only the application's owner
    /// can add it anywhere, so the question of which servers are permitted is
    /// already answered before Auxide connects. That pairing is enforced rather
    /// than assumed — Auxide refuses to start while this is enabled and the
    /// application is still publicly installable.
    ///
    /// Set it to `false` to return to an explicit `guilds` allowlist.
    #[serde(default = "default_allow_all_guilds")]
    pub allow_all_guilds: bool,

    /// Per-server overrides, and the complete allowlist when
    /// [`DiscordConfig::allow_all_guilds`] is `false`.
    ///
    /// A server listed here is narrowed by its entry whether or not every
    /// server is otherwise permitted, so restricting one server does not mean
    /// enumerating the rest.
    #[serde(default)]
    pub guilds: Vec<GuildConfig>,

    /// Upper bound on servers held in memory at once.
    ///
    /// Each one owns a player actor, a playback worker, and a queue, so this
    /// bounds the process instead of leaving it to the systemd memory limit.
    /// Reaching it refuses the request that would have exceeded it and leaves
    /// every existing server working.
    #[serde(default = "default_max_guilds")]
    pub max_guilds: usize,
}

fn default_allow_all_guilds() -> bool {
    true
}

fn default_max_guilds() -> usize {
    50
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
            listen_address: "127.0.0.1:9090".parse().expect("valid default address"),
            json_logs: true,
            log_filter: "auxide=info,serenity=info,songbird=info".to_owned(),
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
        if !self.discord.allow_all_guilds && self.discord.guilds.is_empty() {
            return Err(ConfigError::Validation(
                "discord.guilds must list at least one server when discord.allow_all_guilds is false, \
                 or Auxide would refuse every request"
                    .to_owned(),
            ));
        }
        if self.discord.max_guilds == 0 || self.discord.max_guilds > 1_000 {
            return Err(ConfigError::Validation(
                "discord.max_guilds must be between 1 and 1000".to_owned(),
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
        if self.playback.max_queue_length > 1_000 {
            return Err(ConfigError::Validation(
                "playback.max_queue_length must not exceed 1000".to_owned(),
            ));
        }
        if self.playback.actor_mailbox_capacity > 4_096 {
            return Err(ConfigError::Validation(
                "playback.actor_mailbox_capacity must not exceed 4096".to_owned(),
            ));
        }
        if self.playback.max_concurrent_resolutions > 16 {
            return Err(ConfigError::Validation(
                "playback.max_concurrent_resolutions must not exceed 16".to_owned(),
            ));
        }
        if self.playback.max_track_duration_seconds > 24 * 60 * 60
            || self.playback.idle_timeout_seconds > 24 * 60 * 60
        {
            return Err(ConfigError::Validation(
                "playback durations and timeouts must not exceed 24 hours".to_owned(),
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
            if self.youtube.max_output_bytes > 16 * 1024 * 1024 {
                return Err(ConfigError::Validation(
                    "youtube.max_output_bytes must not exceed 16777216".to_owned(),
                ));
            }
        }

        if self.observability.log_filter.trim().is_empty() {
            return Err(ConfigError::Validation(
                "observability.log_filter must not be empty".to_owned(),
            ));
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

    /// Reports whether Auxide answers requests from a server at all.
    ///
    /// A server with its own entry is always permitted. Otherwise this is the
    /// [`DiscordConfig::allow_all_guilds`] decision, which is what makes an
    /// unlisted server usable without editing configuration.
    #[must_use]
    pub fn allows_guild(&self, guild_id: u64) -> bool {
        self.discord.allow_all_guilds || self.guild(guild_id).is_some()
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

    #[test]
    fn rejects_unbounded_resource_configuration() {
        let source = r#"
[discord]
token_file = "/run/secrets/discord-token"

[[discord.guilds]]
guild_id = 123

[playback]
max_queue_length = 1001
"#;
        let config: Config = toml::from_str(source).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn serves_every_guild_without_a_configured_list() {
        let source = r#"
[discord]
token_file = "/run/secrets/discord-token"
"#;
        let config: Config = toml::from_str(source).unwrap();
        config.validate().unwrap();
        assert!(config.discord.allow_all_guilds);
        assert!(config.discord.guilds.is_empty());
        assert!(config.allows_guild(730_675_093_197_422_623));
        assert!(config.guild(730_675_093_197_422_623).is_none());
    }

    #[test]
    fn an_explicit_allowlist_still_refuses_unlisted_guilds() {
        let source = r#"
[discord]
token_file = "/run/secrets/discord-token"
allow_all_guilds = false

[[discord.guilds]]
guild_id = 123
"#;
        let config: Config = toml::from_str(source).unwrap();
        config.validate().unwrap();
        assert!(config.allows_guild(123));
        assert!(!config.allows_guild(456));
    }

    #[test]
    fn an_empty_allowlist_that_permits_nothing_is_a_configuration_error() {
        let source = r#"
[discord]
token_file = "/run/secrets/discord-token"
allow_all_guilds = false
"#;
        let config: Config = toml::from_str(source).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn a_listed_guild_keeps_its_restrictions_while_serving_every_guild() {
        let source = r#"
[discord]
token_file = "/run/secrets/discord-token"

[[discord.guilds]]
guild_id = 123
authorized_user_ids = [7]
"#;
        let config: Config = toml::from_str(source).unwrap();
        config.validate().unwrap();
        assert!(config.allows_guild(456));
        let guild = config.guild(123).unwrap();
        assert!(guild.authorized_user_ids.contains(&7));
    }

    #[test]
    fn rejects_an_unusable_guild_ceiling() {
        for ceiling in ["0", "1001"] {
            let source = format!(
                r#"
[discord]
token_file = "/run/secrets/discord-token"
max_guilds = {ceiling}
"#
            );
            let config: Config = toml::from_str(&source).unwrap();
            assert!(
                config.validate().is_err(),
                "accepted max_guilds = {ceiling}"
            );
        }
    }
}
