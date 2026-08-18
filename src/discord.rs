use std::{
    collections::{BTreeSet, HashMap},
    fmt::Write as _,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, bail};
use rand::random;
use secrecy::ExposeSecret;
use serenity::{
    async_trait,
    builder::{
        CreateActionRow, CreateAllowedMentions, CreateAttachment, CreateAutocompleteResponse,
        CreateButton, CreateCommand, CreateCommandOption, CreateEmbed, CreateEmbedAuthor,
        CreateEmbedFooter, CreateInteractionResponse, CreateInteractionResponseFollowup,
        CreateInteractionResponseMessage, CreateMessage, EditInteractionResponse,
    },
    client::ClientBuilder,
    client::{Context, EventHandler},
    gateway::{ConnectionStage, ShardStageUpdateEvent},
    http::{Http, HttpBuilder},
    model::application::{
        Command, CommandDataOptionValue, CommandInteraction, CommandOptionType,
        ComponentInteraction, Interaction,
    },
    model::gateway::Ready,
    model::id::{ChannelId, GuildId, UserId},
    model::voice::VoiceState,
};
use songbird::{SerenityInit, Songbird};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
    time,
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{
    audio::AudioPipeline,
    config::{Config, GuildConfig, MAX_VOLUME_PERCENT},
    observability::{ObservabilityServer, ObservabilityState, start_observability},
    player::{
        GuildPlayerHandle, Hold, PlaybackDirective, PlayerSnapshot, PlayerTransition, QueueItem,
        Release, RepeatMode, ShuffleChange, SkipVerdict, spawn_guild_player,
    },
    source::{Playlist, SourceResolver, TrackMetadata, YouTubeResolver},
    suggest::Suggestions,
    voice::{SeekTo, SongbirdVoice, TrackEnded, VoiceGateway, VoiceGatewayFactory},
};

const SEARCH_SELECTION_TTL: Duration = Duration::from_secs(120);
const MAX_PENDING_SEARCHES: usize = 128;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);

/// The colour on every Auxide embed, so a channel reads them as one voice.
const EMBED_COLOUR: u32 = 0x00B5_732E;

// Discord's own embed limits are 256 characters for a title, 2048 for a footer,
// and 4096 for a description. Auxide stays well inside all three, because a
// title long enough to reach the limit is unreadable before it is rejected.
const EMBED_TITLE_CHARS: usize = 200;
const EMBED_FOOTER_CHARS: usize = 100;
const EMBED_DESCRIPTION_CHARS: usize = 3_800;

/// The largest file `/import` will read.
///
/// A full hundred-track export runs to a few tens of kilobytes, so this leaves
/// room to spare while keeping a hostile upload from being read at all.
const MAX_IMPORT_BYTES: u32 = 512 * 1024;

/// Registers the complete Auxide command set for this application.
///
/// Registration is deliberately separate from normal startup. It replaces only this
/// application's own commands and never creates channels or roles.
///
/// Commands are registered globally, so a server added later needs no further
/// action here. Discord can take up to an hour to propagate a *change* to the
/// global set, though a newly installed server receives the current set at once.
///
/// `clear_guild` additionally removes leftover per-server commands from one
/// server, for a server no longer named in configuration.
///
/// # Errors
///
/// Returns an error when the token cannot be loaded, the application is publicly
/// installable while every server is served, or Discord rejects a command
/// definition.
pub async fn register_commands(config: &Config, clear_guild: Option<u64>) -> Result<()> {
    let token = config
        .load_discord_token()
        .context("failed to load the Discord token")?;
    let http = Http::new(token.expose_secret());

    // Every command route is built from the application id, and `Http::new`
    // leaves it unset. A normal run receives it on the gateway's Ready event,
    // but registration deliberately never opens a gateway, so resolve the
    // identity behind this token directly.
    let application = http
        .get_current_application_info()
        .await
        .context("failed to identify the Discord application behind this token")?;
    http.set_application_id(application.id);
    ensure_installation_is_a_boundary(config, application.bot_public)?;

    // Registering globally is what lets a newly installed server work without
    // anyone editing configuration: Discord offers an application's global
    // commands everywhere it is installed, including servers added later.
    let registered = Command::set_global_commands(&http, command_definitions())
        .await
        .context("failed to register global commands")?;
    tracing::info!(commands = registered.len(), "registered global commands");

    // Earlier versions registered per server. Those survive independently of
    // the global set and would show up beside it as duplicates, so clear the
    // ones we still know to look for.
    let stale: BTreeSet<u64> = config
        .discord
        .guilds
        .iter()
        .map(|guild| guild.guild_id)
        .chain(clear_guild)
        .collect();
    for guild_id in stale {
        GuildId::new(guild_id)
            .set_commands(&http, Vec::new())
            .await
            .with_context(|| format!("failed to clear guild commands in {guild_id}"))?;
        tracing::info!(guild_id, "cleared guild-scoped commands");
    }
    Ok(())
}

/// Refuses the combination that would let anyone use this bot.
///
/// Serving every installed server is safe precisely because only the owner can
/// install it. If the application is also publicly installable, that reasoning
/// collapses: anyone could add it and drive it. Rather than trust the two
/// settings to be kept in agreement by hand, treat disagreement as a
/// configuration error.
fn ensure_installation_is_a_boundary(config: &Config, bot_public: bool) -> Result<()> {
    if config.discord.allow_all_guilds && bot_public {
        bail!(
            "this application is publicly installable while discord.allow_all_guilds serves every \
             server, so anyone could add Auxide and control it. Disable Public Bot on the Bot page \
             of the Discord Developer Portal, or set discord.allow_all_guilds = false and list the \
             permitted servers in discord.guilds."
        );
    }
    Ok(())
}

/// Runs the long-lived Discord gateway, voice workers, and private observability server.
///
/// # Errors
///
/// Returns an error when startup fails or the Discord client exits unexpectedly.
pub async fn run(config: Config) -> Result<()> {
    run_with(config, Overrides::default()).await
}

/// What `run` reaches outside this process, and a way to replace it.
///
/// Every field is empty in production. A test fills them to run the real
/// runtime — the real gateway handling, the real command dispatch, the real
/// interaction lifecycle — against a Discord that does not exist, a source that
/// answers from memory, and a voice channel that only records what it was told.
#[derive(Default)]
pub struct Overrides {
    pub source: Option<Arc<dyn SourceResolver>>,
    pub voice: Option<Arc<dyn VoiceGatewayFactory>>,
    /// Where Discord's HTTP API lives, when it is not Discord.
    ///
    /// Serenity asks this for the gateway address too, so redirecting it is
    /// enough to redirect the websocket as well.
    pub api_base: Option<String>,
}

/// Runs the bot with some of what it reaches outside this process replaced.
///
/// # Errors
///
/// Returns whatever [`run`] would, since this is the body of it.
pub async fn run_with(config: Config, overrides: Overrides) -> Result<()> {
    let config = Arc::new(config);
    let token = config
        .load_discord_token()
        .context("failed to load the Discord token")?;

    // Check before opening a listener or a gateway, so an application that
    // anyone could add never reaches the point of accepting commands.
    let application = build_http(token.expose_secret(), overrides.api_base.as_deref())
        .get_current_application_info()
        .await
        .context("failed to identify the Discord application behind this token")?;
    ensure_installation_is_a_boundary(&config, application.bot_public)?;
    tracing::info!(
        application = %application.name,
        "resolved the Discord application"
    );

    let cancellation = CancellationToken::new();
    let observability = ObservabilityState::default();
    let observability_server = start_observability(
        config.observability.listen_address,
        observability.clone(),
        cancellation.child_token(),
    )
    .await
    .context("failed to start the observability listener")?;
    tracing::info!(address = %observability_server.local_address, "observability listener ready");

    let voice = Songbird::serenity();
    let resolver = overrides.source.unwrap_or_else(|| {
        Arc::new(YouTubeResolver::new(
            config.youtube.clone(),
            &config.playback,
        ))
    });
    let voice_factory = match overrides.voice {
        Some(factory) => factory,
        None => Arc::new(SongbirdVoice::new(
            Arc::clone(&voice),
            AudioPipeline::new(Arc::clone(&resolver), config.playback.output_volume)?,
        )),
    };
    let runtime = Arc::new(BotRuntime::new(
        Arc::clone(&config),
        resolver,
        voice_factory,
        observability.clone(),
        cancellation.child_token(),
    ));
    let handler = DiscordHandler {
        runtime: Arc::clone(&runtime),
    };
    let intents = serenity::model::gateway::GatewayIntents::GUILDS
        | serenity::model::gateway::GatewayIntents::GUILD_VOICE_STATES;
    let mut client = ClientBuilder::new_with_http(
        build_http(token.expose_secret(), overrides.api_base.as_deref()),
        intents,
    )
    .event_handler(handler)
    .register_songbird_with(Arc::clone(&voice))
    .await
    .context("failed to construct the Discord client")?;
    let shard_manager = Arc::clone(&client.shard_manager);
    let mut client_task = tokio::spawn(async move { client.start().await });

    let client_result = tokio::select! {
        result = &mut client_task => Some(result),
        signal = shutdown_signal() => {
            tracing::info!(signal, "shutdown requested");
            None
        }
    };

    observability.set_ready(false);
    observability.set_discord_connected(false);
    runtime.shutdown().await;
    shard_manager.shutdown_all().await;
    cancellation.cancel();

    if client_result.is_none()
        && time::timeout(Duration::from_secs(5), &mut client_task)
            .await
            .is_err()
    {
        tracing::warn!("Discord client did not stop within five seconds");
        client_task.abort();
    }
    wait_observability(observability_server).await?;

    if let Some(result) = client_result {
        match result {
            Ok(Ok(())) => bail!("Discord client stopped unexpectedly"),
            Ok(Err(error)) => return Err(error).context("Discord client failed"),
            Err(error) => return Err(error).context("Discord client task failed"),
        }
    }
    Ok(())
}

async fn wait_observability(server: ObservabilityServer) -> Result<()> {
    time::timeout(Duration::from_secs(5), server.wait())
        .await
        .context("observability server did not stop within five seconds")??;
    Ok(())
}

/// Builds an HTTP client, optionally pointed somewhere other than Discord.
///
/// The rate limiter goes off with a redirected base, because it is Discord's
/// budget it is tracking and a local stand-in has none.
fn build_http(token: &str, api_base: Option<&str>) -> Http {
    let mut builder = HttpBuilder::new(token);
    if let Some(base) = api_base {
        builder = builder.proxy(base).ratelimiter_disabled(true);
    }
    builder.build()
}

/// Every command Auxide registers.
///
/// Which members of a server may run which of these is deliberately not decided
/// here. Discord already has a per-command permission editor, an audit log, and
/// somewhere for a server's own administrators to reach both — so these are
/// registered open, and narrowing them is something a server does to itself
/// without anybody touching a file on the host.
///
/// The one restriction applied to all of them is that they are useless outside
/// a server. Auxide has a queue, a voice channel, and a room per server; a
/// direct message has none of those, so a command offered there could only ever
/// be refused.
fn command_definitions() -> Vec<CreateCommand> {
    playing_commands()
        .into_iter()
        .chain(queue_commands())
        .chain([
            CreateCommand::new("help").description("What Auxide can do, and who sees each answer")
        ])
        .filter(|command| {
            described(command).is_none_or(|(name, _)| !DISABLED_COMMANDS.contains(&name.as_str()))
        })
        .map(|command| command.dm_permission(false))
        .collect()
}

/// Commands built but not offered.
///
/// Moving the playhead is the one thing Auxide does that reaches into a
/// half-decoded container rather than into its own state, and it is the only
/// part that has taken the process down: seeking a `WebM` stream can trip an
/// assertion inside Symphonia's Matroska reader, on a mixer thread, where a
/// panic is nobody's to catch. The rest of the failures were milder but the
/// same shape — a seek landing somewhere the decoder could not resume from,
/// reported as a track that cannot be seeked in.
///
/// So they are withdrawn rather than repaired, for now. A queue that always
/// plays is worth more than a playhead that sometimes moves, and every one of
/// these is a convenience nobody had asked for. The handlers, the argument
/// parsing, and their tests all stay: what is wrong is under them, in how a
/// seek meets the decoder, and none of that is rediscovered by deleting the
/// commands that reach it.
///
/// Naming them here is the whole switch. Registration filters on it, help is
/// read back from what registration produced, and dispatch refuses anything
/// listed — so there is no second place to remember, and re-offering one is
/// this line and a release.
const DISABLED_COMMANDS: [&str; 4] = ["seek", "forward", "rewind", "restart"];

/// How `/help` groups commands, by name.
///
/// Only the grouping lives here; every description is read back from the
/// command as it will be registered. A command missing from this table still
/// appears in help under the last heading, so adding one can leave it in the
/// wrong place but never make it disappear.
const HELP_GROUPS: [(&str, &[&str]); 4] = [
    (
        "Playing",
        &[
            "play",
            "now-playing",
            "skip",
            "pause",
            "resume",
            "volume",
            "seek",
            "forward",
            "rewind",
            "restart",
        ],
    ),
    (
        "The queue",
        &[
            "queue", "clear", "remove", "shuffle", "repeat", "history", "export", "import",
        ],
    ),
    ("The voice channel", &["join", "leave", "stop"]),
    ("About Auxide", &["help"]),
];

/// Reads back a command's name and description as it will be registered.
///
/// Serenity keeps a builder's fields private, so the payload it produces is the
/// only place to read them from. Going through that rather than keeping a
/// second list of descriptions is what stops help from ever describing a
/// command that is not there, or wording one differently from Discord's own
/// command picker.
fn described(command: &CreateCommand) -> Option<(String, String)> {
    let payload = serde_json::to_value(command).ok()?;
    let name = payload.get("name")?.as_str()?.to_owned();
    let description = payload.get("description")?.as_str()?.to_owned();
    Some((name, description))
}

/// Renders every registered command, grouped, plus the rules worth knowing.
fn help_text() -> String {
    let commands = command_definitions();
    let described = commands.iter().filter_map(described).collect::<Vec<_>>();
    let mut listed = BTreeSet::new();
    let mut text = String::new();

    for (heading, names) in HELP_GROUPS {
        let mut group = String::new();
        for name in names {
            if let Some((name, description)) =
                described.iter().find(|(candidate, _)| candidate == name)
            {
                listed.insert(name.clone());
                let _ = writeln!(group, "`/{name}` — {description}");
            }
        }
        if !group.is_empty() {
            let _ = write!(text, "**{heading}**\n{group}\n");
        }
    }
    // A command nobody thought to group is still a command somebody can run.
    for (name, description) in described.iter().filter(|(name, _)| !listed.contains(name)) {
        let _ = writeln!(text, "`/{name}` — {description}");
    }

    text.push_str(
        "**Worth knowing**\n\
         Search results and refusals reach only you; what you queued, skipped, or stopped \
         reaches the channel.\n\
         An empty queue is not a departure — Auxide waits, and every track that plays resets \
         that wait.\n\
         Skipping somebody else's track needs half the channel to agree. Your own never does.",
    );
    text
}

/// Commands about what is playing now.
fn playing_commands() -> Vec<CreateCommand> {
    vec![
        CreateCommand::new("play")
            .description("Queue a YouTube URL or search")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "query",
                    "YouTube URL or search terms",
                )
                .required(true)
                .set_autocomplete(true)
                .min_length(1)
                .max_length(200),
            ),
        CreateCommand::new("queue")
            .description("Show the current queue")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::Integer,
                    "page",
                    "Which page of waiting tracks to show",
                )
                .required(false)
                .min_int_value(1),
            ),
        CreateCommand::new("clear").description("Drop everything waiting, and keep playing"),
        CreateCommand::new("remove")
            .description("Take one waiting track out of the queue")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::Integer,
                    "position",
                    "Its number in /queue",
                )
                .required(true)
                .min_int_value(1),
            ),
        CreateCommand::new("skip")
            .description("Skip the current track, or take waiting ones out")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "tracks",
                    "A waiting position or run of them, like 3 or 3-7",
                )
                .required(false)
                .min_length(1)
                .max_length(16),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::User,
                    "requester",
                    "Take out everything this person queued",
                )
                .required(false),
            ),
    ]
}

/// Commands about the queue behind it.
fn queue_commands() -> Vec<CreateCommand> {
    vec![
        CreateCommand::new("seek")
            .description("Move the playhead within the current track")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "to",
                    "Like 90, 2:30 or 1:02:03",
                )
                .required(true)
                .min_length(1)
                .max_length(16),
            ),
        CreateCommand::new("forward")
            .description("Jump forward within the current track")
            .add_option(
                CreateCommandOption::new(CommandOptionType::String, "by", "Like 30 or 2:30")
                    .required(true)
                    .min_length(1)
                    .max_length(16),
            ),
        CreateCommand::new("rewind")
            .description("Jump back within the current track")
            .add_option(
                CreateCommandOption::new(CommandOptionType::String, "by", "Like 30 or 2:30")
                    .required(true)
                    .min_length(1)
                    .max_length(16),
            ),
        CreateCommand::new("restart").description("Play the current track from the beginning"),
        CreateCommand::new("history")
            .description("Show what has already played, or queue one of them again")
            .add_option(
                CreateCommandOption::new(CommandOptionType::Integer, "page", "Which page to show")
                    .required(false)
                    .min_int_value(1),
            )
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::Integer,
                    "replay",
                    "Queue this one again, by its number",
                )
                .required(false)
                .min_int_value(1),
            ),
        CreateCommand::new("export").description("Save the queue to a file you can bring back"),
        CreateCommand::new("import")
            .description("Queue everything in a file /export made")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::Attachment,
                    "file",
                    "A file /export made",
                )
                .required(true),
            ),
        CreateCommand::new("join")
            .description("Come into your voice channel, and pick up any queue"),
        CreateCommand::new("leave").description("Leave the voice channel, and keep the queue"),
        CreateCommand::new("stop").description("Clear the queue and disconnect"),
        CreateCommand::new("shuffle")
            .description("Play the queue in random order, or reorder it once")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::String,
                    "mode",
                    "Omit to turn shuffle on or off",
                )
                .required(false)
                .add_string_choice("on", "on")
                .add_string_choice("off", "off")
                .add_string_choice("once", "once"),
            ),
        CreateCommand::new("repeat")
            .description("Choose what happens when a track ends")
            .add_option(
                CreateCommandOption::new(CommandOptionType::String, "mode", "What to repeat")
                    .required(true)
                    .add_string_choice("off", "off")
                    .add_string_choice("single", "single")
                    .add_string_choice("all", "all"),
            ),
        CreateCommand::new("now-playing").description("Show the current track"),
        CreateCommand::new("pause").description("Hold the current track where it is"),
        CreateCommand::new("resume").description("Continue a held track"),
        CreateCommand::new("volume")
            .description("Show or set how loud Auxide plays")
            .add_option(
                CreateCommandOption::new(
                    CommandOptionType::Integer,
                    "level",
                    "Percent of the source's own level; omit to show the current one",
                )
                .required(false)
                .min_int_value(1)
                .max_int_value(u64::from(MAX_VOLUME_PERCENT)),
            ),
    ]
}

struct DiscordHandler {
    runtime: Arc<BotRuntime>,
}

#[async_trait]
impl EventHandler for DiscordHandler {
    async fn ready(&self, _ctx: Context, ready: Ready) {
        self.runtime.observability.set_discord_connected(true);
        self.runtime.observability.set_ready(true);
        tracing::info!(bot_user = %ready.user.name, "Discord gateway is ready");
    }

    async fn shard_stage_update(&self, _ctx: Context, event: ShardStageUpdateEvent) {
        let connected = event.new == ConnectionStage::Connected;
        self.runtime.observability.set_discord_connected(connected);
        self.runtime.observability.set_ready(connected);
        tracing::info!(
            shard = event.shard_id.get(),
            old = ?event.old,
            new = ?event.new,
            "Discord shard stage changed"
        );
    }

    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        self.runtime.handle_interaction(&ctx, interaction).await;
    }

    async fn voice_state_update(&self, ctx: Context, old: Option<VoiceState>, new: VoiceState) {
        let Some(guild_id) = new
            .guild_id
            .or_else(|| old.as_ref().and_then(|old| old.guild_id))
        else {
            return;
        };
        self.runtime.handle_voice_state_update(&ctx, guild_id).await;
    }
}

struct BotRuntime {
    config: Arc<Config>,
    resolver: Arc<dyn SourceResolver>,
    suggestions: Suggestions,
    voice_factory: Arc<dyn VoiceGatewayFactory>,
    /// Guild players, created when a server first uses Auxide.
    ///
    /// These used to be built up front from the configured guild list. Without
    /// such a list there is nothing to enumerate at startup, and creating an
    /// actor, a worker, and a queue for every server the bot merely sits in
    /// would pay for servers that never issue a command.
    sessions: Mutex<HashMap<u64, GuildSession>>,
    pending_searches: Mutex<HashMap<Uuid, PendingSearch>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    observability: ObservabilityState,
    cancellation: CancellationToken,
}

#[derive(Clone)]
struct GuildSession {
    player: GuildPlayerHandle,
    /// Kept beside the player because moving the playhead is not queue state.
    ///
    /// Pausing and volume are properties of a session and so belong to the
    /// actor that owns it. A seek is a one-shot act on the track that is
    /// playing right now, and its answer — where it actually landed — has to
    /// come back to the person who asked.
    voice: Arc<dyn VoiceGateway>,
}

/// The part of a message that is the same whichever route it takes to Discord.
///
/// An interaction reply, a follow-up, and an unprompted post are three
/// different builders, and every one of them can carry this.
#[derive(Clone, Debug, Default)]
struct Announcement {
    content: String,
    embeds: Vec<CreateEmbed>,
}

impl Announcement {
    fn text(content: impl Into<String>) -> Self {
        Self {
            content: bounded_message(content.into()),
            embeds: Vec::new(),
        }
    }

    /// A card with no sentence above it.
    ///
    /// The embed already names the track, who queued it, and where it landed,
    /// so a line of prose repeating that is the same message twice.
    fn card(embed: CreateEmbed) -> Self {
        Self {
            content: String::new(),
            embeds: vec![embed],
        }
    }
}

/// Posts the things nobody asked for.
///
/// A track that failed and a channel everyone left are events, not answers:
/// there is no interaction to reply to, so these are ordinary messages and need
/// a channel to send them to and permission to send them there. Both can be
/// absent, and both being absent is a supported way to run Auxide — it is the
/// behaviour every version before this one had.
#[derive(Clone)]
struct Announcer {
    http: Arc<Http>,
    config: Arc<Config>,
}

impl Announcer {
    /// Reports whether this server wants each track named as it starts.
    fn announces_tracks(&self, guild_id: u64) -> bool {
        self.config
            .guild(guild_id)
            .is_some_and(|guild| guild.announce_tracks)
    }

    /// Describes how long an emptied queue keeps its voice channel.
    fn idle_hold(&self) -> String {
        idle_hold(&self.config)
    }

    /// Posts to a server's announcement channel, if it has one.
    ///
    /// `origin` is where the request that started the session came from, used
    /// when no channel is configured. Failure is logged and never propagated:
    /// every caller is in the middle of something that matters more than the
    /// message, and a server that has not granted **Send Messages** should keep
    /// playing music rather than fail.
    async fn announce(&self, guild_id: u64, origin: Option<u64>, announcement: Announcement) {
        let Some(channel_id) = announcement_channel(self.config.guild(guild_id), origin) else {
            tracing::debug!(guild_id, "no announcement channel for this server");
            return;
        };
        let message = CreateMessage::new()
            .content(announcement.content)
            .embeds(announcement.embeds)
            .allowed_mentions(no_mentions());
        if let Err(error) = ChannelId::new(channel_id)
            .send_message(&self.http, message)
            .await
        {
            tracing::warn!(
                %error,
                guild_id,
                channel_id,
                "failed to announce; check that Auxide may send messages there"
            );
        }
    }
}

#[derive(Clone, Debug)]
struct PendingSearch {
    created_at: Instant,
    guild_id: u64,
    response_channel_id: u64,
    voice_channel_id: u64,
    user_id: u64,
    tracks: Vec<TrackMetadata>,
}

/// Who a command's answer is for.
///
/// Auxide is driven by a room, not by one person: what somebody queued or
/// skipped is the room's business, while their half-finished search and the
/// reason a command was refused are not. This is fixed before the interaction is
/// acknowledged because Discord decides an interaction's audience when it is
/// first answered and will not revisit it afterwards.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Audience {
    /// Visible to the whole text channel.
    Channel,
    /// Visible only to the person who ran the command.
    Requester,
}

#[derive(Debug)]
struct InteractionReply {
    content: String,
    embeds: Vec<CreateEmbed>,
    attachments: Vec<CreateAttachment>,
    components: Vec<CreateActionRow>,
    /// A second, channel-visible message to post alongside a private answer.
    ///
    /// A search picker has to stay private while the choice made with it does
    /// not, and one interaction cannot answer twice with different audiences.
    announcement: Option<Announcement>,
}

impl InteractionReply {
    fn message(content: impl Into<String>) -> Self {
        Self {
            content: bounded_message(content.into()),
            embeds: Vec::new(),
            attachments: Vec::new(),
            components: Vec::new(),
            announcement: None,
        }
    }

    fn attaching(mut self, attachment: CreateAttachment) -> Self {
        self.attachments.push(attachment);
        self
    }

    fn from(announcement: Announcement) -> Self {
        Self {
            content: announcement.content,
            embeds: announcement.embeds,
            attachments: Vec::new(),
            components: Vec::new(),
            announcement: None,
        }
    }

    fn announcing(mut self, announcement: Announcement) -> Self {
        self.announcement = Some(announcement);
        self
    }
}

impl BotRuntime {
    fn new(
        config: Arc<Config>,
        resolver: Arc<dyn SourceResolver>,
        voice_factory: Arc<dyn VoiceGatewayFactory>,
        observability: ObservabilityState,
        cancellation: CancellationToken,
    ) -> Self {
        observability.set_guild_players(0);
        Self {
            config,
            suggestions: Suggestions::new(Arc::clone(&resolver)),
            resolver,
            voice_factory,
            sessions: Mutex::new(HashMap::new()),
            pending_searches: Mutex::new(HashMap::new()),
            tasks: Mutex::new(Vec::new()),
            observability,
            cancellation,
        }
    }

    /// Returns this server's player, creating it on first use.
    ///
    /// Refusing past [`DiscordConfig::max_guilds`] keeps an unbounded number of
    /// servers from becoming an unbounded number of actors and queues. It is a
    /// per-request refusal, so servers already being served keep working.
    ///
    /// [`DiscordConfig::max_guilds`]: crate::config::DiscordConfig::max_guilds
    async fn session(&self, guild_id: u64, http: &Arc<Http>) -> Result<GuildSession> {
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.get(&guild_id) {
            return Ok(session.clone());
        }
        if sessions.len() >= self.config.discord.max_guilds {
            bail!("Auxide is already serving as many servers as it is configured to hold");
        }

        let (player, transitions, actor_task) = spawn_guild_player(
            guild_id,
            self.config.playback.max_queue_length,
            self.config.playback.actor_mailbox_capacity,
            random(),
            Duration::from_secs(self.config.playback.idle_timeout_seconds),
            self.config.playback.starting_volume_percent(),
        );
        let voice = self.voice_factory.create(guild_id);
        let worker_task = tokio::spawn(playback_worker(
            guild_id,
            player.clone(),
            transitions,
            Arc::clone(&voice),
            self.announcer(http),
            self.observability.clone(),
            self.cancellation.child_token(),
        ));
        {
            let mut tasks = self.tasks.lock().await;
            tasks.push(actor_task);
            tasks.push(worker_task);
        }

        let session = GuildSession { player, voice };
        sessions.insert(guild_id, session.clone());
        self.observability
            .set_guild_players(sessions.len().try_into().unwrap_or(u64::MAX));
        tracing::info!(guild_id, servers = sessions.len(), "started a guild player");
        Ok(session)
    }

    /// Offers what has already been searched for, and never waits.
    ///
    /// Deliberately unauthorized: a suggestion reveals only what `YouTube`
    /// would return for words the person typed themselves, changes nothing, and
    /// costs nothing beyond a cache lookup. Refusing here would mean answering
    /// a keystroke with a permission check.
    async fn suggest(&self, ctx: &Context, autocomplete: &CommandInteraction) {
        let choices = match autocomplete
            .data
            .autocomplete()
            .filter(|option| option.name == "query")
        {
            Some(option) => self.suggestions.for_query(option.value).await,
            None => Vec::new(),
        };
        let mut response = CreateAutocompleteResponse::new();
        for (name, value) in choices {
            response = response.add_string_choice(name, value);
        }
        let answered = autocomplete
            .create_response(&ctx.http, CreateInteractionResponse::Autocomplete(response))
            .await;
        self.observability.record_interaction(answered.is_ok());
        if let Err(error) = answered {
            tracing::warn!(%error, "failed to answer an autocomplete interaction");
        }
    }

    fn announcer(&self, http: &Arc<Http>) -> Announcer {
        Announcer {
            http: Arc::clone(http),
            config: Arc::clone(&self.config),
        }
    }

    /// Returns a server's player only if it already has one.
    ///
    /// Voice traffic arrives for every server Auxide sits in, including ones
    /// that have never issued a command, so this deliberately cannot create a
    /// player the way [`BotRuntime::session`] does.
    async fn existing_session(&self, guild_id: u64) -> Option<GuildSession> {
        self.sessions.lock().await.get(&guild_id).cloned()
    }

    /// Ends the session once the last person leaves the channel Auxide is in.
    ///
    /// Playing to an empty channel holds a voice connection and burns bandwidth
    /// for an audience of nobody, and the idle timer never fires while a track
    /// is still running, so nothing else would end it.
    async fn handle_voice_state_update(&self, ctx: &Context, guild_id: GuildId) {
        let Some(session) = self.existing_session(guild_id.get()).await else {
            return;
        };
        let Some(seen) = occupied_voice_channel(ctx, guild_id) else {
            return;
        };

        if seen.listeners > 0 {
            // Somebody walked back in. This lifts only a hold the room emptying
            // put there, so a pause anybody asked for survives being alone.
            match session.player.release(Release::OnlyAbandoned).await {
                Ok(transition) if transition.directive == PlaybackDirective::Resume => {
                    tracing::info!(guild_id = guild_id.get(), "somebody came back");
                }
                _ => {}
            }
            return;
        }

        // Nobody is listening. Holding rather than stopping keeps a queue
        // somebody spent an evening on through a trip to the kitchen, and the
        // countdown it starts is what ends a room nobody comes back to.
        let origin = session
            .player
            .snapshot()
            .await
            .ok()
            .and_then(|snapshot| snapshot.text_channel_id);
        if session.player.hold(Hold::Abandoned).await.is_err() {
            // Nothing was playing to hold, so the countdown an emptied queue
            // already started is what ends this.
            return;
        }
        tracing::info!(
            guild_id = guild_id.get(),
            channel_id = seen.channel_id,
            "paused an empty voice channel"
        );
        self.announcer(&ctx.http)
            .announce(
                guild_id.get(),
                origin,
                Announcement::text(format!(
                    "Everyone left, so I have paused. I will carry on if somebody comes back, and leave in {} if nobody does.",
                    idle_hold(&self.config)
                )),
            )
            .await;
    }

    async fn handle_interaction(&self, ctx: &Context, interaction: Interaction) {
        match interaction {
            Interaction::Command(command) => self.handle_command(ctx, &command).await,
            Interaction::Component(component) => self.handle_component(ctx, &component).await,
            Interaction::Autocomplete(autocomplete) => {
                self.suggest(ctx, &autocomplete).await;
            }
            Interaction::Ping(ping) => {
                let result = ctx
                    .http
                    .create_interaction_response(
                        ping.id,
                        &ping.token,
                        &CreateInteractionResponse::Pong,
                        Vec::new(),
                    )
                    .await;
                self.observability.record_interaction(result.is_ok());
                if let Err(error) = result {
                    tracing::warn!(%error, "failed to acknowledge ping interaction");
                }
            }
            Interaction::Modal(modal) => {
                let result = modal
                    .create_response(
                        &ctx.http,
                        CreateInteractionResponse::Message(response_message(
                            "Auxide does not use modal interactions.",
                        )),
                    )
                    .await;
                self.observability.record_interaction(result.is_ok());
                if let Err(error) = result {
                    tracing::warn!(%error, "failed to acknowledge modal interaction");
                }
            }
            _ => {}
        }
    }

    async fn handle_command(&self, ctx: &Context, command: &CommandInteraction) {
        let audience = command_audience(command);
        let deferred = match audience {
            Audience::Channel => command.defer(&ctx.http).await,
            Audience::Requester => command.defer_ephemeral(&ctx.http).await,
        };
        if let Err(error) = deferred {
            self.observability.record_interaction(false);
            tracing::warn!(%error, command = %command.data.name, "failed to defer interaction");
            return;
        }

        let reply = match self.dispatch_command(ctx, command).await {
            Ok(reply) => reply,
            Err(error) => {
                tracing::warn!(%error, command = %command.data.name, "command failed");
                self.refuse_command(ctx, command, audience, &error).await;
                return;
            }
        };
        let mut edit = EditInteractionResponse::new()
            .content(reply.content)
            .embeds(reply.embeds)
            .components(reply.components)
            .allowed_mentions(no_mentions());
        for attachment in reply.attachments {
            edit = edit.new_attachment(attachment);
        }
        let response = command.edit_response(&ctx.http, edit).await;
        self.observability.record_interaction(response.is_ok());
        if let Err(error) = response {
            tracing::warn!(%error, command = %command.data.name, "failed to edit interaction response");
        }
    }

    /// Answers a failed command privately, whoever its success would have been for.
    ///
    /// Refusals name missing authorization and absent voice channels, which is
    /// the requester's business alone. A command whose answer was going to be
    /// public has already been acknowledged in the channel, so the placeholder
    /// Discord is showing there has to be withdrawn before answering again.
    async fn refuse_command(
        &self,
        ctx: &Context,
        command: &CommandInteraction,
        audience: Audience,
        error: &anyhow::Error,
    ) {
        let content = format!("Unable to complete that command: {error}");
        let response = if audience == Audience::Channel {
            if let Err(error) = command.delete_response(&ctx.http).await {
                tracing::warn!(%error, command = %command.data.name, "failed to withdraw the public placeholder");
            }
            command
                .create_followup(&ctx.http, followup_message(content).ephemeral(true))
                .await
                .map(|_| ())
        } else {
            command
                .edit_response(
                    &ctx.http,
                    EditInteractionResponse::new()
                        .content(bounded_message(content))
                        .allowed_mentions(no_mentions()),
                )
                .await
                .map(|_| ())
        };
        self.observability.record_interaction(false);
        if let Err(error) = response {
            tracing::warn!(%error, command = %command.data.name, "failed to report a refused command");
        }
    }

    async fn dispatch_command(
        &self,
        ctx: &Context,
        command: &CommandInteraction,
    ) -> Result<InteractionReply> {
        // Discord keeps offering a command until the guild's copy is replaced,
        // so a withdrawn one stays clickable for a while after it stops being
        // registered. Refusing here is what makes the withdrawal true straight
        // away, and says so rather than failing somewhere further in.
        if DISABLED_COMMANDS.contains(&command.data.name.as_str()) {
            bail!(
                "`/{}` is turned off in this version of Auxide. It could stop playback \
                 altogether, so it has been withdrawn until that is fixed.",
                command.data.name
            );
        }
        let authorization = self.authorize_command(ctx, command).await?;
        match command.data.name.as_str() {
            "play" => self.play(ctx, command, authorization).await,
            "queue" => self.queue(command, authorization).await,
            "clear" => self.clear(ctx, authorization).await,
            "seek" | "forward" | "rewind" | "restart" => {
                self.seek(ctx, command, authorization).await
            }
            "history" => self.history(command, authorization).await,
            "export" => self.export(authorization).await,
            "import" => self.import(ctx, command, authorization).await,
            "remove" => self.remove(ctx, command, authorization).await,
            "skip" => self.skip(ctx, command, authorization).await,
            "help" => Ok(Self::help()),
            "join" => self.join(authorization).await,
            "leave" => self.leave(ctx, authorization).await,
            "stop" => self.stop(ctx, authorization).await,
            "shuffle" => self.shuffle(ctx, command, authorization).await,
            "repeat" => self.repeat(ctx, command, authorization).await,
            "now-playing" => self.now_playing(authorization).await,
            "pause" => self.set_paused(ctx, authorization, true).await,
            "resume" => self.set_paused(ctx, authorization, false).await,
            "volume" => self.volume(ctx, command, authorization).await,
            other => bail!("unsupported command {other:?}"),
        }
    }

    async fn authorize_command(
        &self,
        ctx: &Context,
        command: &CommandInteraction,
    ) -> Result<Authorization> {
        let guild_id = command
            .guild_id
            .context("Auxide commands are available only in configured servers")?;
        let member = command
            .member
            .as_deref()
            .context("Discord did not supply server membership for this request")?;
        self.authorize(
            ctx,
            guild_id.get(),
            command.channel_id.get(),
            command.user.id,
            &member
                .roles
                .iter()
                .map(|role| role.get())
                .collect::<Vec<_>>(),
        )
        .await
    }

    async fn authorize_component(
        &self,
        ctx: &Context,
        component: &ComponentInteraction,
    ) -> Result<Authorization> {
        let guild_id = component
            .guild_id
            .context("Auxide controls are available only in configured servers")?;
        let member = component
            .member
            .as_ref()
            .context("Discord did not supply server membership for this request")?;
        self.authorize(
            ctx,
            guild_id.get(),
            component.channel_id.get(),
            component.user.id,
            &member
                .roles
                .iter()
                .map(|role| role.get())
                .collect::<Vec<_>>(),
        )
        .await
    }

    async fn authorize(
        &self,
        ctx: &Context,
        guild_id: u64,
        response_channel_id: u64,
        user_id: UserId,
        role_ids: &[u64],
    ) -> Result<Authorization> {
        if !self.config.allows_guild(guild_id) {
            bail!("server {guild_id} is not allowlisted");
        }
        // A server with no entry of its own is unrestricted within itself, the
        // same treatment an entry with empty lists already receives. Narrowing
        // one server therefore stays possible without enumerating the others.
        if let Some(guild) = self.config.guild(guild_id) {
            // Both of these are the host's boundary rather than the server's,
            // and saying so is the difference between an administrator fixing
            // it and hunting for a Discord setting that was never involved.
            if !guild.command_channel_ids.is_empty()
                && !guild.command_channel_ids.contains(&response_channel_id)
            {
                bail!(
                    "Auxide's own configuration does not enable commands in this channel. That \
                     is set on the host that runs the bot, not in Discord."
                );
            }
            if !identity_is_authorized(guild, user_id.get(), role_ids) {
                bail!(
                    "Auxide's own configuration does not list you as permitted to control it. \
                     That is set on the host that runs the bot, not in Discord."
                );
            }
        }
        let session = self.session(guild_id, &ctx.http).await?;
        let voice_channel_id = requester_voice_channel(ctx, guild_id, user_id);
        Ok(Authorization {
            guild_id,
            response_channel_id,
            user_id: user_id.get(),
            voice_channel_id,
            session,
        })
    }

    async fn play(
        &self,
        _ctx: &Context,
        command: &CommandInteraction,
        authorization: Authorization,
    ) -> Result<InteractionReply> {
        let voice_channel_id = authorization
            .voice_channel_id
            .context("join a voice channel before using /play")?;
        let query = query_option(command).context("the required query option was missing")?;

        if let Ok(url) = Url::parse(query) {
            let result = self.resolver.playlist(&url).await;
            self.observability.record_source_resolution(result.is_ok());
            if let Some(playlist) = result? {
                return self
                    .enqueue_playlist(&authorization, voice_channel_id, &url, playlist)
                    .await
                    .map(InteractionReply::from);
            }

            let result = self.resolver.inspect(&url).await;
            self.observability.record_source_resolution(result.is_ok());
            let track = result?;
            return self
                .enqueue_track(&authorization, voice_channel_id, track)
                .await
                .map(InteractionReply::from);
        }

        let result = self.resolver.search(query).await;
        self.observability.record_source_resolution(result.is_ok());
        let tracks = result?;
        if tracks.is_empty() {
            bail!("YouTube returned no search results");
        }
        let search_id = Uuid::new_v4();
        let tracks = tracks.into_iter().take(5).collect::<Vec<_>>();
        let mut searches = self.pending_searches.lock().await;
        prune_searches(&mut searches);
        if searches.len() >= MAX_PENDING_SEARCHES {
            if let Some(oldest) = searches
                .iter()
                .min_by_key(|(_, pending)| pending.created_at)
                .map(|(id, _)| *id)
            {
                searches.remove(&oldest);
            }
        }
        searches.insert(
            search_id,
            PendingSearch {
                created_at: Instant::now(),
                guild_id: authorization.guild_id,
                response_channel_id: authorization.response_channel_id,
                voice_channel_id,
                user_id: authorization.user_id,
                tracks: tracks.clone(),
            },
        );
        drop(searches);

        let mut content = String::from("Choose a search result:\n");
        let mut buttons = Vec::with_capacity(tracks.len());
        for (index, track) in tracks.iter().enumerate() {
            let _ = writeln!(
                content,
                "{}. {} ({})",
                index + 1,
                single_line(&track.title, 120),
                format_duration(track.duration)
            );
            buttons.push(
                CreateButton::new(format!("auxide:select:{search_id}:{index}"))
                    .label((index + 1).to_string()),
            );
        }
        Ok(InteractionReply {
            content: bounded_message(content),
            embeds: Vec::new(),
            attachments: Vec::new(),
            components: vec![CreateActionRow::Buttons(buttons)],
            announcement: None,
        })
    }

    /// Adds a track and describes the result for the whole channel.
    ///
    /// Both callers announce the same thing, because a URL played outright and a
    /// search result chosen from a picker are the same event to everyone else in
    /// the room.
    async fn enqueue_track(
        &self,
        authorization: &Authorization,
        voice_channel_id: u64,
        track: TrackMetadata,
    ) -> Result<Announcement> {
        let item = QueueItem::new(
            track.clone(),
            authorization.user_id,
            authorization.response_channel_id,
        );
        let queue_id = item.queue_id;
        let transition = authorization
            .session
            .player
            .enqueue(item, voice_channel_id)
            .await?;
        if matches!(transition.directive, PlaybackDirective::Play(_)) {
            return Ok(Announcement::card(track_embed(
                "Now playing",
                &track,
                authorization.user_id,
                None,
            )));
        }
        let position = transition
            .snapshot
            .pending
            .iter()
            .position(|queued| queued.queue_id == queue_id)
            .map_or(1, |index| index + 2);
        Ok(Announcement::card(track_embed(
            "Added to the queue",
            &track,
            authorization.user_id,
            Some(position),
        )))
    }

    /// Moves the playhead within whatever is playing.
    async fn seek(
        &self,
        ctx: &Context,
        command: &CommandInteraction,
        authorization: Authorization,
    ) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        let to = match command.data.name.as_str() {
            "restart" => SeekTo::Start,
            "seek" => SeekTo::Position(parse_timestamp(
                string_option(command, "to").context("a time is required")?,
            )?),
            "forward" => SeekTo::Forward(parse_timestamp(
                string_option(command, "by").context("a duration is required")?,
            )?),
            other => {
                debug_assert_eq!(other, "rewind");
                SeekTo::Backward(parse_timestamp(
                    string_option(command, "by").context("a duration is required")?,
                )?)
            }
        };

        // Songbird reports where it settled rather than where it was sent: a
        // container seeks to a boundary it can resume decoding from, which is
        // rarely the exact instant that was asked for.
        let landed = authorization.session.voice.seek(to).await?;
        Ok(InteractionReply::message(format!(
            "{} moved the track to {}.",
            mention(authorization.user_id),
            format_duration(landed)
        )))
    }

    /// Lists what has already played, or queues one of them again.
    async fn history(
        &self,
        command: &CommandInteraction,
        authorization: Authorization,
    ) -> Result<InteractionReply> {
        let history = authorization.session.player.history().await?;
        if history.is_empty() {
            bail!("nothing has played yet");
        }

        if let Some(position) =
            integer_option(command, "replay").and_then(|position| usize::try_from(position).ok())
        {
            let voice_channel_id = authorization
                .voice_channel_id
                .context("join a voice channel before queueing something")?;
            let item = history
                .get(position - 1)
                .with_context(|| format!("nothing played at position {position}"))?;
            // A history entry is a whole track, so this needs no resolution —
            // the media URL is fetched just before it plays, as it always is.
            return self
                .enqueue_track(&authorization, voice_channel_id, item.track.clone())
                .await
                .map(InteractionReply::from);
        }

        let page = integer_option(command, "page")
            .and_then(|page| usize::try_from(page).ok())
            .unwrap_or(1);
        let pages = history.len().div_ceil(QUEUE_PAGE_SIZE).max(1);
        let page = page.clamp(1, pages);
        let mut description = String::new();
        for (offset, item) in history
            .iter()
            .skip((page - 1) * QUEUE_PAGE_SIZE)
            .take(QUEUE_PAGE_SIZE)
            .enumerate()
        {
            let _ = writeln!(
                description,
                "{}. {} ({}) — {}",
                (page - 1) * QUEUE_PAGE_SIZE + offset + 1,
                single_line(&item.track.title, 100),
                format_duration(item.track.duration),
                mention(item.requested_by)
            );
        }
        let mut embed = CreateEmbed::new()
            .author(CreateEmbedAuthor::new("Already played"))
            .description(bounded_message(description))
            .colour(EMBED_COLOUR)
            .footer(CreateEmbedFooter::new(if pages > 1 {
                format!("Page {page} of {pages} · /history replay:N to queue one again")
            } else {
                "/history replay:N to queue one again".to_owned()
            }));
        if pages == 1 {
            embed = embed.footer(CreateEmbedFooter::new(
                "/history replay:N to queue one again",
            ));
        }
        Ok(InteractionReply::from(Announcement::card(embed)))
    }

    /// Writes the queue to a file that `/import` can bring back.
    ///
    /// The whole track travels, not just its link, so bringing a queue back
    /// costs no resolutions at all — which is what makes a hundred-track export
    /// usable. A media URL was never in here to begin with; those are fetched
    /// just before a track plays.
    async fn export(&self, authorization: Authorization) -> Result<InteractionReply> {
        let snapshot = authorization.session.player.snapshot().await?;
        let tracks = snapshot
            .current
            .into_iter()
            .chain(snapshot.pending)
            .map(|item| item.track)
            .collect::<Vec<_>>();
        if tracks.is_empty() {
            bail!("there is nothing in the queue to save");
        }
        let saved = serde_json::to_vec_pretty(&Export { tracks })
            .context("failed to write the queue out")?;
        Ok(
            InteractionReply::message("Here is the queue. Bring it back with `/import`.")
                .attaching(CreateAttachment::bytes(saved, "auxide-queue.json")),
        )
    }

    /// Queues everything in a file `/export` made.
    async fn import(
        &self,
        _ctx: &Context,
        command: &CommandInteraction,
        authorization: Authorization,
    ) -> Result<InteractionReply> {
        let voice_channel_id = authorization
            .voice_channel_id
            .context("join a voice channel before using /import")?;
        let attachment =
            attachment_option(command, "file").context("attach a file that /export made")?;
        if attachment.size > MAX_IMPORT_BYTES {
            bail!(
                "that file is larger than the {} KiB an exported queue can be",
                MAX_IMPORT_BYTES / 1024
            );
        }

        let body = attachment
            .download()
            .await
            .context("failed to read the attached file")?;
        let export: Export =
            serde_json::from_slice(&body).context("that file is not one /export made")?;

        // Everything past this point came from a file somebody uploaded. Each
        // link is checked against what the source will accept before any of
        // them can become a subprocess argument later.
        let mut tracks = Vec::with_capacity(export.tracks.len());
        for track in export
            .tracks
            .into_iter()
            .take(self.config.playback.max_queue_length)
        {
            self.resolver.accepts(&track.canonical_url)?;
            tracks.push(QueueItem::new(
                track,
                authorization.user_id,
                authorization.response_channel_id,
            ));
        }
        if tracks.is_empty() {
            bail!("that file has no tracks in it");
        }

        let bulk = authorization
            .session
            .player
            .enqueue_all(tracks, voice_channel_id)
            .await?;
        Ok(InteractionReply::message(format!(
            "{} brought back {} track(s).",
            mention(authorization.user_id),
            bulk.accepted
        )))
    }

    /// Adds a whole playlist and describes it as the one thing it was.
    async fn enqueue_playlist(
        &self,
        authorization: &Authorization,
        voice_channel_id: u64,
        url: &Url,
        playlist: Playlist,
    ) -> Result<Announcement> {
        if playlist.tracks.is_empty() {
            bail!("that playlist has nothing Auxide can play");
        }
        let played = playlist.tracks.len();
        let listed = playlist.total;
        let duration: Duration = playlist.tracks.iter().map(|track| track.duration).sum();
        let items = playlist
            .tracks
            .into_iter()
            .map(|track| {
                QueueItem::new(
                    track,
                    authorization.user_id,
                    authorization.response_channel_id,
                )
            })
            .collect::<Vec<_>>();
        let bulk = authorization
            .session
            .player
            .enqueue_all(items, voice_channel_id)
            .await?;

        let title = playlist
            .title
            .as_deref()
            .map_or_else(|| "a playlist".to_owned(), |title| single_line(title, 150));
        let mut embed = CreateEmbed::new()
            .author(CreateEmbedAuthor::new("Added to the queue"))
            .title(single_line(&title, EMBED_TITLE_CHARS))
            .url(url.as_str())
            .colour(EMBED_COLOUR)
            .field("Tracks", bulk.accepted.to_string(), true)
            .field("Duration", format_duration(duration), true)
            .field("Requested by", mention(authorization.user_id), true);
        // Say what was left behind rather than quietly dropping it. Entries can
        // go missing twice over: unplayable ones never became tracks, and the
        // queue may not have had room for the ones that did.
        let dropped = listed.saturating_sub(played) + bulk.refused;
        if dropped > 0 {
            embed = embed.footer(CreateEmbedFooter::new(format!(
                "{dropped} of {listed} left out — unplayable, or past the {}-track queue limit",
                self.config.playback.max_queue_length
            )));
        }
        Ok(Announcement::card(embed))
    }

    async fn queue(
        &self,
        command: &CommandInteraction,
        authorization: Authorization,
    ) -> Result<InteractionReply> {
        let snapshot = authorization.session.player.snapshot().await?;
        let page = integer_option(command, "page")
            .and_then(|page| usize::try_from(page).ok())
            .unwrap_or(1);
        let Some(rendered) = format_queue(&snapshot, page) else {
            return Ok(InteractionReply::message("The queue is empty."));
        };
        let mut embed = CreateEmbed::new()
            .author(CreateEmbedAuthor::new("Queue"))
            .description(rendered.description)
            .colour(EMBED_COLOUR);
        // A queue that is repeating or picking at random does not play in the
        // order it is printed in, and a page says nothing about how many there
        // are, so both belong beside the list rather than inside it.
        let mut footer = Vec::new();
        if rendered.pages > 1 {
            footer.push(format!("Page {} of {}", rendered.page, rendered.pages));
        }
        if snapshot.repeat != RepeatMode::Off {
            footer.push(format!("Repeat: {}", snapshot.repeat.as_str()));
        }
        if snapshot.shuffle {
            footer.push("Shuffle: on".to_owned());
        }
        if !footer.is_empty() {
            embed = embed.footer(CreateEmbedFooter::new(footer.join(" · ")));
        }
        Ok(InteractionReply::from(Announcement::card(embed)))
    }

    async fn clear(&self, ctx: &Context, authorization: Authorization) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        let outcome = authorization.session.player.clear().await?;
        if outcome.removed == 0 {
            bail!("nothing is waiting in the queue");
        }
        Ok(InteractionReply::message(format!(
            "{} cleared {} waiting track(s). What is playing carries on.",
            mention(authorization.user_id),
            outcome.removed
        )))
    }

    async fn remove(
        &self,
        ctx: &Context,
        command: &CommandInteraction,
        authorization: Authorization,
    ) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        let position = integer_option(command, "position")
            .and_then(|position| usize::try_from(position).ok())
            .context("a position is required")?;
        let outcome = authorization.session.player.remove(position).await?;
        Ok(InteractionReply::message(format!(
            "{} removed **{}** from the queue.",
            mention(authorization.user_id),
            single_line(&outcome.removed.track.title, 150)
        )))
    }

    async fn now_playing(&self, authorization: Authorization) -> Result<InteractionReply> {
        let snapshot = authorization.session.player.snapshot().await?;
        let heading = playback_heading(snapshot.paused());
        let Some(item) = snapshot.current else {
            return Ok(InteractionReply::message("Nothing is playing."));
        };
        Ok(InteractionReply::from(Announcement::card(track_embed(
            heading,
            &item.track,
            item.requested_by,
            None,
        ))))
    }

    async fn skip(
        &self,
        ctx: &Context,
        command: &CommandInteraction,
        authorization: Authorization,
    ) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        let requester = mention(authorization.user_id);

        // Taking waiting tracks out is not the same act as cutting short what
        // the room is listening to, and only the second one needs anybody's
        // agreement.
        if let Some(who) = user_option(command, "requester") {
            let removed = authorization.session.player.remove_requester(who).await?;
            if removed.removed.is_empty() {
                bail!("{} has nothing waiting in the queue", mention(who));
            }
            return Ok(InteractionReply::message(format!(
                "{requester} took out {} track(s) queued by {}.",
                removed.removed.len(),
                mention(who)
            )));
        }
        if let Some(spec) = string_option(command, "tracks") {
            let (from, to) = parse_track_range(spec)?;
            let removed = authorization.session.player.remove_range(from, to).await?;
            let names = removed
                .removed
                .first()
                .map(|item| single_line(&item.track.title, 100))
                .unwrap_or_default();
            return Ok(InteractionReply::message(if removed.removed.len() == 1 {
                format!("{requester} took **{names}** out of the queue.")
            } else {
                format!(
                    "{requester} took {} tracks out of the queue, starting with **{names}**.",
                    removed.removed.len()
                )
            }));
        }

        let listeners = listener_count(ctx, GuildId::new(authorization.guild_id));
        let needed = votes_needed(listeners);
        match authorization
            .session
            .player
            .vote_skip(authorization.user_id, needed)
            .await?
        {
            SkipVerdict::Pending { have, needed } => Ok(InteractionReply::message(format!(
                "{requester} wants to skip this one. {have} of {needed} so far — \
                 run `/skip` to agree."
            ))),
            SkipVerdict::Skipped(outcome) => {
                let skipped = outcome.skipped.context("nothing is playing to skip")?;
                let title = single_line(&skipped.track.title, 150);
                let next = outcome.transition.snapshot.current.as_ref().map_or_else(
                    || {
                        format!(
                            "Nothing else is queued; leaving in {}.",
                            idle_hold(&self.config)
                        )
                    },
                    |item| format!("Now playing **{}**.", single_line(&item.track.title, 150)),
                );
                Ok(InteractionReply::message(format!(
                    "{requester} skipped **{title}**. {next}"
                )))
            }
        }
    }

    async fn set_paused(
        &self,
        ctx: &Context,
        authorization: Authorization,
        paused: bool,
    ) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        let transition = if paused {
            authorization.session.player.hold(Hold::Requested).await?
        } else {
            authorization
                .session
                .player
                .release(Release::Anything)
                .await?
        };
        let title = transition.snapshot.current.as_ref().map_or_else(
            || "the current track".to_owned(),
            |item| format!("**{}**", single_line(&item.track.title, 150)),
        );
        let requester = mention(authorization.user_id);
        Ok(InteractionReply::message(if paused {
            format!(
                "{requester} paused {title}. Leaving in {} unless it resumes.",
                idle_hold(&self.config)
            )
        } else {
            format!("{requester} resumed {title}.")
        }))
    }

    async fn volume(
        &self,
        ctx: &Context,
        command: &CommandInteraction,
        authorization: Authorization,
    ) -> Result<InteractionReply> {
        let Some(level) = volume_option(command) else {
            let snapshot = authorization.session.player.snapshot().await?;
            return Ok(InteractionReply::message(format!(
                "Auxide is playing at {}% of the source's own level.",
                snapshot.volume_percent
            )));
        };
        self.require_same_voice(ctx, &authorization).await?;
        let transition = authorization.session.player.set_volume(level).await?;
        Ok(InteractionReply::message(format!(
            "{} set the volume to {}%.",
            mention(authorization.user_id),
            transition.snapshot.volume_percent
        )))
    }

    /// Describes every command, from the same list Discord is given.
    fn help() -> InteractionReply {
        InteractionReply::from(Announcement::card(
            CreateEmbed::new()
                .author(CreateEmbedAuthor::new("Auxide"))
                .description(bounded_embed(help_text()))
                .colour(EMBED_COLOUR),
        ))
    }

    /// Comes into the requester's channel, picking a parked queue back up.
    async fn join(&self, authorization: Authorization) -> Result<InteractionReply> {
        let voice_channel_id = authorization
            .voice_channel_id
            .context("join a voice channel first, so Auxide knows where to go")?;
        let transition = authorization.session.player.join(voice_channel_id).await?;
        let requester = mention(authorization.user_id);
        Ok(InteractionReply::message(
            transition.snapshot.current.as_ref().map_or_else(
                || format!("{requester} brought Auxide in. Queue something with `/play`."),
                |item| {
                    format!(
                        "{requester} brought Auxide back. Starting **{}** again.",
                        single_line(&item.track.title, 150)
                    )
                },
            ),
        ))
    }

    /// Gives up the channel, and keeps the queue for coming back to.
    async fn leave(&self, ctx: &Context, authorization: Authorization) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        let transition = authorization.session.player.park().await?;
        let waiting = transition.snapshot.len();
        let requester = mention(authorization.user_id);
        Ok(InteractionReply::message(if waiting == 0 {
            format!("{requester} sent Auxide away.")
        } else {
            format!(
                "{requester} sent Auxide away. {waiting} track(s) are kept for {}; bring it back with `/join`.",
                idle_hold(&self.config)
            )
        }))
    }

    async fn stop(&self, ctx: &Context, authorization: Authorization) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        authorization.session.player.stop().await?;
        Ok(InteractionReply::message(format!(
            "{} stopped playback, cleared the queue, and disconnected.",
            mention(authorization.user_id)
        )))
    }

    async fn repeat(
        &self,
        ctx: &Context,
        command: &CommandInteraction,
        authorization: Authorization,
    ) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        let repeat = match mode_option(command) {
            Some("single") => RepeatMode::Single,
            Some("all") => RepeatMode::All,
            Some("off") => RepeatMode::Off,
            _ => bail!("choose one of off, single, or all"),
        };
        authorization.session.player.set_repeat(repeat).await?;
        let requester = mention(authorization.user_id);
        Ok(InteractionReply::message(match repeat {
            RepeatMode::Off => format!("{requester} turned repeat off."),
            RepeatMode::Single => format!("{requester} set the current track to repeat."),
            RepeatMode::All => format!("{requester} set the queue to repeat."),
        }))
    }

    async fn shuffle(
        &self,
        ctx: &Context,
        command: &CommandInteraction,
        authorization: Authorization,
    ) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        let change = match mode_option(command) {
            Some("on") => ShuffleChange::Enable,
            Some("off") => ShuffleChange::Disable,
            Some("once") => ShuffleChange::Reorder,
            None => ShuffleChange::Toggle,
            Some(_) => bail!("choose one of on, off, or once"),
        };
        let transition = authorization.session.player.shuffle(change).await?;
        let requester = mention(authorization.user_id);
        if change == ShuffleChange::Reorder {
            return Ok(InteractionReply::message(format!(
                "{requester} reordered {} waiting track(s).",
                transition.snapshot.pending.len()
            )));
        }
        Ok(InteractionReply::message(format!(
            "{requester} turned shuffle {}.",
            if transition.snapshot.shuffle {
                "on"
            } else {
                "off"
            }
        )))
    }

    async fn require_same_voice(
        &self,
        _ctx: &Context,
        authorization: &Authorization,
    ) -> Result<()> {
        let requester = authorization
            .voice_channel_id
            .context("join Auxide's voice channel before using that command")?;
        let snapshot = authorization.session.player.snapshot().await?;
        let current = snapshot
            .voice_channel_id
            .context("Auxide is not connected to voice in this server")?;
        if current != requester {
            bail!("join Auxide's current voice channel before using that command");
        }
        Ok(())
    }

    async fn handle_component(&self, ctx: &Context, component: &ComponentInteraction) {
        let result = self.dispatch_component(ctx, component).await;
        let succeeded = result.is_ok();
        let reply = result.unwrap_or_else(|error| {
            tracing::warn!(%error, custom_id = %component.data.custom_id, "component interaction failed");
            InteractionReply::message(format!("Unable to use that selection: {error}"))
        });
        let announcement = reply.announcement;
        let response = component
            .create_response(
                &ctx.http,
                CreateInteractionResponse::UpdateMessage(
                    update_message(reply.content)
                        .embeds(reply.embeds)
                        .components(reply.components),
                ),
            )
            .await;
        self.observability
            .record_interaction(succeeded && response.is_ok());
        if let Err(error) = response {
            tracing::warn!(%error, "failed to acknowledge component interaction");
        }

        // The picker this replaced was private, so telling the channel what was
        // chosen takes a second message. Losing it costs the announcement only;
        // the track is already queued either way.
        if let Some(announcement) = announcement {
            if let Err(error) = component
                .create_followup(
                    &ctx.http,
                    followup_message(announcement.content).embeds(announcement.embeds),
                )
                .await
            {
                tracing::warn!(%error, "failed to announce a search selection");
            }
        }
    }

    async fn dispatch_component(
        &self,
        ctx: &Context,
        component: &ComponentInteraction,
    ) -> Result<InteractionReply> {
        let authorization = self.authorize_component(ctx, component).await?;
        let (search_id, index) = parse_selection_id(&component.data.custom_id)?;
        let mut searches = self.pending_searches.lock().await;
        prune_searches(&mut searches);
        let pending = searches
            .remove(&search_id)
            .context("this search has expired or was already used")?;
        drop(searches);

        if pending.guild_id != authorization.guild_id
            || pending.response_channel_id != authorization.response_channel_id
            || pending.user_id != authorization.user_id
        {
            bail!("this search belongs to a different request");
        }
        if authorization.voice_channel_id != Some(pending.voice_channel_id) {
            bail!("return to the voice channel where the search was requested");
        }
        let track = pending
            .tracks
            .get(index)
            .cloned()
            .context("the selected result does not exist")?;
        let title = single_line(&track.title, 150);
        let announcement = self
            .enqueue_track(&authorization, pending.voice_channel_id, track)
            .await?;
        Ok(
            InteractionReply::message(format!("Sent **{title}** to the queue."))
                .announcing(announcement),
        )
    }

    async fn shutdown(&self) {
        // Take the map rather than holding its lock, so shutting a player down
        // cannot deadlock against a request creating one.
        let sessions = {
            let mut sessions = self.sessions.lock().await;
            std::mem::take(&mut *sessions)
        };
        for session in sessions.values() {
            if let Err(error) = session.player.shutdown().await {
                tracing::debug!(%error, guild_id = session.player.guild_id(), "guild actor already stopped");
            }
        }
        self.cancellation.cancel();
        let mut tasks = self.tasks.lock().await;
        let draining = std::mem::take(&mut *tasks);
        drop(tasks);
        for mut task in draining {
            if time::timeout(SHUTDOWN_TIMEOUT, &mut task).await.is_err() {
                task.abort();
            }
        }
        self.observability.set_guild_players(0);
    }
}

#[derive(Clone)]
struct Authorization {
    guild_id: u64,
    response_channel_id: u64,
    user_id: u64,
    voice_channel_id: Option<u64>,
    session: GuildSession,
}

/// Decides who a command's answer is for, before Discord is told anything.
///
/// Commands that change what the room hears are announced to the room. Ones that
/// only look something up, and a search whose answer is a list of candidates
/// nobody has chosen between yet, stay with the person who ran them.
fn command_audience(command: &CommandInteraction) -> Audience {
    // Three commands are told apart by an argument: `/play` by whether its
    // query is a link, and `/volume` and `/history` by whether they were given
    // anything to change with. A page number is not one of those.
    let argument = match command.data.name.as_str() {
        "play" => query_option(command),
        "volume" => volume_option(command).map(|_| "level"),
        "history" => integer_option(command, "replay").map(|_| "replay"),
        _ => None,
    };
    audience_for(&command.data.name, argument)
}

fn mode_option(command: &CommandInteraction) -> Option<&str> {
    command
        .data
        .options
        .iter()
        .find(|option| option.name == "mode")
        .and_then(|option| match &option.value {
            CommandDataOptionValue::String(value) => Some(value.as_str()),
            _ => None,
        })
}

fn integer_option(command: &CommandInteraction, name: &str) -> Option<u64> {
    command
        .data
        .options
        .iter()
        .find(|option| option.name == name)
        .and_then(|option| match &option.value {
            CommandDataOptionValue::Integer(value) => u64::try_from(*value).ok(),
            _ => None,
        })
}

fn volume_option(command: &CommandInteraction) -> Option<u16> {
    command
        .data
        .options
        .iter()
        .find(|option| option.name == "level")
        .and_then(|option| match &option.value {
            CommandDataOptionValue::Integer(value) => u16::try_from(*value).ok(),
            _ => None,
        })
}

fn audience_for(command: &str, argument: Option<&str>) -> Audience {
    match command {
        // A URL is played outright, so the answer is the track. Search terms
        // are answered with candidates, and the choice announces itself once it
        // is made.
        "play" if argument.is_some_and(|query| Url::parse(query).is_ok()) => Audience::Channel,
        // Setting the level changes what the room hears; asking what it is does
        // not.
        "volume" | "history" if argument.is_some() => Audience::Channel,
        // Everything that changes what the room hears answers the room.
        // Queueing a whole file does; asking what already played, or saving it,
        // does not.
        "skip" | "stop" | "shuffle" | "repeat" | "pause" | "resume" | "clear" | "remove"
        | "import" | "seek" | "forward" | "rewind" | "restart" | "join" | "leave" => {
            Audience::Channel
        }
        _ => Audience::Requester,
    }
}

fn user_option(command: &CommandInteraction, name: &str) -> Option<u64> {
    command
        .data
        .options
        .iter()
        .find(|option| option.name == name)
        .and_then(|option| match &option.value {
            CommandDataOptionValue::User(id) => Some(id.get()),
            _ => None,
        })
}

/// Reads `3` or `3-7` as the run of waiting positions it names.
///
/// Inclusive at both ends, because that is how the numbers in `/queue` read.
fn parse_track_range(spec: &str) -> Result<(usize, usize)> {
    let spec = spec.trim();
    let (first, last) = spec.split_once('-').unwrap_or((spec, spec));
    let unreadable = || format!("{spec:?} is not a position or a run of them, like 3 or 3-7");
    let first: usize = first.trim().parse().with_context(unreadable)?;
    let last: usize = last.trim().parse().with_context(unreadable)?;
    if first == 0 {
        bail!("positions start at 1");
    }
    if last < first {
        bail!("{spec:?} runs backwards");
    }
    Ok((first, last))
}

/// Reads `90`, `2:30`, or `1:02:03` as the duration it names.
fn parse_timestamp(spec: &str) -> Result<Duration> {
    let spec = spec.trim();
    let mut seconds: u64 = 0;
    let mut parts = 0;
    for part in spec.split(':') {
        let part = part.trim();
        if part.is_empty() || parts == 3 {
            bail!("{spec:?} is not a time like 90, 2:30 or 1:02:03");
        }
        let value: u64 = part
            .parse()
            .with_context(|| format!("{spec:?} is not a time like 90, 2:30 or 1:02:03"))?;
        // Only the leading field may exceed its base, so 1:90 is a mistake
        // rather than two and a half minutes.
        if parts > 0 && value >= 60 {
            bail!("{spec:?} has more than sixty in a minutes or seconds field");
        }
        seconds = seconds
            .checked_mul(60)
            .and_then(|seconds| seconds.checked_add(value))
            .context("that time is too long")?;
        parts += 1;
    }
    if parts == 0 {
        bail!("a time is required");
    }
    Ok(Duration::from_secs(seconds))
}

/// How many people have to agree before one of them cuts a track short.
///
/// Half the channel, but never fewer than two: with one other person present a
/// threshold of one would mean there was no vote at all. Somebody listening
/// alone never needs anybody's agreement.
fn votes_needed(listeners: usize) -> usize {
    if listeners <= 1 {
        return 1;
    }
    listeners.div_ceil(2).max(2)
}

fn query_option(command: &CommandInteraction) -> Option<&str> {
    string_option(command, "query")
}

fn string_option<'a>(command: &'a CommandInteraction, name: &str) -> Option<&'a str> {
    command
        .data
        .options
        .iter()
        .find(|option| option.name == name)
        .and_then(|option| match &option.value {
            CommandDataOptionValue::String(value) => Some(value.as_str()),
            _ => None,
        })
}

/// Renders a requester as a mention, which [`no_mentions`] keeps from pinging.
fn mention(user_id: u64) -> String {
    format!("<@{user_id}>")
}

/// Builds the card Auxide shows for one track.
///
/// Everything on it was already being parsed out of the source and carried
/// through the queue: the uploader, the artwork, and who asked for it were
/// collected from the first version of the resolver and had never reached
/// Discord.
///
/// `heading` is what happened rather than what the track is — the same track
/// is a different event when it starts playing than when it joins a queue.
/// `position` is present only for a track that has to wait its turn.
fn track_embed(
    heading: &str,
    track: &TrackMetadata,
    requested_by: u64,
    position: Option<usize>,
) -> CreateEmbed {
    let mut embed = CreateEmbed::new()
        .author(CreateEmbedAuthor::new(heading))
        .title(single_line(&track.title, EMBED_TITLE_CHARS))
        .url(track.canonical_url.as_str())
        .colour(EMBED_COLOUR)
        .field("Duration", format_duration(track.duration), true);
    if let Some(position) = position {
        embed = embed.field("Position", position.to_string(), true);
    }
    // A mention resolves inside a field value, and `no_mentions` still keeps it
    // from notifying anybody — the same trade the plain-text replies make.
    embed = embed.field("Requested by", mention(requested_by), true);
    if let Some(channel) = &track.channel {
        // Footers are plain text, so an uploader called `**bold**` renders as
        // written rather than as markup.
        embed = embed.footer(CreateEmbedFooter::new(single_line(
            channel,
            EMBED_FOOTER_CHARS,
        )));
    }
    // Already parsed as an HTTPS URL by the resolver, which is what makes it
    // safe to hand to Discord to fetch.
    if let Some(thumbnail) = &track.thumbnail_url {
        embed = embed.thumbnail(thumbnail.as_str());
    }
    embed
}

/// Chooses where an unprompted message goes.
///
/// A configured channel is a decision and beats the fallback. Without one, the
/// channel the session was started from is where the people who care about the
/// answer already are.
fn announcement_channel(guild: Option<&GuildConfig>, origin: Option<u64>) -> Option<u64> {
    guild.and_then(|guild| guild.announce_channel_id).or(origin)
}

/// Names the state of the current track, so a held one does not read as playing.
const fn playback_heading(paused: bool) -> &'static str {
    if paused { "Paused" } else { "Now playing" }
}

/// Converts a session's whole-percent level into the scale Songbird takes.
fn volume_scale(percent: u16) -> f32 {
    f32::from(percent.clamp(1, MAX_VOLUME_PERCENT)) / 100.0
}

fn idle_hold(config: &Config) -> String {
    format_hold(Duration::from_secs(config.playback.idle_timeout_seconds))
}

/// Renders an idle timeout the way someone waiting through it would say it.
fn format_hold(hold: Duration) -> String {
    let minutes = hold.as_secs() / 60;
    match minutes {
        0 => "under a minute".to_owned(),
        1 => "1 minute".to_owned(),
        _ => format!("{minutes} minutes"),
    }
}

fn identity_is_authorized(guild: &GuildConfig, user_id: u64, role_ids: &[u64]) -> bool {
    if guild.authorized_user_ids.is_empty() && guild.authorized_role_ids.is_empty() {
        return true;
    }
    guild.authorized_user_ids.contains(&user_id)
        || role_ids
            .iter()
            .any(|role| guild.authorized_role_ids.contains(role))
}

fn requester_voice_channel(ctx: &Context, guild_id: u64, user_id: UserId) -> Option<u64> {
    ctx.cache
        .guild(GuildId::new(guild_id))
        .and_then(|guild| {
            guild
                .voice_states
                .get(&user_id)
                .and_then(|state| state.channel_id)
        })
        .map(ChannelId::get)
}

/// A queue written out to a file, and read back from one.
///
/// Whole tracks rather than links, because the queue has always held canonical
/// identity and metadata while media URLs are resolved just before playback —
/// so a round trip costs no resolutions and expires nothing.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Export {
    tracks: Vec<TrackMetadata>,
}

fn attachment_option<'a>(
    command: &'a CommandInteraction,
    name: &str,
) -> Option<&'a serenity::model::channel::Attachment> {
    let id = command
        .data
        .options
        .iter()
        .find(|option| option.name == name)
        .and_then(|option| match &option.value {
            CommandDataOptionValue::Attachment(id) => Some(*id),
            _ => None,
        })?;
    command.data.resolved.attachments.get(&id)
}

/// One occupant of a voice channel, reduced to what deciding to leave needs.
///
/// Reading the cache and judging it are separated so the judgement can be
/// tested without standing up a gateway, a cache, and a guild.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct VoiceOccupant {
    user_id: u64,
    channel_id: Option<u64>,
    is_bot: bool,
}

/// What Auxide's own voice channel looks like right now.
///
/// The cache is authoritative here rather than the player's own record of the
/// channel, so an administrator dragging Auxide elsewhere moves the question to
/// the channel it actually sits in. Returning `None` while a join is still in
/// flight is what keeps that race from reading as an empty channel.
fn occupied_voice_channel(ctx: &Context, guild_id: GuildId) -> Option<Occupancy> {
    let bot_user_id = ctx.cache.current_user().id.get();
    let guild = ctx.cache.guild(guild_id)?;
    let occupants = guild
        .voice_states
        .values()
        .map(|state| VoiceOccupant {
            user_id: state.user_id.get(),
            channel_id: state.channel_id.map(ChannelId::get),
            // A member reaches the cache with every voice state update, so this
            // is known for everyone who has moved since startup. Anyone
            // unaccounted for counts as a listener, which errs towards staying.
            is_bot: guild
                .members
                .get(&state.user_id)
                .or(state.member.as_ref())
                .is_some_and(|member| member.user.bot),
        })
        .collect::<Vec<_>>();
    occupancy(&occupants, bot_user_id)
}

/// Counts the people who would hear a track being cut short.
fn listener_count(ctx: &Context, guild_id: GuildId) -> usize {
    occupied_voice_channel(ctx, guild_id).map_or(1, |seen| seen.listeners.max(1))
}

/// Auxide's channel, and how many people are in it with it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Occupancy {
    channel_id: u64,
    /// People in it, not counting Auxide or any other bot.
    listeners: usize,
}

/// Counts the people in whichever channel Auxide occupies.
fn occupancy(occupants: &[VoiceOccupant], bot_user_id: u64) -> Option<Occupancy> {
    let channel_id = occupants
        .iter()
        .find(|occupant| occupant.user_id == bot_user_id)
        .and_then(|occupant| occupant.channel_id)?;
    let listeners = occupants
        .iter()
        .filter(|occupant| {
            occupant.channel_id == Some(channel_id)
                && occupant.user_id != bot_user_id
                && !occupant.is_bot
        })
        .count();
    Some(Occupancy {
        channel_id,
        listeners,
    })
}

#[allow(clippy::too_many_arguments)]
async fn playback_worker(
    guild_id: u64,
    player: GuildPlayerHandle,
    mut transitions: mpsc::Receiver<PlayerTransition>,
    voice: Arc<dyn VoiceGateway>,
    announcer: Announcer,
    observability: ObservabilityState,
    cancellation: CancellationToken,
) {
    let mut deferred: Option<PlayerTransition> = None;

    loop {
        let transition = if let Some(transition) = deferred.take() {
            Some(transition)
        } else {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => None,
                transition = transitions.recv() => transition,
            }
        };
        let Some(transition) = transition else {
            break;
        };
        // Every transition carries the session's level, so the worker never has
        // to be told it separately — a track starting later reads it from here
        // and a change to it while one plays arrives as its own directive.
        let volume = volume_scale(transition.snapshot.volume_percent);

        match transition.directive {
            PlaybackDirective::None => {}
            PlaybackDirective::Stop => voice.stop().await,
            PlaybackDirective::Join(channel_id) => {
                if let Err(error) = voice.join(channel_id).await {
                    tracing::warn!(%error, guild_id, channel_id, "failed to take a voice channel");
                }
            }
            PlaybackDirective::Pause => voice.pause().await,
            PlaybackDirective::Resume => voice.resume().await,
            PlaybackDirective::SetVolume => voice.set_volume(volume).await,
            PlaybackDirective::StopAndDisconnect | PlaybackDirective::Disconnect => {
                voice.leave().await;
            }
            PlaybackDirective::IdleDisconnect => {
                voice.leave().await;
                // The only departure nobody asked for, so the only one that has
                // to explain itself.
                announcer
                    .announce(
                        guild_id,
                        transition.snapshot.text_channel_id,
                        Announcement::text(format!(
                            "Nothing has been queued for {}, so I have left the voice channel.",
                            announcer.idle_hold()
                        )),
                    )
                    .await;
            }
            PlaybackDirective::Play(item) | PlaybackDirective::Replace(item) => {
                voice.stop().await;
                let Some(channel_id) = transition.snapshot.voice_channel_id else {
                    tracing::warn!(guild_id, queue_id = %item.queue_id, "play directive had no voice channel");
                    continue;
                };
                match start_track(
                    guild_id,
                    channel_id,
                    &player,
                    &mut transitions,
                    voice.as_ref(),
                    &announcer,
                    &observability,
                    &cancellation,
                    item,
                    volume,
                )
                .await
                {
                    StartTrackOutcome::Started | StartTrackOutcome::Failed => {}
                    StartTrackOutcome::Superseded(next) => deferred = Some(*next),
                    StartTrackOutcome::Shutdown => break,
                }
            }
        }
    }

    voice.leave().await;
}

enum StartTrackOutcome {
    Started,
    Superseded(Box<PlayerTransition>),
    Failed,
    Shutdown,
}

#[allow(clippy::too_many_arguments)]
async fn start_track(
    guild_id: u64,
    channel_id: u64,
    player: &GuildPlayerHandle,
    transitions: &mut mpsc::Receiver<PlayerTransition>,
    voice: &dyn VoiceGateway,
    announcer: &Announcer,
    observability: &ObservabilityState,
    cancellation: &CancellationToken,
    item: QueueItem,
    volume: f32,
) -> StartTrackOutcome {
    // Preparing reaches the network and takes seconds, so a skip arriving in
    // the middle of it has to be able to supersede the track being prepared.
    let preparation = voice.prepare(&item.track);
    tokio::pin!(preparation);
    let prepared = tokio::select! {
        biased;
        () = cancellation.cancelled() => return StartTrackOutcome::Shutdown,
        next = transitions.recv() => {
            return next.map_or(StartTrackOutcome::Shutdown, |transition| {
                StartTrackOutcome::Superseded(Box::new(transition))
            });
        }
        result = &mut preparation => result,
    };
    observability.record_source_resolution(prepared.is_ok());
    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            tracing::warn!(
                %error,
                guild_id,
                queue_id = %item.queue_id,
                source_id = %item.track.source_id,
                "failed to prepare queued source; advancing"
            );
            // Without this the track simply is not there any more, and nothing
            // in Discord distinguishes a video that went private from a bot
            // that dropped the request. The reason is bounded and stripped of
            // line breaks because it comes from a resolver reporting on input
            // nobody here controls.
            announcer
                .announce(
                    guild_id,
                    Some(item.response_channel_id),
                    Announcement::text(format!(
                        "Skipping **{}** — {}.",
                        single_line(&item.track.title, 150),
                        single_line(&error.to_string(), 200)
                    )),
                )
                .await;
            let _ = player.track_finished(item.queue_id).await;
            return StartTrackOutcome::Failed;
        }
    };
    let current = match player.snapshot().await {
        Ok(snapshot) => snapshot.current.map(|queued| queued.queue_id),
        Err(error) => {
            tracing::warn!(%error, guild_id, "failed to verify queued track");
            return StartTrackOutcome::Failed;
        }
    };
    if current != Some(item.queue_id) {
        tracing::debug!(guild_id, queue_id = %item.queue_id, "discarding stale prepared source");
        return StartTrackOutcome::Failed;
    }

    let ended = Arc::new(RuntimeTrackFinished {
        player: player.clone(),
        guild_id,
        queue_id: item.queue_id,
    });
    if let Err(error) = voice.play(channel_id, prepared, volume, ended).await {
        tracing::warn!(%error, guild_id, channel_id, "failed to start playback; advancing");
        let _ = player.track_finished(item.queue_id).await;
        return StartTrackOutcome::Failed;
    }
    tracing::info!(
        guild_id,
        channel_id,
        queue_id = %item.queue_id,
        source_id = %item.track.source_id,
        title = %single_line(&item.track.title, 200),
        "playback started"
    );
    if announcer.announces_tracks(guild_id) {
        announcer
            .announce(
                guild_id,
                Some(item.response_channel_id),
                Announcement::card(track_embed(
                    "Now playing",
                    &item.track,
                    item.requested_by,
                    None,
                )),
            )
            .await;
    }
    StartTrackOutcome::Started
}

struct RuntimeTrackFinished {
    player: GuildPlayerHandle,
    guild_id: u64,
    queue_id: Uuid,
}

#[async_trait]
impl TrackEnded for RuntimeTrackFinished {
    async fn ended(&self) {
        if let Err(error) = self.player.track_finished(self.queue_id).await {
            tracing::debug!(%error, guild_id = self.guild_id, queue_id = %self.queue_id, "track completion arrived after shutdown");
        }
    }
}

fn parse_selection_id(custom_id: &str) -> Result<(Uuid, usize)> {
    let mut parts = custom_id.split(':');
    if parts.next() != Some("auxide") || parts.next() != Some("select") {
        bail!("unknown Auxide control");
    }
    let search_id = parts
        .next()
        .context("selection search ID was missing")?
        .parse()
        .context("selection search ID was invalid")?;
    let index = parts
        .next()
        .context("selection index was missing")?
        .parse()
        .context("selection index was invalid")?;
    if parts.next().is_some() {
        bail!("selection control had unexpected data");
    }
    Ok((search_id, index))
}

fn prune_searches(searches: &mut HashMap<Uuid, PendingSearch>) {
    let now = Instant::now();
    searches.retain(|_, pending| now.duration_since(pending.created_at) < SEARCH_SELECTION_TTL);
}

/// Renders the queue, or `None` when there is nothing in it.
///
/// Each line names who asked for the track. In a queue four people are filling
/// at once that is the thing worth knowing, and it was recorded on every item
/// from the first version of the player without ever being shown.
/// How many waiting tracks one page of `/queue` lists.
///
/// A fixed count rather than as many as fit, so a position printed on page two
/// is the position `/remove` takes, whatever the titles happen to be.
const QUEUE_PAGE_SIZE: usize = 20;

/// One page of the queue, and where it sits among the rest.
struct QueuePage {
    description: String,
    page: usize,
    pages: usize,
}

/// Renders a page of the queue, or `None` when there is nothing in it.
///
/// Each line names who asked for the track. In a queue four people are filling
/// at once that is the thing worth knowing, and it was recorded on every item
/// from the first version of the player without ever being shown.
fn format_queue(snapshot: &PlayerSnapshot, page: usize) -> Option<QueuePage> {
    let current = snapshot.current.as_ref()?;
    let pages = snapshot.pending.len().div_ceil(QUEUE_PAGE_SIZE).max(1);
    let page = page.clamp(1, pages);
    let mut description = format!(
        "**{}**\n{} ({}) — {}\n",
        playback_heading(snapshot.paused()),
        single_line(&current.track.title, 120),
        format_duration(current.track.duration),
        mention(current.requested_by)
    );
    if snapshot.pending.is_empty() {
        description.push_str("\nNo tracks are waiting.");
        return Some(QueuePage {
            description,
            page,
            pages,
        });
    }

    let first = (page - 1) * QUEUE_PAGE_SIZE;
    description.push_str("\n**Up next**\n");
    for (offset, item) in snapshot
        .pending
        .iter()
        .skip(first)
        .take(QUEUE_PAGE_SIZE)
        .enumerate()
    {
        // Numbered by position in the whole queue rather than on the page, so
        // the number a reader sees is the one `/remove` takes.
        let line = format!(
            "{}. {} ({}) — {}\n",
            first + offset + 1,
            single_line(&item.track.title, 100),
            format_duration(item.track.duration),
            mention(item.requested_by)
        );
        // A page is bounded by its track count, but titles are not bounded at
        // all, so the character limit is still the backstop.
        if description.chars().count() + line.chars().count() > EMBED_DESCRIPTION_CHARS {
            let _ = write!(description, "…and more.");
            break;
        }
        description.push_str(&line);
    }
    Some(QueuePage {
        description,
        page,
        pages,
    })
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let seconds = seconds % 60;
    if hours > 0 {
        format!("{hours}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes}:{seconds:02}")
    }
}

fn single_line(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        normalized
    } else {
        normalized
            .chars()
            .take(max_chars.saturating_sub(1))
            .collect::<String>()
            + "…"
    }
}

/// Trims a string to what an embed description will hold.
fn bounded_embed(value: String) -> String {
    if value.chars().count() <= EMBED_DESCRIPTION_CHARS {
        value
    } else {
        value
            .chars()
            .take(EMBED_DESCRIPTION_CHARS - 1)
            .collect::<String>()
            + "…"
    }
}

fn bounded_message(value: String) -> String {
    if value.chars().count() <= 1_950 {
        value
    } else {
        value.chars().take(1_949).collect::<String>() + "…"
    }
}

fn no_mentions() -> CreateAllowedMentions {
    CreateAllowedMentions::new()
        .everyone(false)
        .all_roles(false)
        .all_users(false)
        .replied_user(false)
}

fn response_message(content: impl Into<String>) -> CreateInteractionResponseMessage {
    CreateInteractionResponseMessage::new()
        .content(bounded_message(content.into()))
        .ephemeral(true)
        .allowed_mentions(no_mentions())
}

fn update_message(content: impl Into<String>) -> CreateInteractionResponseMessage {
    CreateInteractionResponseMessage::new()
        .content(bounded_message(content.into()))
        .allowed_mentions(no_mentions())
}

/// Builds a follow-up message, which is channel-visible unless made ephemeral.
///
/// Follow-ups travel on the interaction's own token, so they reach the channel
/// without Auxide holding permission to post there of its own accord.
fn followup_message(content: impl Into<String>) -> CreateInteractionResponseFollowup {
    CreateInteractionResponseFollowup::new()
        .content(bounded_message(content.into()))
        .allowed_mentions(no_mentions())
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

/// End-to-end tests of a real player driving a real worker, with a fake voice
/// channel on the far side.
///
/// Everything here was unprovable before the gateway became a trait: each test
/// asserts on the sequence of things that reached voice, which is the step
/// between a queue deciding something and anybody hearing it.
#[cfg(test)]
mod playback_tests {
    use std::time::Duration;

    use tokio::time;
    use url::Url;

    use super::{Announcer, playback_worker};
    use crate::voice::VoiceGateway as _;
    use crate::{
        config::Config,
        observability::ObservabilityState,
        player::{Hold, QueueItem, Release, RepeatMode, spawn_guild_player},
        source::TrackMetadata,
        voice::fake::{FakeVoice, VoiceAction},
    };
    use serenity::http::Http;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    const GUILD: u64 = 7;
    const VOICE_CHANNEL: u64 = 10;

    fn config() -> Arc<Config> {
        Arc::new(
            toml::from_str(
                r#"
[discord]
token_file = "/dev/null"

[playback]
idle_timeout_seconds = 900
"#,
            )
            .expect("the test configuration parses"),
        )
    }

    fn item(id: u128) -> QueueItem {
        QueueItem {
            queue_id: uuid::Uuid::from_u128(id),
            track: TrackMetadata {
                source_id: format!("source-{id}"),
                canonical_url: Url::parse(&format!("https://www.youtube.com/watch?v=id{id}"))
                    .unwrap(),
                title: format!("Track {id}"),
                channel: None,
                duration: Duration::from_secs(60),
                thumbnail_url: None,
            },
            requested_by: 42,
            response_channel_id: 99,
        }
    }

    /// A player, a worker, and a fake voice channel, wired as production wires
    /// them.
    struct Session {
        player: crate::player::GuildPlayerHandle,
        voice: FakeVoice,
        cancellation: CancellationToken,
        worker: tokio::task::JoinHandle<()>,
    }

    impl Session {
        fn start(idle_timeout: Duration) -> Self {
            let (player, transitions, _actor) =
                spawn_guild_player(GUILD, 100, 128, 1, idle_timeout, 50);
            let voice = FakeVoice::new();
            let cancellation = CancellationToken::new();
            // No announcement channel is configured and no request has been
            // answered, so the announcer stays silent and never reaches Discord.
            let announcer = Announcer {
                http: Arc::new(Http::new("not-a-real-token")),
                config: config(),
            };
            let worker = tokio::spawn(playback_worker(
                GUILD,
                player.clone(),
                transitions,
                Arc::new(voice.clone()),
                announcer,
                ObservabilityState::default(),
                cancellation.child_token(),
            ));
            Self {
                player,
                voice,
                cancellation,
                worker,
            }
        }

        async fn finish(self) {
            self.cancellation.cancel();
            let _ = time::timeout(Duration::from_secs(2), self.worker).await;
        }
    }

    fn played(id: u128, volume_percent: u16) -> VoiceAction {
        VoiceAction::Played {
            channel_id: VOICE_CHANNEL,
            source_id: format!("source-{id}"),
            volume_percent,
        }
    }

    #[tokio::test]
    async fn a_queued_track_reaches_the_voice_channel_at_the_session_level() {
        let session = Session::start(Duration::from_secs(900));
        session
            .player
            .enqueue(item(1), VOICE_CHANNEL)
            .await
            .unwrap();
        session.voice.settle(2).await;

        // Stopped first: starting a track replaces whatever was playing, and
        // doing that in the other order would cut off the track it started.
        assert_eq!(
            session.voice.actions(),
            [VoiceAction::Stopped, played(1, 50)]
        );
        session.finish().await;
    }

    #[tokio::test]
    async fn a_whole_playlist_produces_one_join_and_one_play() {
        let session = Session::start(Duration::from_secs(900));
        let items = (1..=5).map(item).collect::<Vec<_>>();
        let bulk = session
            .player
            .enqueue_all(items, VOICE_CHANNEL)
            .await
            .unwrap();
        assert_eq!(bulk.accepted, 5);
        session.voice.settle(2).await;

        // The bug this pins: returning the last transition of a batch rather
        // than the first meaningful one told the worker to play nothing, so a
        // playlist queued silently and never started.
        assert_eq!(
            session.voice.actions(),
            [VoiceAction::Stopped, played(1, 50)]
        );
        session.finish().await;
    }

    #[tokio::test]
    async fn a_track_that_cannot_be_prepared_is_skipped_rather_than_stalling() {
        let session = Session::start(Duration::from_secs(900));
        session.voice.refuse("source-1");
        session
            .player
            .enqueue(item(1), VOICE_CHANNEL)
            .await
            .unwrap();
        session
            .player
            .enqueue(item(2), VOICE_CHANNEL)
            .await
            .unwrap();
        session.voice.settle(3).await;

        // The first never plays, and the queue moves past it on its own.
        assert_eq!(
            session.voice.actions(),
            [VoiceAction::Stopped, VoiceAction::Stopped, played(2, 50)]
        );
        session.finish().await;
    }

    #[tokio::test]
    async fn holding_and_continuing_reach_the_track_that_is_playing() {
        let session = Session::start(Duration::from_secs(900));
        session
            .player
            .enqueue(item(1), VOICE_CHANNEL)
            .await
            .unwrap();
        session.voice.settle(2).await;

        session.player.hold(Hold::Requested).await.unwrap();
        session.voice.settle(3).await;
        session.player.release(Release::Anything).await.unwrap();
        session.voice.settle(4).await;
        session.player.set_volume(30).await.unwrap();
        session.voice.settle(5).await;

        assert_eq!(
            session.voice.actions()[2..],
            [
                VoiceAction::Paused,
                VoiceAction::Resumed,
                VoiceAction::VolumeSet(30)
            ]
        );
        session.finish().await;
    }

    #[tokio::test]
    async fn a_level_set_mid_session_applies_to_the_next_track_too() {
        let session = Session::start(Duration::from_secs(900));
        session
            .player
            .enqueue(item(1), VOICE_CHANNEL)
            .await
            .unwrap();
        session
            .player
            .enqueue(item(2), VOICE_CHANNEL)
            .await
            .unwrap();
        session.voice.settle(2).await;
        session.player.set_volume(20).await.unwrap();
        session.voice.settle(3).await;

        session.voice.finish_track().await;
        session.voice.settle(5).await;
        assert_eq!(
            session.voice.actions()[3..],
            [VoiceAction::Stopped, played(2, 20)]
        );
        session.finish().await;
    }

    #[tokio::test]
    async fn an_exhausted_queue_stops_playing_but_keeps_the_channel() {
        let session = Session::start(Duration::from_secs(900));
        session
            .player
            .enqueue(item(1), VOICE_CHANNEL)
            .await
            .unwrap();
        session.voice.settle(2).await;

        session.voice.finish_track().await;
        session.voice.settle(3).await;
        // Stopped, not Left: the channel is held for the whole idle timeout so
        // the next request joins a bot that is already connected.
        assert_eq!(session.voice.actions()[2..], [VoiceAction::Stopped]);
        session.finish().await;
    }

    #[tokio::test]
    async fn an_expired_idle_hold_gives_up_the_channel() {
        let session = Session::start(Duration::from_millis(80));
        session
            .player
            .enqueue(item(1), VOICE_CHANNEL)
            .await
            .unwrap();
        session.voice.settle(2).await;
        session.voice.finish_track().await;

        time::timeout(Duration::from_secs(2), session.voice.settle(4))
            .await
            .expect("the idle hold expired without leaving");
        assert_eq!(
            session.voice.actions()[2..],
            [VoiceAction::Stopped, VoiceAction::Left]
        );
        session.finish().await;
    }

    #[tokio::test]
    async fn a_track_queued_inside_the_idle_window_keeps_the_channel() {
        let session = Session::start(Duration::from_millis(200));
        session
            .player
            .enqueue(item(1), VOICE_CHANNEL)
            .await
            .unwrap();
        session.voice.settle(2).await;
        session.voice.finish_track().await;
        session.voice.settle(3).await;

        session
            .player
            .enqueue(item(2), VOICE_CHANNEL)
            .await
            .unwrap();
        session.voice.settle(5).await;
        time::sleep(Duration::from_millis(400)).await;
        assert!(
            !session.voice.actions().contains(&VoiceAction::Left),
            "the disconnect armed by the first track survived the second"
        );
        session.finish().await;
    }

    #[tokio::test]
    async fn clearing_the_queue_never_reaches_the_voice_channel() {
        let session = Session::start(Duration::from_secs(900));
        session
            .player
            .enqueue(item(1), VOICE_CHANNEL)
            .await
            .unwrap();
        session
            .player
            .enqueue(item(2), VOICE_CHANNEL)
            .await
            .unwrap();
        session.voice.settle(2).await;

        session.player.clear().await.unwrap();
        // Nothing at all: the track carries on and the channel is kept. This is
        // the entire difference between /clear and /stop, and it is invisible
        // at the actor, which reports the same empty pending list either way.
        time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            session.voice.actions(),
            [VoiceAction::Stopped, played(1, 50)]
        );

        // And the cleared track really is gone, so finishing moves to silence
        // rather than to what used to be queued behind it.
        session.voice.finish_track().await;
        session.voice.settle(3).await;
        assert_eq!(session.voice.actions()[2..], [VoiceAction::Stopped]);
        session.finish().await;
    }

    #[tokio::test]
    async fn removing_a_waiting_track_never_reaches_the_voice_channel() {
        let session = Session::start(Duration::from_secs(900));
        for id in 1..=3 {
            session
                .player
                .enqueue(item(id), VOICE_CHANNEL)
                .await
                .unwrap();
        }
        session.voice.settle(2).await;

        session.player.remove(1).await.unwrap();
        time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            session.voice.actions().len(),
            2,
            "removing disturbed playback"
        );

        // The one that was removed is skipped over, and the one behind it plays.
        session.voice.finish_track().await;
        session.voice.settle(4).await;
        assert_eq!(
            session.voice.actions()[2..],
            [VoiceAction::Stopped, played(3, 50)]
        );
        session.finish().await;
    }

    #[tokio::test]
    async fn a_repeating_queue_plays_the_same_track_again() {
        let session = Session::start(Duration::from_millis(80));
        session
            .player
            .enqueue(item(1), VOICE_CHANNEL)
            .await
            .unwrap();
        session.voice.settle(2).await;
        session.player.set_repeat(RepeatMode::Single).await.unwrap();

        session.voice.finish_track().await;
        session.voice.settle(4).await;
        assert_eq!(
            session.voice.actions()[2..],
            [VoiceAction::Stopped, played(1, 50)]
        );
        // A cycling queue never runs dry, so the idle hold it would otherwise
        // have armed never fires.
        time::sleep(Duration::from_millis(250)).await;
        assert!(!session.voice.actions().contains(&VoiceAction::Left));
        session.finish().await;
    }

    #[tokio::test]
    async fn moving_the_playhead_reaches_the_track_that_is_playing() {
        use crate::voice::SeekTo;

        let session = Session::start(Duration::from_secs(900));
        session
            .player
            .enqueue(item(1), VOICE_CHANNEL)
            .await
            .unwrap();
        session.voice.settle(2).await;

        // The gateway is asked where to go rather than told an offset, because
        // only it knows where the playhead currently is.
        session
            .voice
            .seek(SeekTo::Forward(Duration::from_secs(30)))
            .await
            .unwrap();
        session.voice.settle(3).await;
        assert_eq!(
            session.voice.actions()[2..],
            [VoiceAction::Sought(SeekTo::Forward(Duration::from_secs(
                30
            )))]
        );
        session.finish().await;
    }

    #[tokio::test]
    async fn an_empty_room_pauses_and_a_return_continues() {
        let session = Session::start(Duration::from_secs(900));
        session
            .player
            .enqueue(item(1), VOICE_CHANNEL)
            .await
            .unwrap();
        session.voice.settle(2).await;

        // Everyone left: the track is held rather than the session ended, so
        // the queue survives a trip to the kitchen.
        session.player.hold(Hold::Abandoned).await.unwrap();
        session.voice.settle(3).await;
        assert_eq!(session.voice.actions()[2..], [VoiceAction::Paused]);

        session
            .player
            .release(Release::OnlyAbandoned)
            .await
            .unwrap();
        session.voice.settle(4).await;
        assert_eq!(session.voice.actions()[3..], [VoiceAction::Resumed]);
        assert!(
            !session.voice.actions().contains(&VoiceAction::Left),
            "an empty room gave up the channel instead of waiting"
        );
        session.finish().await;
    }

    #[tokio::test]
    async fn stopping_gives_up_the_channel_where_an_empty_queue_does_not() {
        let session = Session::start(Duration::from_secs(900));
        session
            .player
            .enqueue(item(1), VOICE_CHANNEL)
            .await
            .unwrap();
        session.voice.settle(2).await;

        session.player.stop().await.unwrap();
        session.voice.settle(3).await;
        assert_eq!(session.voice.actions()[2..], [VoiceAction::Left]);
        session.finish().await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use anyhow::anyhow;

    use super::*;
    use crate::player::PlayerError;

    fn guild() -> GuildConfig {
        GuildConfig {
            guild_id: 1,
            command_channel_ids: BTreeSet::new(),
            authorized_role_ids: BTreeSet::new(),
            authorized_user_ids: BTreeSet::new(),
            announce_channel_id: None,
            announce_tracks: false,
        }
    }

    #[test]
    fn authorization_is_open_only_when_both_identity_lists_are_empty() {
        let mut config = guild();
        assert!(identity_is_authorized(&config, 10, &[]));
        config.authorized_user_ids.insert(11);
        assert!(!identity_is_authorized(&config, 10, &[]));
        assert!(identity_is_authorized(&config, 11, &[]));
        config.authorized_role_ids.insert(22);
        assert!(identity_is_authorized(&config, 10, &[22]));
    }

    #[test]
    fn selection_ids_are_strict() {
        let id = Uuid::new_v4();
        assert_eq!(
            parse_selection_id(&format!("auxide:select:{id}:3")).unwrap(),
            (id, 3)
        );
        assert!(parse_selection_id(&format!("auxide:select:{id}:3:extra")).is_err());
        assert!(parse_selection_id("something:else").is_err());
    }

    #[test]
    fn untrusted_titles_are_single_line_and_bounded() {
        assert_eq!(single_line("hello\n@everyone", 100), "hello @everyone");
        assert_eq!(single_line("abcdefgh", 5), "abcd…");
        assert!(bounded_message("x".repeat(4_000)).chars().count() <= 1_950);
    }

    fn track(title: &str) -> TrackMetadata {
        TrackMetadata {
            source_id: "id".to_owned(),
            canonical_url: Url::parse("https://www.youtube.com/watch?v=id").unwrap(),
            title: title.to_owned(),
            channel: Some("Example Channel".to_owned()),
            duration: Duration::from_secs(120),
            thumbnail_url: Some(Url::parse("https://i.ytimg.com/example.jpg").unwrap()),
        }
    }

    #[test]
    fn queue_rendering_stays_inside_discord_limits() {
        let item = QueueItem::new(track(&"x".repeat(500)), 1, 2);
        let snapshot = PlayerSnapshot {
            current: Some(item.clone()),
            pending: vec![item; 100],
            voice_channel_id: Some(3),
            text_channel_id: Some(2),
            ..PlayerSnapshot::default()
        };
        let rendered = format_queue(&snapshot, 1).unwrap();
        assert!(rendered.description.chars().count() <= EMBED_DESCRIPTION_CHARS + 100);
        assert_eq!(rendered.pages, 5);
    }

    #[test]
    fn the_queue_names_who_asked_for_each_track() {
        let snapshot = PlayerSnapshot {
            current: Some(QueueItem::new(track("Playing"), 11, 2)),
            pending: vec![QueueItem::new(track("Waiting"), 22, 2)],
            voice_channel_id: Some(3),
            text_channel_id: Some(2),
            ..PlayerSnapshot::default()
        };
        let rendered = format_queue(&snapshot, 1).unwrap();
        assert!(
            rendered
                .description
                .contains("**Now playing**\nPlaying (2:00) — <@11>")
        );
        assert!(rendered.description.contains("1. Waiting (2:00) — <@22>"));
    }

    #[test]
    fn a_second_page_keeps_numbering_from_the_whole_queue() {
        let snapshot = PlayerSnapshot {
            current: Some(QueueItem::new(track("Playing"), 11, 2)),
            pending: (0..25)
                .map(|index| QueueItem::new(track(&format!("Waiting {index}")), 22, 2))
                .collect(),
            ..PlayerSnapshot::default()
        };

        let first = format_queue(&snapshot, 1).unwrap();
        assert_eq!((first.page, first.pages), (1, 2));
        assert!(first.description.contains("1. Waiting 0 "));
        assert!(first.description.contains("20. Waiting 19 "));
        assert!(!first.description.contains("21. "));

        // Positions continue rather than restarting, because the number a
        // reader sees is the number /remove takes.
        let second = format_queue(&snapshot, 2).unwrap();
        assert_eq!((second.page, second.pages), (2, 2));
        assert!(second.description.contains("21. Waiting 20 "));
        assert!(second.description.contains("25. Waiting 24 "));

        // A page past the end shows the last one rather than an empty list.
        assert_eq!(format_queue(&snapshot, 99).unwrap().page, 2);
    }

    #[test]
    fn an_empty_queue_has_nothing_to_render() {
        assert!(format_queue(&PlayerSnapshot::default(), 1).is_none());
    }

    #[test]
    fn a_track_card_carries_the_metadata_and_bounds_it() {
        let embed = track_embed("Added to the queue", &track(&"y".repeat(400)), 7, Some(3));
        let json = serde_json::to_value(&embed).unwrap();
        assert_eq!(json["author"]["name"], "Added to the queue");
        assert_eq!(json["url"], "https://www.youtube.com/watch?v=id");
        assert_eq!(json["footer"]["text"], "Example Channel");
        assert_eq!(json["thumbnail"]["url"], "https://i.ytimg.com/example.jpg");
        let fields = json["fields"].as_array().unwrap();
        assert_eq!(fields[0]["value"], "2:00");
        assert_eq!(fields[1]["value"], "3");
        assert_eq!(fields[2]["value"], "<@7>");
        // Discord rejects a title over 256 characters outright, so an
        // over-long one has to be cut before it is sent, not after.
        assert!(json["title"].as_str().unwrap().chars().count() <= 256);
    }

    #[test]
    fn a_playing_track_has_no_position_to_wait_in() {
        let embed = track_embed("Now playing", &track("Playing"), 7, None);
        let json = serde_json::to_value(&embed).unwrap();
        let names: Vec<_> = json["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| field["name"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(names, ["Duration", "Requested by"]);
    }

    #[test]
    fn formats_track_durations() {
        assert_eq!(format_duration(Duration::from_secs(65)), "1:05");
        assert_eq!(format_duration(Duration::from_secs(3_665)), "1:01:05");
    }

    #[test]
    fn an_unprompted_message_prefers_a_configured_channel_over_the_request_it_came_from() {
        let mut config = guild();
        assert_eq!(announcement_channel(Some(&config), Some(50)), Some(50));
        config.announce_channel_id = Some(60);
        assert_eq!(announcement_channel(Some(&config), Some(50)), Some(60));
        assert_eq!(announcement_channel(Some(&config), None), Some(60));
    }

    #[test]
    fn a_server_with_no_entry_and_no_request_channel_stays_silent() {
        // Staying silent is the supported outcome, not a failure: it is how
        // every version before announcements behaved.
        assert_eq!(announcement_channel(None, None), None);
        assert_eq!(announcement_channel(Some(&guild()), None), None);
        assert_eq!(announcement_channel(None, Some(50)), Some(50));
    }

    #[test]
    fn a_time_reads_the_way_a_track_length_is_written() {
        let at = |spec| parse_timestamp(spec).unwrap();
        assert_eq!(at("90"), Duration::from_secs(90));
        assert_eq!(at("2:30"), Duration::from_secs(150));
        assert_eq!(at("1:02:03"), Duration::from_secs(3_723));
        assert_eq!(at(" 2:30 "), Duration::from_secs(150));
        // The leading field carries the overflow, so a long track can be
        // seeked into by seconds alone.
        assert_eq!(at("5000"), Duration::from_secs(5_000));
        assert_eq!(at("90:00"), Duration::from_secs(5_400));

        for bad in ["", ":", "2:", ":30", "1:90", "1:2:3:4", "two", "-5", "1:60"] {
            assert!(parse_timestamp(bad).is_err(), "accepted {bad:?}");
        }
    }

    #[test]
    fn a_vote_needs_half_the_room_and_never_fewer_than_two() {
        // Alone, there is nobody to agree with.
        assert_eq!(votes_needed(0), 1);
        assert_eq!(votes_needed(1), 1);
        // With one other person, half would be one — which is no vote at all.
        assert_eq!(votes_needed(2), 2);
        assert_eq!(votes_needed(3), 2);
        assert_eq!(votes_needed(4), 2);
        assert_eq!(votes_needed(5), 3);
        assert_eq!(votes_needed(9), 5);
    }

    #[test]
    fn a_run_of_positions_reads_the_way_the_queue_prints_them() {
        assert_eq!(parse_track_range("3").unwrap(), (3, 3));
        assert_eq!(parse_track_range("3-7").unwrap(), (3, 7));
        assert_eq!(parse_track_range(" 3 - 7 ").unwrap(), (3, 7));
        // Both ends are inclusive, so a single position is a run of one.
        assert_eq!(parse_track_range("7-7").unwrap(), (7, 7));

        for bad in ["0", "0-3", "7-3", "", "three", "3-", "-3", "3-4-5"] {
            assert!(parse_track_range(bad).is_err(), "accepted {bad:?}");
        }
    }

    #[test]
    fn every_command_is_offered_only_inside_a_server() {
        // Auxide has a queue, a voice channel, and a room per server. A command
        // offered in a direct message could only ever be refused, so none of
        // them should be offered there — and a command added later must not be
        // able to slip through without this.
        for command in command_definitions() {
            let payload = serde_json::to_value(&command).expect("a command serialises");
            let name = payload["name"].as_str().unwrap_or_default().to_owned();
            assert_eq!(
                payload
                    .get("dm_permission")
                    .and_then(serde_json::Value::as_bool),
                Some(false),
                "/{name} is offered in direct messages"
            );
        }
    }

    #[test]
    fn nothing_is_narrowed_before_a_server_has_narrowed_it() {
        // Deciding here who may run what would take the decision away from the
        // server administrators who have Discord's editor, its audit log, and
        // the context to use both.
        for command in command_definitions() {
            let payload = serde_json::to_value(&command).expect("a command serialises");
            let name = payload["name"].as_str().unwrap_or_default().to_owned();
            assert!(
                payload.get("default_member_permissions").is_none(),
                "/{name} decides who may run it, instead of leaving that to the server"
            );
        }
    }

    #[test]
    fn help_describes_every_command_that_is_registered() {
        let text = help_text();
        for command in command_definitions() {
            let (name, description) =
                described(&command).expect("a registered command has a name and a description");
            assert!(
                text.contains(&format!("`/{name}` — {description}")),
                "/{name} is registered but missing from help"
            );
        }
        // Including itself, which is the command somebody runs to find the
        // others.
        assert!(text.contains("`/help`"));
    }

    #[test]
    fn a_withdrawn_command_is_offered_nowhere() {
        // Discord's picker, `/help`, and the dispatcher are three different
        // places a command can be visible, and a command that is off has to be
        // off in all of them: one that help still lists is a promise the bot
        // will refuse, and one Discord still offers is the crash it was
        // withdrawn for.
        let text = help_text();
        let registered: Vec<String> = command_definitions()
            .iter()
            .filter_map(described)
            .map(|(name, _)| name)
            .collect();
        for name in DISABLED_COMMANDS {
            assert!(
                !registered.iter().any(|listed| listed == name),
                "/{name} is disabled but still registered"
            );
            assert!(!text.contains(&format!("`/{name}`")), "/{name} is in help");
        }
    }

    #[test]
    fn what_is_still_offered_did_not_go_with_them() {
        // The withdrawal is four commands, not the group they sat in. `/pause`
        // and `/resume` are in the same help heading and are the ones people
        // actually use.
        let registered: Vec<String> = command_definitions()
            .iter()
            .filter_map(described)
            .map(|(name, _)| name)
            .collect();
        for name in [
            "play",
            "pause",
            "resume",
            "skip",
            "now-playing",
            "queue",
            "help",
        ] {
            assert!(
                registered.iter().any(|listed| listed == name),
                "/{name} went missing"
            );
        }
    }

    #[test]
    fn help_stays_inside_what_an_embed_will_hold() {
        assert!(bounded_embed(help_text()).chars().count() <= EMBED_DESCRIPTION_CHARS);
        assert_eq!(
            bounded_embed("x".repeat(EMBED_DESCRIPTION_CHARS + 100))
                .chars()
                .count(),
            EMBED_DESCRIPTION_CHARS
        );
    }

    #[test]
    fn a_command_nobody_grouped_still_appears() {
        // Every name in the grouping table has to be a command that exists, or
        // the heading it sits under is describing something imaginary. A
        // withdrawn one is allowed to stay in the table and is skipped when
        // help is rendered, so that re-offering it stays a one-line change
        // rather than a hunt for where its heading used to be.
        let registered = command_definitions()
            .iter()
            .filter_map(described)
            .map(|(name, _)| name)
            .collect::<BTreeSet<_>>();
        for (heading, names) in HELP_GROUPS {
            for name in names {
                assert!(
                    registered.contains(*name) || DISABLED_COMMANDS.contains(name),
                    "help groups /{name} under {heading}, but nothing registers it"
                );
            }
        }
    }

    #[test]
    fn formats_the_idle_hold_in_whole_minutes() {
        assert_eq!(format_hold(Duration::from_secs(900)), "15 minutes");
        assert_eq!(format_hold(Duration::from_secs(60)), "1 minute");
        assert_eq!(format_hold(Duration::from_secs(30)), "under a minute");
    }

    #[test]
    fn only_the_room_facing_commands_answer_the_room() {
        assert_eq!(
            audience_for("play", Some("https://www.youtube.com/watch?v=id")),
            Audience::Channel
        );
        // The picker has to stay private, or every search would paste five
        // candidates into the channel and only one of them gets played.
        assert_eq!(
            audience_for("play", Some("artist and track")),
            Audience::Requester
        );
        assert_eq!(audience_for("skip", None), Audience::Channel);
        assert_eq!(audience_for("stop", None), Audience::Channel);
        assert_eq!(audience_for("shuffle", None), Audience::Channel);
        assert_eq!(audience_for("pause", None), Audience::Channel);
        assert_eq!(audience_for("resume", None), Audience::Channel);
        // Setting the level changes what everyone hears; asking what it is only
        // answers the person who asked.
        assert_eq!(audience_for("volume", Some("30")), Audience::Channel);
        assert_eq!(audience_for("volume", None), Audience::Requester);
        assert_eq!(audience_for("queue", None), Audience::Requester);
        assert_eq!(audience_for("now-playing", None), Audience::Requester);
    }

    #[test]
    fn a_percentage_becomes_the_scale_songbird_takes() {
        assert!((volume_scale(100) - 1.0).abs() < f32::EPSILON);
        assert!((volume_scale(30) - 0.3).abs() < f32::EPSILON);
        // Nothing should reach this clamped, but silence and amplification are
        // both worse failures than the nearest level that was asked for.
        assert!((volume_scale(0) - 0.01).abs() < f32::EPSILON);
        assert!((volume_scale(500) - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn a_held_track_does_not_read_as_playing() {
        assert_eq!(playback_heading(false), "Now playing");
        assert_eq!(playback_heading(true), "Paused");
    }

    fn occupant(user_id: u64, channel_id: Option<u64>, is_bot: bool) -> VoiceOccupant {
        VoiceOccupant {
            user_id,
            channel_id,
            is_bot,
        }
    }

    #[test]
    fn occupancy_counts_only_the_people_who_could_be_listening() {
        let bot = 1;
        let alone = [occupant(bot, Some(10), true)];
        assert_eq!(
            occupancy(&alone, bot),
            Some(Occupancy {
                channel_id: 10,
                listeners: 0
            })
        );

        let attended = [occupant(bot, Some(10), true), occupant(2, Some(10), false)];
        assert_eq!(occupancy(&attended, bot).unwrap().listeners, 1);

        // Someone listening somewhere else in the same server is not listening
        // here, and other music bots are not an audience either.
        let elsewhere = [
            occupant(bot, Some(10), true),
            occupant(2, Some(11), false),
            occupant(3, Some(10), true),
        ];
        assert_eq!(occupancy(&elsewhere, bot).unwrap().listeners, 0);
    }

    #[test]
    fn player_errors_remain_safe_for_user_responses() {
        let error = anyhow!(PlayerError::QueueFull { max_tracks: 100 });
        assert_eq!(
            error.to_string(),
            "guild queue has reached its 100-track limit"
        );
    }
}
