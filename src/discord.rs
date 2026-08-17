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
    Client, async_trait,
    builder::{
        CreateActionRow, CreateAllowedMentions, CreateAutocompleteResponse, CreateButton,
        CreateCommand, CreateCommandOption, CreateEmbed, CreateEmbedAuthor, CreateEmbedFooter,
        CreateInteractionResponse, CreateInteractionResponseFollowup,
        CreateInteractionResponseMessage, CreateMessage, EditInteractionResponse,
    },
    client::{Context, EventHandler},
    gateway::{ConnectionStage, ShardStageUpdateEvent},
    http::Http,
    model::application::{
        Command, CommandDataOptionValue, CommandInteraction, CommandOptionType,
        ComponentInteraction, Interaction,
    },
    model::gateway::Ready,
    model::id::{ChannelId, GuildId, UserId},
    model::voice::VoiceState,
};
use songbird::{
    Event, EventContext, EventHandler as VoiceEventHandler, SerenityInit, Songbird, TrackEvent,
    tracks::{PlayMode, TrackHandle},
};
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
        GuildPlayerHandle, PlaybackDirective, PlayerSnapshot, PlayerTransition, QueueItem,
        spawn_guild_player,
    },
    source::{SourceResolver, TrackMetadata, YouTubeResolver},
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
    let config = Arc::new(config);
    let token = config
        .load_discord_token()
        .context("failed to load the Discord token")?;

    // Check before opening a listener or a gateway, so an application that
    // anyone could add never reaches the point of accepting commands.
    let application = Http::new(token.expose_secret())
        .get_current_application_info()
        .await
        .context("failed to identify the Discord application behind this token")?;
    ensure_installation_is_a_boundary(&config, application.bot_public)?;
    tracing::info!(
        application = %application.name,
        allow_all_guilds = config.discord.allow_all_guilds,
        configured_guilds = config.discord.guilds.len(),
        max_guilds = config.discord.max_guilds,
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
    let runtime = Arc::new(BotRuntime::new(
        Arc::clone(&config),
        Arc::clone(&voice),
        observability.clone(),
        cancellation.child_token(),
    )?);
    let handler = DiscordHandler {
        runtime: Arc::clone(&runtime),
    };
    let intents = serenity::model::gateway::GatewayIntents::GUILDS
        | serenity::model::gateway::GatewayIntents::GUILD_VOICE_STATES;
    let mut client = Client::builder(token.expose_secret(), intents)
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

fn command_definitions() -> Vec<CreateCommand> {
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
                .min_length(1)
                .max_length(200),
            ),
        CreateCommand::new("queue").description("Show the current queue"),
        CreateCommand::new("skip").description("Skip the current track"),
        CreateCommand::new("stop").description("Clear the queue and disconnect"),
        CreateCommand::new("shuffle").description("Shuffle tracks waiting in the queue"),
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
    resolver: Arc<YouTubeResolver>,
    pipeline: AudioPipeline,
    /// Guild players, created when a server first uses Auxide.
    ///
    /// These used to be built up front from the configured guild list. Without
    /// such a list there is nothing to enumerate at startup, and creating an
    /// actor, a worker, and a queue for every server the bot merely sits in
    /// would pay for servers that never issue a command.
    sessions: Mutex<HashMap<u64, GuildSession>>,
    pending_searches: Mutex<HashMap<Uuid, PendingSearch>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    voice: Arc<Songbird>,
    observability: ObservabilityState,
    cancellation: CancellationToken,
}

#[derive(Clone)]
struct GuildSession {
    player: GuildPlayerHandle,
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
            components: Vec::new(),
            announcement: None,
        }
    }

    fn from(announcement: Announcement) -> Self {
        Self {
            content: announcement.content,
            embeds: announcement.embeds,
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
        voice: Arc<Songbird>,
        observability: ObservabilityState,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        let resolver = Arc::new(YouTubeResolver::new(
            config.youtube.clone(),
            &config.playback,
        ));
        let source: Arc<dyn SourceResolver> = resolver.clone();
        let pipeline = AudioPipeline::new(source, config.playback.output_volume)?;
        observability.set_guild_players(0);

        Ok(Self {
            config,
            resolver,
            pipeline,
            sessions: Mutex::new(HashMap::new()),
            pending_searches: Mutex::new(HashMap::new()),
            tasks: Mutex::new(Vec::new()),
            voice,
            observability,
            cancellation,
        })
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
        let worker_task = tokio::spawn(playback_worker(
            guild_id,
            player.clone(),
            transitions,
            Arc::clone(&self.voice),
            self.pipeline.clone(),
            self.announcer(http),
            self.observability.clone(),
            self.cancellation.child_token(),
        ));
        {
            let mut tasks = self.tasks.lock().await;
            tasks.push(actor_task);
            tasks.push(worker_task);
        }

        let session = GuildSession { player };
        sessions.insert(guild_id, session.clone());
        self.observability
            .set_guild_players(sessions.len().try_into().unwrap_or(u64::MAX));
        tracing::info!(guild_id, servers = sessions.len(), "started a guild player");
        Ok(session)
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
        let Some(channel_id) = abandoned_voice_channel(ctx, guild_id) else {
            return;
        };

        tracing::info!(
            guild_id = guild_id.get(),
            channel_id = channel_id.get(),
            "left an empty voice channel"
        );
        // Read the origin channel before stopping, because stopping is what
        // ends the session that knows it.
        let origin = session
            .player
            .snapshot()
            .await
            .ok()
            .and_then(|snapshot| snapshot.text_channel_id);
        if let Err(error) = session.player.stop().await {
            tracing::debug!(%error, guild_id = guild_id.get(), "guild player already stopped");
        }
        self.announcer(&ctx.http)
            .announce(
                guild_id.get(),
                origin,
                Announcement::text(
                    "Everyone left the voice channel, so I stopped playing and disconnected.",
                ),
            )
            .await;
    }

    async fn handle_interaction(&self, ctx: &Context, interaction: Interaction) {
        match interaction {
            Interaction::Command(command) => self.handle_command(ctx, &command).await,
            Interaction::Component(component) => self.handle_component(ctx, &component).await,
            Interaction::Autocomplete(autocomplete) => {
                let result = autocomplete
                    .create_response(
                        &ctx.http,
                        CreateInteractionResponse::Autocomplete(CreateAutocompleteResponse::new()),
                    )
                    .await;
                self.observability.record_interaction(result.is_ok());
                if let Err(error) = result {
                    tracing::warn!(%error, "failed to acknowledge autocomplete interaction");
                }
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
        let edit = EditInteractionResponse::new()
            .content(reply.content)
            .embeds(reply.embeds)
            .components(reply.components)
            .allowed_mentions(no_mentions());
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
        let authorization = self.authorize_command(ctx, command).await?;
        match command.data.name.as_str() {
            "play" => self.play(ctx, command, authorization).await,
            "queue" => self.queue(authorization).await,
            "skip" => self.skip(ctx, authorization).await,
            "stop" => self.stop(ctx, authorization).await,
            "shuffle" => self.shuffle(ctx, authorization).await,
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
            if !guild.command_channel_ids.is_empty()
                && !guild.command_channel_ids.contains(&response_channel_id)
            {
                bail!("commands are not enabled in this channel");
            }
            if !identity_is_authorized(guild, user_id.get(), role_ids) {
                bail!("you are not authorized to control Auxide");
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

    async fn queue(&self, authorization: Authorization) -> Result<InteractionReply> {
        let snapshot = authorization.session.player.snapshot().await?;
        let Some(description) = format_queue(&snapshot) else {
            return Ok(InteractionReply::message("The queue is empty."));
        };
        // An embed description holds twice what a message does, which is the
        // difference between listing twenty of a hundred queued tracks and
        // listing most of them.
        Ok(InteractionReply::from(Announcement::card(
            CreateEmbed::new()
                .author(CreateEmbedAuthor::new("Queue"))
                .description(description)
                .colour(EMBED_COLOUR),
        )))
    }

    async fn now_playing(&self, authorization: Authorization) -> Result<InteractionReply> {
        let snapshot = authorization.session.player.snapshot().await?;
        let Some(item) = snapshot.current else {
            return Ok(InteractionReply::message("Nothing is playing."));
        };
        Ok(InteractionReply::from(Announcement::card(track_embed(
            playback_heading(snapshot.paused),
            &item.track,
            item.requested_by,
            None,
        ))))
    }

    async fn skip(&self, ctx: &Context, authorization: Authorization) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        let outcome = authorization.session.player.skip().await?;
        // Nothing changed, so this is the requester's mistake to hear about and
        // not an event worth announcing to the channel.
        let skipped = outcome.skipped.context("nothing is playing to skip")?;
        let requester = mention(authorization.user_id);
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

    async fn set_paused(
        &self,
        ctx: &Context,
        authorization: Authorization,
        paused: bool,
    ) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        let transition = authorization.session.player.set_paused(paused).await?;
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

    async fn stop(&self, ctx: &Context, authorization: Authorization) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        authorization.session.player.stop().await?;
        Ok(InteractionReply::message(format!(
            "{} stopped playback, cleared the queue, and disconnected.",
            mention(authorization.user_id)
        )))
    }

    async fn shuffle(
        &self,
        ctx: &Context,
        authorization: Authorization,
    ) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        let transition = authorization.session.player.shuffle().await?;
        Ok(InteractionReply::message(format!(
            "{} shuffled {} waiting track(s).",
            mention(authorization.user_id),
            transition.snapshot.pending.len()
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
        for guild_id in sessions.keys().copied() {
            if let Err(error) = self.voice.remove(GuildId::new(guild_id)).await {
                tracing::debug!(%error, guild_id, "voice session was already absent during shutdown");
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
    // Both commands that take an argument are told apart by having one, so the
    // presence of any argument is all this needs to see.
    let argument = query_option(command).or_else(|| volume_option(command).map(|_| "level"));
    audience_for(&command.data.name, argument)
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
        "volume" if argument.is_some() => Audience::Channel,
        "skip" | "stop" | "shuffle" | "pause" | "resume" => Audience::Channel,
        _ => Audience::Requester,
    }
}

fn query_option(command: &CommandInteraction) -> Option<&str> {
    command
        .data
        .options
        .iter()
        .find(|option| option.name == "query")
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

/// Returns the voice channel Auxide should leave because nobody is left in it.
///
/// The cache is authoritative here rather than the player's own record of the
/// channel, so an administrator dragging Auxide elsewhere moves the question to
/// the channel it actually sits in. Returning `None` while a join is still in
/// flight is what keeps that race from reading as an empty channel.
fn abandoned_voice_channel(ctx: &Context, guild_id: GuildId) -> Option<ChannelId> {
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
    abandoned_channel(&occupants, bot_user_id).map(ChannelId::new)
}

/// Returns the channel Auxide occupies when no listener is left in it.
fn abandoned_channel(occupants: &[VoiceOccupant], bot_user_id: u64) -> Option<u64> {
    let channel_id = occupants
        .iter()
        .find(|occupant| occupant.user_id == bot_user_id)
        .and_then(|occupant| occupant.channel_id)?;
    let listening = occupants.iter().any(|occupant| {
        occupant.channel_id == Some(channel_id)
            && occupant.user_id != bot_user_id
            && !occupant.is_bot
    });
    (!listening).then_some(channel_id)
}

#[allow(clippy::too_many_arguments)]
async fn playback_worker(
    guild_id: u64,
    player: GuildPlayerHandle,
    mut transitions: mpsc::Receiver<PlayerTransition>,
    voice: Arc<Songbird>,
    pipeline: AudioPipeline,
    announcer: Announcer,
    observability: ObservabilityState,
    cancellation: CancellationToken,
) {
    let mut active: Option<TrackHandle> = None;
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
            PlaybackDirective::Stop => {
                if let Some(handle) = active.take() {
                    let _ = handle.stop();
                }
            }
            directive @ (PlaybackDirective::Pause
            | PlaybackDirective::Resume
            | PlaybackDirective::SetVolume) => {
                adjust_track(guild_id, active.as_ref(), &directive, volume);
            }
            PlaybackDirective::StopAndDisconnect | PlaybackDirective::Disconnect => {
                if let Some(handle) = active.take() {
                    let _ = handle.stop();
                }
                remove_voice(&voice, guild_id).await;
            }
            PlaybackDirective::IdleDisconnect => {
                if let Some(handle) = active.take() {
                    let _ = handle.stop();
                }
                remove_voice(&voice, guild_id).await;
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
                if let Some(handle) = active.take() {
                    let _ = handle.stop();
                }
                let Some(channel_id) = transition.snapshot.voice_channel_id else {
                    tracing::warn!(guild_id, queue_id = %item.queue_id, "play directive had no voice channel");
                    continue;
                };
                match start_track(
                    guild_id,
                    channel_id,
                    &player,
                    &mut transitions,
                    &voice,
                    &pipeline,
                    &announcer,
                    &observability,
                    &cancellation,
                    item,
                    volume,
                )
                .await
                {
                    StartTrackOutcome::Active(handle) => active = Some(handle),
                    StartTrackOutcome::Superseded(next) => deferred = Some(*next),
                    StartTrackOutcome::Failed => {}
                    StartTrackOutcome::Shutdown => break,
                }
            }
        }
    }

    if let Some(handle) = active {
        let _ = handle.stop();
    }
    remove_voice(&voice, guild_id).await;
}

enum StartTrackOutcome {
    Active(TrackHandle),
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
    voice: &Songbird,
    pipeline: &AudioPipeline,
    announcer: &Announcer,
    observability: &ObservabilityState,
    cancellation: &CancellationToken,
    item: QueueItem,
    volume: f32,
) -> StartTrackOutcome {
    let preparation = pipeline.prepare(&item.track);
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
    let input = match prepared {
        Ok(input) => input,
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

    let call = match voice
        .join(GuildId::new(guild_id), ChannelId::new(channel_id))
        .await
    {
        Ok(call) => call,
        Err(error) => {
            tracing::warn!(%error, guild_id, channel_id, "failed to join voice; advancing");
            let _ = player.track_finished(item.queue_id).await;
            return StartTrackOutcome::Failed;
        }
    };
    let handle = call.lock().await.play_only_input(input);
    if let Err(error) = handle.set_volume(volume) {
        tracing::warn!(%error, guild_id, "failed to set playback volume");
    }
    let event = RuntimeTrackFinished {
        player: player.clone(),
        guild_id,
        queue_id: item.queue_id,
    };
    if let Err(error) = handle.add_event(Event::Track(TrackEvent::End), event) {
        tracing::warn!(%error, guild_id, "failed to subscribe to track completion");
        let _ = handle.stop();
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
    StartTrackOutcome::Active(handle)
}

/// Applies a session-level change to the track that is already playing.
///
/// Doing nothing is the right answer when there is no track: the actor refuses
/// to hold a session with nothing in it, and a level set while the queue is
/// empty rides the snapshot to whatever starts next.
fn adjust_track(
    guild_id: u64,
    active: Option<&TrackHandle>,
    directive: &PlaybackDirective,
    volume: f32,
) {
    let Some(handle) = active else {
        return;
    };
    let adjusted = match directive {
        PlaybackDirective::Pause => handle.pause(),
        PlaybackDirective::Resume => handle.play(),
        PlaybackDirective::SetVolume => handle.set_volume(volume),
        _ => return,
    };
    if let Err(error) = adjusted {
        tracing::warn!(%error, guild_id, ?directive, "failed to adjust the current track");
    }
}

async fn remove_voice(voice: &Songbird, guild_id: u64) {
    if let Err(error) = voice.remove(GuildId::new(guild_id)).await {
        tracing::debug!(%error, guild_id, "voice session was already absent");
    }
}

struct RuntimeTrackFinished {
    player: GuildPlayerHandle,
    guild_id: u64,
    queue_id: Uuid,
}

#[async_trait]
impl VoiceEventHandler for RuntimeTrackFinished {
    async fn act(&self, context: &EventContext<'_>) -> Option<Event> {
        if let EventContext::Track([(state, _)]) = context {
            if let PlayMode::Errored(error) = &state.playing {
                tracing::warn!(%error, guild_id = self.guild_id, queue_id = %self.queue_id, "track ended with an error");
            }
        }
        if let Err(error) = self.player.track_finished(self.queue_id).await {
            tracing::debug!(%error, guild_id = self.guild_id, queue_id = %self.queue_id, "track completion arrived after shutdown");
        }
        Some(Event::Cancel)
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
fn format_queue(snapshot: &PlayerSnapshot) -> Option<String> {
    let current = snapshot.current.as_ref()?;
    let mut content = format!(
        "**{}**\n{} ({}) — {}\n",
        playback_heading(snapshot.paused),
        single_line(&current.track.title, 120),
        format_duration(current.track.duration),
        mention(current.requested_by)
    );
    if snapshot.pending.is_empty() {
        content.push_str("\nNo tracks are waiting.");
        return Some(content);
    }
    content.push_str("\n**Up next**\n");
    for (index, item) in snapshot.pending.iter().enumerate() {
        let line = format!(
            "{}. {} ({}) — {}\n",
            index + 1,
            single_line(&item.track.title, 100),
            format_duration(item.track.duration),
            mention(item.requested_by)
        );
        if content.chars().count() + line.chars().count() > EMBED_DESCRIPTION_CHARS {
            let _ = write!(content, "…and {} more.", snapshot.pending.len() - index);
            break;
        }
        content.push_str(&line);
    }
    Some(content)
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
        let rendered = format_queue(&snapshot).unwrap();
        assert!(rendered.chars().count() <= EMBED_DESCRIPTION_CHARS + 100);
        assert!(rendered.contains("…and"), "truncation was not reported");
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
        let rendered = format_queue(&snapshot).unwrap();
        assert!(rendered.contains("**Now playing**\nPlaying (2:00) — <@11>"));
        assert!(rendered.contains("1. Waiting (2:00) — <@22>"));
    }

    #[test]
    fn an_empty_queue_has_nothing_to_render() {
        assert_eq!(format_queue(&PlayerSnapshot::default()), None);
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
    fn a_channel_is_abandoned_once_its_last_listener_leaves() {
        let bot = 1;
        let alone = [occupant(bot, Some(10), true)];
        assert_eq!(abandoned_channel(&alone, bot), Some(10));

        let attended = [occupant(bot, Some(10), true), occupant(2, Some(10), false)];
        assert_eq!(abandoned_channel(&attended, bot), None);

        // Someone listening somewhere else in the same server is not listening
        // here, and other music bots are not an audience either.
        let elsewhere = [
            occupant(bot, Some(10), true),
            occupant(2, Some(11), false),
            occupant(3, Some(10), true),
        ];
        assert_eq!(abandoned_channel(&elsewhere, bot), Some(10));
    }

    #[test]
    fn a_disconnected_bot_has_no_channel_to_abandon() {
        let bot = 1;
        assert_eq!(abandoned_channel(&[], bot), None);
        assert_eq!(
            abandoned_channel(&[occupant(2, Some(10), false)], bot),
            None
        );
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
