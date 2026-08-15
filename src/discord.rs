use std::{
    collections::HashMap,
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
        CreateCommand, CreateCommandOption, CreateInteractionResponse,
        CreateInteractionResponseMessage, EditInteractionResponse,
    },
    client::{Context, EventHandler},
    gateway::{ConnectionStage, ShardStageUpdateEvent},
    http::Http,
    model::application::{
        CommandDataOptionValue, CommandInteraction, CommandOptionType, ComponentInteraction,
        Interaction,
    },
    model::gateway::Ready,
    model::id::{ChannelId, GuildId, UserId},
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
    config::{Config, GuildConfig},
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

/// Registers the complete Auxide command set in configured guilds.
///
/// Registration is deliberately separate from normal startup. It overwrites only this
/// application's commands in each selected guild and never creates channels or roles.
///
/// # Errors
///
/// Returns an error when the token cannot be loaded, a requested guild is not allowlisted, or
/// Discord rejects a command definition.
pub async fn register_commands(config: &Config, selected_guild: Option<u64>) -> Result<()> {
    let token = config
        .load_discord_token()
        .context("failed to load the Discord token")?;
    let http = Http::new(token.expose_secret());
    let guilds = if let Some(guild_id) = selected_guild {
        if config.guild(guild_id).is_none() {
            bail!("guild {guild_id} is not in the configuration allowlist");
        }
        vec![guild_id]
    } else {
        config
            .discord
            .guilds
            .iter()
            .map(|guild| guild.guild_id)
            .collect()
    };

    // Every command route is built from the application id, and `Http::new`
    // leaves it unset. A normal run receives it on the gateway's Ready event,
    // but registration deliberately never opens a gateway, so resolve the
    // identity behind this token directly. Doing it after the guild check keeps
    // a configuration mistake from costing an API call.
    let application = http
        .get_current_application_info()
        .await
        .context("failed to identify the Discord application behind this token")?;
    http.set_application_id(application.id);

    for guild_id in guilds {
        let registered = GuildId::new(guild_id)
            .set_commands(&http, command_definitions())
            .await
            .with_context(|| format!("failed to register commands in guild {guild_id}"))?;
        tracing::info!(
            guild_id,
            commands = registered.len(),
            "registered guild commands"
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
}

struct BotRuntime {
    config: Arc<Config>,
    resolver: Arc<YouTubeResolver>,
    sessions: HashMap<u64, GuildSession>,
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

#[derive(Clone, Debug)]
struct PendingSearch {
    created_at: Instant,
    guild_id: u64,
    response_channel_id: u64,
    voice_channel_id: u64,
    user_id: u64,
    tracks: Vec<TrackMetadata>,
}

#[derive(Debug)]
struct InteractionReply {
    content: String,
    components: Vec<CreateActionRow>,
}

impl InteractionReply {
    fn message(content: impl Into<String>) -> Self {
        Self {
            content: bounded_message(content.into()),
            components: Vec::new(),
        }
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
        let pipeline = AudioPipeline::new(source)?;
        let mut sessions = HashMap::with_capacity(config.discord.guilds.len());
        let mut tasks = Vec::with_capacity(config.discord.guilds.len() * 2);

        for guild in &config.discord.guilds {
            let (player, transitions, actor_task) = spawn_guild_player(
                guild.guild_id,
                config.playback.max_queue_length,
                config.playback.actor_mailbox_capacity,
                random(),
                Duration::from_secs(config.playback.idle_timeout_seconds),
            );
            tasks.push(actor_task);
            tasks.push(tokio::spawn(playback_worker(
                guild.guild_id,
                player.clone(),
                transitions,
                Arc::clone(&voice),
                pipeline.clone(),
                observability.clone(),
                cancellation.child_token(),
            )));
            sessions.insert(guild.guild_id, GuildSession { player });
        }
        observability.set_guild_players(sessions.len().try_into().unwrap_or(u64::MAX));

        Ok(Self {
            config,
            resolver,
            sessions,
            pending_searches: Mutex::new(HashMap::new()),
            tasks: Mutex::new(tasks),
            voice,
            observability,
            cancellation,
        })
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
        if let Err(error) = command.defer_ephemeral(&ctx.http).await {
            self.observability.record_interaction(false);
            tracing::warn!(%error, command = %command.data.name, "failed to defer interaction");
            return;
        }

        let result = self.dispatch_command(ctx, command).await;
        let succeeded = result.is_ok();
        let reply = result.unwrap_or_else(|error| {
            tracing::warn!(%error, command = %command.data.name, "command failed");
            InteractionReply::message(format!("Unable to complete that command: {error}"))
        });
        let edit = EditInteractionResponse::new()
            .content(reply.content)
            .components(reply.components)
            .allowed_mentions(no_mentions());
        let response = command.edit_response(&ctx.http, edit).await;
        self.observability
            .record_interaction(succeeded && response.is_ok());
        if let Err(error) = response {
            tracing::warn!(%error, command = %command.data.name, "failed to edit interaction response");
        }
    }

    async fn dispatch_command(
        &self,
        ctx: &Context,
        command: &CommandInteraction,
    ) -> Result<InteractionReply> {
        let authorization = self.authorize_command(ctx, command)?;
        match command.data.name.as_str() {
            "play" => self.play(ctx, command, authorization).await,
            "queue" => self.queue(authorization).await,
            "skip" => self.skip(ctx, authorization).await,
            "stop" => self.stop(ctx, authorization).await,
            "shuffle" => self.shuffle(ctx, authorization).await,
            "now-playing" => self.now_playing(authorization).await,
            other => bail!("unsupported command {other:?}"),
        }
    }

    fn authorize_command(
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
    }

    fn authorize_component(
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
    }

    fn authorize(
        &self,
        ctx: &Context,
        guild_id: u64,
        response_channel_id: u64,
        user_id: UserId,
        role_ids: &[u64],
    ) -> Result<Authorization> {
        let guild = self
            .config
            .guild(guild_id)
            .with_context(|| format!("server {guild_id} is not allowlisted"))?;
        if !guild.command_channel_ids.is_empty()
            && !guild.command_channel_ids.contains(&response_channel_id)
        {
            bail!("commands are not enabled in this channel");
        }
        if !identity_is_authorized(guild, user_id.get(), role_ids) {
            bail!("you are not authorized to control Auxide");
        }
        let session = self
            .sessions
            .get(&guild_id)
            .context("the server player is unavailable")?
            .clone();
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
        let query = command
            .data
            .options
            .iter()
            .find(|option| option.name == "query")
            .and_then(|option| match &option.value {
                CommandDataOptionValue::String(value) => Some(value.as_str()),
                _ => None,
            })
            .context("the required query option was missing")?;

        if let Ok(url) = Url::parse(query) {
            let result = self.resolver.inspect(&url).await;
            self.observability.record_source_resolution(result.is_ok());
            let track = result?;
            return self
                .enqueue_track(authorization, voice_channel_id, track)
                .await;
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
            components: vec![CreateActionRow::Buttons(buttons)],
        })
    }

    async fn enqueue_track(
        &self,
        authorization: Authorization,
        voice_channel_id: u64,
        track: TrackMetadata,
    ) -> Result<InteractionReply> {
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
        let position = transition
            .snapshot
            .pending
            .iter()
            .position(|queued| queued.queue_id == queue_id)
            .map_or(1, |index| index + 2);
        Ok(InteractionReply::message(format!(
            "Queued **{}** at position {position}.",
            single_line(&track.title, 150)
        )))
    }

    async fn queue(&self, authorization: Authorization) -> Result<InteractionReply> {
        let snapshot = authorization.session.player.snapshot().await?;
        Ok(InteractionReply::message(format_queue(&snapshot)))
    }

    async fn now_playing(&self, authorization: Authorization) -> Result<InteractionReply> {
        let snapshot = authorization.session.player.snapshot().await?;
        let content = snapshot.current.as_ref().map_or_else(
            || "Nothing is playing.".to_owned(),
            |item| {
                format!(
                    "Now playing **{}** ({})\n{}",
                    single_line(&item.track.title, 150),
                    format_duration(item.track.duration),
                    item.track.canonical_url
                )
            },
        );
        Ok(InteractionReply::message(content))
    }

    async fn skip(&self, ctx: &Context, authorization: Authorization) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        let transition = authorization.session.player.skip().await?;
        let content = transition.snapshot.current.as_ref().map_or_else(
            || "Skipped the final track and disconnected.".to_owned(),
            |item| {
                format!(
                    "Skipped. Next: **{}**.",
                    single_line(&item.track.title, 150)
                )
            },
        );
        Ok(InteractionReply::message(content))
    }

    async fn stop(&self, ctx: &Context, authorization: Authorization) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        authorization.session.player.stop().await?;
        Ok(InteractionReply::message(
            "Stopped playback, cleared the queue, and disconnected.",
        ))
    }

    async fn shuffle(
        &self,
        ctx: &Context,
        authorization: Authorization,
    ) -> Result<InteractionReply> {
        self.require_same_voice(ctx, &authorization).await?;
        let transition = authorization.session.player.shuffle().await?;
        Ok(InteractionReply::message(format!(
            "Shuffled {} waiting track(s).",
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
        let response = component
            .create_response(
                &ctx.http,
                CreateInteractionResponse::UpdateMessage(
                    update_message(reply.content).components(reply.components),
                ),
            )
            .await;
        self.observability
            .record_interaction(succeeded && response.is_ok());
        if let Err(error) = response {
            tracing::warn!(%error, "failed to acknowledge component interaction");
        }
    }

    async fn dispatch_component(
        &self,
        ctx: &Context,
        component: &ComponentInteraction,
    ) -> Result<InteractionReply> {
        let authorization = self.authorize_component(ctx, component)?;
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
        self.enqueue_track(authorization, pending.voice_channel_id, track)
            .await
    }

    async fn shutdown(&self) {
        for session in self.sessions.values() {
            if let Err(error) = session.player.shutdown().await {
                tracing::debug!(%error, guild_id = session.player.guild_id(), "guild actor already stopped");
            }
        }
        for guild_id in self.sessions.keys().copied() {
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

async fn playback_worker(
    guild_id: u64,
    player: GuildPlayerHandle,
    mut transitions: mpsc::Receiver<PlayerTransition>,
    voice: Arc<Songbird>,
    pipeline: AudioPipeline,
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

        match transition.directive {
            PlaybackDirective::None => {}
            PlaybackDirective::StopAndDisconnect | PlaybackDirective::Disconnect => {
                if let Some(handle) = active.take() {
                    let _ = handle.stop();
                }
                remove_voice(&voice, guild_id).await;
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
                    &observability,
                    &cancellation,
                    item,
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
    observability: &ObservabilityState,
    cancellation: &CancellationToken,
    item: QueueItem,
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
    StartTrackOutcome::Active(handle)
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

fn format_queue(snapshot: &PlayerSnapshot) -> String {
    let Some(current) = &snapshot.current else {
        return "The queue is empty.".to_owned();
    };
    let mut content = format!(
        "Now: **{}** ({})\n",
        single_line(&current.track.title, 120),
        format_duration(current.track.duration)
    );
    if snapshot.pending.is_empty() {
        content.push_str("No tracks are waiting.");
        return content;
    }
    content.push_str("Up next:\n");
    for (index, item) in snapshot.pending.iter().enumerate() {
        let line = format!(
            "{}. {} ({})\n",
            index + 1,
            single_line(&item.track.title, 100),
            format_duration(item.track.duration)
        );
        if content.chars().count() + line.chars().count() > 1_850 {
            content.push_str("…and more.");
            break;
        }
        content.push_str(&line);
    }
    content
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

    #[test]
    fn queue_rendering_stays_inside_discord_limits() {
        let track = TrackMetadata {
            source_id: "id".to_owned(),
            canonical_url: Url::parse("https://www.youtube.com/watch?v=id").unwrap(),
            title: "x".repeat(500),
            channel: None,
            duration: Duration::from_secs(120),
            thumbnail_url: None,
        };
        let item = QueueItem::new(track, 1, 2);
        let snapshot = PlayerSnapshot {
            current: Some(item.clone()),
            pending: vec![item; 100],
            voice_channel_id: Some(3),
        };
        assert!(format_queue(&snapshot).chars().count() < 2_000);
    }

    #[test]
    fn formats_track_durations() {
        assert_eq!(format_duration(Duration::from_secs(65)), "1:05");
        assert_eq!(format_duration(Duration::from_secs(3_665)), "1:01:05");
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
