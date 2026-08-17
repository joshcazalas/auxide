//! The seam between deciding what to play and actually playing it.
//!
//! [`crate::source::SourceResolver`] already lets a test hand the player a fake
//! `YouTube`. This is the matching boundary on the other side: without it, the
//! only way to exercise the playback worker is to connect to Discord, so the
//! step between a queue deciding something and a voice channel hearing it was
//! the one part of Auxide with no tests at all.
//!
//! One implementation per guild session rather than one shared across servers,
//! because a worker is per guild too — it removes the guild id from every
//! method and gives the implementation somewhere to keep the track it started.

use std::{any::Any, sync::Arc, time::Duration};

use async_trait::async_trait;
use serenity::model::id::{ChannelId, GuildId};
use songbird::{
    Event, EventContext, EventHandler as VoiceEventHandler, Songbird, TrackEvent,
    input::Input,
    tracks::{PlayMode, TrackHandle},
};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{audio::AudioPipeline, source::TrackMetadata};

/// Told once when the track it was given to finishes.
#[async_trait]
pub trait TrackEnded: Send + Sync + 'static {
    async fn ended(&self);
}

/// Everything the playback worker does to a voice channel.
///
/// Preparing is separate from playing because preparing reaches the network and
/// takes seconds, and a skip arriving during it has to be able to supersede the
/// track being prepared. Keeping them apart is what lets the worker race the
/// two against each other.
#[async_trait]
pub trait VoiceGateway: Send + Sync + 'static {
    /// Resolves a track into something playable, without playing it.
    async fn prepare(&self, track: &TrackMetadata) -> Result<Prepared, VoiceError>;

    /// Takes a voice channel without playing anything through it.
    async fn join(&self, channel_id: u64) -> Result<(), VoiceError>;

    /// Joins `channel_id` if needed and plays, replacing anything already playing.
    async fn play(
        &self,
        channel_id: u64,
        prepared: Prepared,
        volume: f32,
        ended: Arc<dyn TrackEnded>,
    ) -> Result<(), VoiceError>;

    async fn pause(&self);
    async fn resume(&self);
    async fn set_volume(&self, volume: f32);

    /// Moves the playhead within the current track.
    ///
    /// Where to go is worked out here rather than by the caller, because only
    /// a gateway knows where the playhead currently is.
    async fn seek(&self, to: SeekTo) -> Result<Duration, VoiceError>;

    /// Stops what is playing and keeps the channel.
    async fn stop(&self);

    /// Stops what is playing and gives up the channel.
    async fn leave(&self);
}

/// A source made ready to play, opaque to everything but the gateway that made it.
///
/// Boxed rather than an associated type so the trait stays object-safe, which
/// is what lets the whole runtime be handed a different gateway without every
/// type between here and `run` becoming generic. The worker only ever passes
/// this straight back to the gateway it came from.
pub struct Prepared(Box<dyn Any + Send>);

impl Prepared {
    #[must_use]
    pub fn new<T: Send + 'static>(value: T) -> Self {
        Self(Box::new(value))
    }

    /// Recovers what [`Prepared::new`] was given.
    ///
    /// # Panics
    ///
    /// Panics if a gateway is handed something another gateway prepared, which
    /// the worker's structure makes impossible: it passes back exactly what the
    /// same gateway just returned.
    #[must_use]
    pub fn take<T: Send + 'static>(self) -> T {
        *self
            .0
            .downcast()
            .expect("a gateway only ever receives what it prepared")
    }
}

impl std::fmt::Debug for Prepared {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Prepared").finish_non_exhaustive()
    }
}

/// Where a seek should land.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SeekTo {
    Position(Duration),
    Forward(Duration),
    Backward(Duration),
    Start,
}

#[derive(Debug, Error)]
pub enum VoiceError {
    /// The source could not be made playable, which is the track's fault.
    ///
    /// Distinct from the rest because it is the only one worth telling a
    /// channel about: a video went private, or is live, or is too long.
    #[error("{0}")]
    Prepare(String),
    #[error("failed to join voice: {0}")]
    Join(String),
    #[error("failed to start playback: {0}")]
    Play(String),
    #[error("nothing is playing")]
    NothingPlaying,
    #[error("this track cannot be seeked in")]
    NotSeekable,
}

/// Makes a gateway for a guild, the first time that guild needs one.
///
/// A session's gateway cannot be built before the session exists, so the
/// runtime is given something that builds them rather than one to share. It is
/// also the seam a test replaces to run the whole bot without touching voice.
pub trait VoiceGatewayFactory: Send + Sync + 'static {
    fn create(&self, guild_id: u64) -> Arc<dyn VoiceGateway>;
}

/// Builds Songbird-backed gateways, one per guild.
pub struct SongbirdVoice {
    songbird: Arc<Songbird>,
    pipeline: AudioPipeline,
}

impl SongbirdVoice {
    #[must_use]
    pub const fn new(songbird: Arc<Songbird>, pipeline: AudioPipeline) -> Self {
        Self { songbird, pipeline }
    }
}

impl VoiceGatewayFactory for SongbirdVoice {
    fn create(&self, guild_id: u64) -> Arc<dyn VoiceGateway> {
        Arc::new(SongbirdGateway::new(
            Arc::clone(&self.songbird),
            self.pipeline.clone(),
            guild_id,
        ))
    }
}

/// The real thing: Songbird, driving one guild's voice connection.
pub struct SongbirdGateway {
    songbird: Arc<Songbird>,
    pipeline: AudioPipeline,
    guild_id: u64,
    /// The track currently playing, if any.
    ///
    /// Held here rather than by the worker so that pausing and setting a level
    /// are things done to a voice channel rather than things the worker has to
    /// keep bookkeeping for.
    active: Mutex<Option<TrackHandle>>,
}

impl SongbirdGateway {
    #[must_use]
    pub fn new(songbird: Arc<Songbird>, pipeline: AudioPipeline, guild_id: u64) -> Self {
        Self {
            songbird,
            pipeline,
            guild_id,
            active: Mutex::new(None),
        }
    }

    /// Applies something to the current track, if there is one.
    ///
    /// Every one of these is a no-op with nothing playing, which is the right
    /// answer: the actor refuses to hold an empty session, and a level set
    /// while the queue is empty is remembered for whatever starts next.
    async fn adjust(
        &self,
        what: &'static str,
        apply: impl Fn(&TrackHandle) -> songbird::error::TrackResult<()>,
    ) {
        let active = self.active.lock().await;
        let Some(handle) = active.as_ref() else {
            return;
        };
        if let Err(error) = apply(handle) {
            tracing::warn!(%error, guild_id = self.guild_id, what, "failed to adjust the current track");
        }
    }
}

#[async_trait]
impl VoiceGateway for SongbirdGateway {
    async fn prepare(&self, track: &TrackMetadata) -> Result<Prepared, VoiceError> {
        self.pipeline
            .prepare(track)
            .await
            .map(Prepared::new::<Input>)
            .map_err(|error| VoiceError::Prepare(error.to_string()))
    }

    async fn join(&self, channel_id: u64) -> Result<(), VoiceError> {
        self.songbird
            .join(GuildId::new(self.guild_id), ChannelId::new(channel_id))
            .await
            .map(|_| ())
            .map_err(|error| VoiceError::Join(error.to_string()))
    }

    async fn play(
        &self,
        channel_id: u64,
        prepared: Prepared,
        volume: f32,
        ended: Arc<dyn TrackEnded>,
    ) -> Result<(), VoiceError> {
        let prepared: Input = prepared.take();
        let call = self
            .songbird
            .join(GuildId::new(self.guild_id), ChannelId::new(channel_id))
            .await
            .map_err(|error| VoiceError::Join(error.to_string()))?;
        let handle = call.lock().await.play_only_input(prepared);
        if let Err(error) = handle.set_volume(volume) {
            tracing::warn!(%error, guild_id = self.guild_id, "failed to set playback volume");
        }
        if let Err(error) = handle.add_event(
            Event::Track(TrackEvent::End),
            EndedEvent {
                guild_id: self.guild_id,
                ended,
            },
        ) {
            let _ = handle.stop();
            return Err(VoiceError::Play(error.to_string()));
        }
        *self.active.lock().await = Some(handle);
        Ok(())
    }

    async fn pause(&self) {
        self.adjust("pause", TrackHandle::pause).await;
    }

    async fn resume(&self) {
        self.adjust("resume", TrackHandle::play).await;
    }

    async fn set_volume(&self, volume: f32) {
        self.adjust("volume", |handle| handle.set_volume(volume))
            .await;
    }

    async fn seek(&self, to: SeekTo) -> Result<Duration, VoiceError> {
        let handle = self
            .active
            .lock()
            .await
            .clone()
            .ok_or(VoiceError::NothingPlaying)?;
        let now = handle
            .get_info()
            .await
            .map_err(|_| VoiceError::NothingPlaying)?
            .position;
        let target = match to {
            SeekTo::Position(at) => at,
            SeekTo::Forward(by) => now.saturating_add(by),
            SeekTo::Backward(by) => now.saturating_sub(by),
            SeekTo::Start => Duration::ZERO,
        };
        // Songbird reports the position it settled on, which need not be the
        // one asked for: a container seeks to a boundary it can resume from.
        handle
            .seek(target)
            .result_async()
            .await
            .map_err(|_| VoiceError::NotSeekable)
    }

    async fn stop(&self) {
        if let Some(handle) = self.active.lock().await.take() {
            let _ = handle.stop();
        }
    }

    async fn leave(&self) {
        self.stop().await;
        if let Err(error) = self.songbird.remove(GuildId::new(self.guild_id)).await {
            tracing::debug!(%error, guild_id = self.guild_id, "voice session was already absent");
        }
    }
}

/// Bridges Songbird's completion event onto [`TrackEnded`].
struct EndedEvent {
    guild_id: u64,
    ended: Arc<dyn TrackEnded>,
}

#[async_trait]
impl VoiceEventHandler for EndedEvent {
    async fn act(&self, context: &EventContext<'_>) -> Option<Event> {
        if let EventContext::Track([(state, _)]) = context {
            if let PlayMode::Errored(error) = &state.playing {
                tracing::warn!(%error, guild_id = self.guild_id, "track ended with an error");
            }
        }
        self.ended.ended().await;
        Some(Event::Cancel)
    }
}

/// A gateway that records what was asked of it instead of doing it.
///
/// Public within the crate rather than private to one test module, because the
/// tests that need it are in `discord`, beside the worker it stands in for.
#[cfg(test)]
pub mod fake {
    // A poisoned lock here means a test panicked while holding it, which the
    // panic already reports. Documenting that on every accessor would say
    // nothing a reader of a test double needs.
    #![allow(clippy::missing_panics_doc)]

    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_trait::async_trait;
    use tokio::sync::Notify;

    use super::{Prepared, TrackEnded, VoiceError, VoiceGateway};
    use crate::source::TrackMetadata;

    /// One thing that happened to a voice channel.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum VoiceAction {
        Joined(u64),
        Played {
            channel_id: u64,
            source_id: String,
            volume_percent: u16,
        },
        Paused,
        Resumed,
        VolumeSet(u16),
        Sought(super::SeekTo),
        Stopped,
        Left,
    }

    #[derive(Default)]
    struct State {
        actions: Vec<VoiceAction>,
        /// Source ids `prepare` should refuse, and why.
        unplayable: Vec<String>,
        /// The end-of-track callback for whatever is playing.
        ended: Option<Arc<dyn TrackEnded>>,
    }

    /// Records the sequence of things done to a voice channel.
    #[derive(Clone, Default)]
    pub struct FakeVoice {
        state: Arc<Mutex<State>>,
        acted: Arc<Notify>,
    }

    impl FakeVoice {
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        /// Makes one source refuse to prepare, as an unplayable track would.
        pub fn refuse(&self, source_id: &str) {
            self.state
                .lock()
                .expect("fake voice state is not poisoned")
                .unplayable
                .push(source_id.to_owned());
        }

        #[must_use]
        pub fn actions(&self) -> Vec<VoiceAction> {
            self.state
                .lock()
                .expect("fake voice state is not poisoned")
                .actions
                .clone()
        }

        /// Ends whatever is playing, as Songbird would at the end of a track.
        pub async fn finish_track(&self) {
            let ended = self
                .state
                .lock()
                .expect("fake voice state is not poisoned")
                .ended
                .take();
            if let Some(ended) = ended {
                ended.ended().await;
            }
        }

        /// Waits until at least `count` actions have been recorded.
        ///
        /// The worker acts on its own task, so a test that asserted straight
        /// after sending a command would be racing it.
        pub async fn settle(&self, count: usize) {
            while self.actions().len() < count {
                self.acted.notified().await;
            }
        }

        fn record(&self, action: VoiceAction) {
            self.state
                .lock()
                .expect("fake voice state is not poisoned")
                .actions
                .push(action);
            self.acted.notify_waiters();
        }
    }

    impl super::VoiceGatewayFactory for FakeVoice {
        fn create(&self, _guild_id: u64) -> Arc<dyn VoiceGateway> {
            Arc::new(self.clone())
        }
    }

    #[async_trait]
    impl VoiceGateway for FakeVoice {
        async fn prepare(&self, track: &TrackMetadata) -> Result<Prepared, VoiceError> {
            if self
                .state
                .lock()
                .expect("fake voice state is not poisoned")
                .unplayable
                .contains(&track.source_id)
            {
                return Err(VoiceError::Prepare(
                    "live streams are not supported".to_owned(),
                ));
            }
            Ok(Prepared::new(track.source_id.clone()))
        }

        async fn join(&self, channel_id: u64) -> Result<(), VoiceError> {
            self.record(VoiceAction::Joined(channel_id));
            Ok(())
        }

        async fn play(
            &self,
            channel_id: u64,
            prepared: Prepared,
            volume: f32,
            ended: Arc<dyn TrackEnded>,
        ) -> Result<(), VoiceError> {
            let source_id: String = prepared.take();
            self.state
                .lock()
                .expect("fake voice state is not poisoned")
                .ended = Some(ended);
            self.record(VoiceAction::Played {
                channel_id,
                source_id,
                volume_percent: percent(volume),
            });
            Ok(())
        }

        async fn pause(&self) {
            self.record(VoiceAction::Paused);
        }

        async fn resume(&self) {
            self.record(VoiceAction::Resumed);
        }

        async fn set_volume(&self, volume: f32) {
            self.record(VoiceAction::VolumeSet(percent(volume)));
        }

        async fn seek(&self, to: super::SeekTo) -> Result<Duration, VoiceError> {
            self.record(VoiceAction::Sought(to));
            Ok(Duration::ZERO)
        }

        async fn stop(&self) {
            self.state
                .lock()
                .expect("fake voice state is not poisoned")
                .ended = None;
            self.record(VoiceAction::Stopped);
        }

        async fn leave(&self) {
            self.state
                .lock()
                .expect("fake voice state is not poisoned")
                .ended = None;
            self.record(VoiceAction::Left);
        }
    }

    /// Back to whole percent, so assertions read in the units commands use.
    fn percent(volume: f32) -> u16 {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let percent = (volume * 100.0).round() as u16;
        percent
    }
}
