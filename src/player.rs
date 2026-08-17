use std::{collections::VecDeque, time::Duration};

use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{self, Instant},
};
use uuid::Uuid;

use crate::{config::MAX_VOLUME_PERCENT, source::TrackMetadata};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueItem {
    pub queue_id: Uuid,
    pub track: TrackMetadata,
    pub requested_by: u64,
    pub response_channel_id: u64,
}

impl QueueItem {
    #[must_use]
    pub fn new(track: TrackMetadata, requested_by: u64, response_channel_id: u64) -> Self {
        Self {
            queue_id: Uuid::new_v4(),
            track,
            requested_by,
            response_channel_id,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlayerSnapshot {
    pub current: Option<QueueItem>,
    pub pending: Vec<QueueItem>,
    pub voice_channel_id: Option<u64>,

    /// Where the most recent request came from.
    ///
    /// Carried on the snapshot rather than read off the current item because
    /// the events that most need somewhere to speak — a queue that ran out, a
    /// channel everyone left — are precisely the ones with no item left to ask.
    pub text_channel_id: Option<u64>,

    pub paused: bool,

    /// Playback level as whole percent of the source's own.
    ///
    /// Percent rather than the configuration's float so that a transition stays
    /// exactly comparable, which is what every test in this module relies on.
    pub volume_percent: u16,
}

impl PlayerSnapshot {
    #[must_use]
    pub fn len(&self) -> usize {
        usize::from(self.current.is_some()) + self.pending.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.current.is_none() && self.pending.is_empty()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum PlaybackDirective {
    #[default]
    None,
    Play(QueueItem),
    Replace(QueueItem),
    /// Stop what is playing but stay in the voice channel.
    ///
    /// Emptying the queue is not a reason to leave. Auxide holds the channel
    /// until the idle timer expires, so the next request joins an already
    /// connected bot instead of waiting through another handshake.
    Stop,
    /// Hold the current track where it is.
    Pause,
    /// Continue a held track.
    Resume,
    /// Apply [`PlayerSnapshot::volume_percent`] to what is playing now.
    ///
    /// The level itself travels on the snapshot, as session state rather than
    /// as a property of one message, so a track starting later plays at it too.
    SetVolume,
    StopAndDisconnect,
    Disconnect,
    /// Leave because the idle hold expired with nothing queued.
    ///
    /// Separate from [`PlaybackDirective::Disconnect`] only so the departure can
    /// be explained. Every other way of leaving was asked for by somebody who
    /// therefore already knows why it happened.
    IdleDisconnect,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlayerTransition {
    pub directive: PlaybackDirective,
    pub snapshot: PlayerSnapshot,
}

/// The result of a [`GuildPlayerHandle::enqueue_all`].
///
/// A bulk addition can be partly refused where a single one can only be
/// accepted or rejected, so the answer has to say how much of it landed.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BulkEnqueue {
    pub accepted: usize,
    /// Items the queue had no room for.
    pub refused: usize,
    pub transition: PlayerTransition,
}

/// The result of a [`GuildPlayerHandle::skip`], carrying what was interrupted.
///
/// The transition alone describes only what plays next, and announcing a skip
/// means naming the track that was cut short, which is gone from the snapshot by
/// the time the caller sees it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SkipOutcome {
    /// The track that was playing, or `None` when nothing was.
    pub skipped: Option<QueueItem>,
    pub transition: PlayerTransition,
}

#[derive(Clone, Debug)]
pub struct GuildPlayerHandle {
    guild_id: u64,
    commands: mpsc::Sender<PlayerCommand>,
}

impl GuildPlayerHandle {
    #[must_use]
    pub const fn guild_id(&self) -> u64 {
        self.guild_id
    }

    /// Adds one item to the bounded guild queue.
    ///
    /// # Errors
    ///
    /// Returns [`PlayerError::QueueFull`] when the configured track limit has been reached, or a
    /// channel error if the actor has stopped.
    pub async fn enqueue(
        &self,
        item: QueueItem,
        voice_channel_id: u64,
    ) -> Result<PlayerTransition, PlayerError> {
        self.request(|reply| PlayerCommand::Enqueue {
            item: Box::new(item),
            voice_channel_id,
            reply,
        })
        .await?
    }

    /// Adds as many items as the queue has room for, in one step.
    ///
    /// A playlist is one request, so it takes one pass through the actor rather
    /// than one per track: nobody else's addition can land in the middle of it,
    /// and it produces one answer rather than fifty.
    ///
    /// # Errors
    ///
    /// Returns [`PlayerError::QueueFull`] only when there was no room for even
    /// one item, [`PlayerError::VoiceChannelConflict`] when a session is already
    /// running elsewhere, or a channel error if the actor has stopped.
    pub async fn enqueue_all(
        &self,
        items: Vec<QueueItem>,
        voice_channel_id: u64,
    ) -> Result<BulkEnqueue, PlayerError> {
        self.request(|reply| PlayerCommand::EnqueueAll {
            items,
            voice_channel_id,
            reply,
        })
        .await?
    }

    /// Removes the current item and advances atomically to the next item, if any.
    ///
    /// Skipping the last track stops playback and starts the idle timer rather
    /// than disconnecting, so the queue can be refilled without rejoining.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has stopped.
    pub async fn skip(&self) -> Result<SkipOutcome, PlayerError> {
        self.request(|reply| PlayerCommand::Skip { reply }).await
    }

    /// Clears the current item and all pending items.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has stopped.
    pub async fn stop(&self) -> Result<PlayerTransition, PlayerError> {
        self.request(|reply| PlayerCommand::Stop { reply }).await
    }

    /// Holds the current track, or continues a held one.
    ///
    /// Holding starts the idle countdown. A held session is playing nothing to
    /// people who may well have wandered off, which is the same situation an
    /// exhausted queue is in, and it should end the same way.
    ///
    /// # Errors
    ///
    /// Returns [`PlayerError::NothingPlaying`] when there is no current track,
    /// [`PlayerError::AlreadyPaused`] or [`PlayerError::NotPaused`] when the
    /// session is already in the requested state, or a channel error if the
    /// actor has stopped.
    pub async fn set_paused(&self, paused: bool) -> Result<PlayerTransition, PlayerError> {
        self.request(|reply| PlayerCommand::SetPaused { paused, reply })
            .await?
    }

    /// Sets the level every track in this session plays at.
    ///
    /// # Errors
    ///
    /// Returns [`PlayerError::VolumeOutOfRange`] outside 1..=100, or a channel
    /// error if the actor has stopped.
    pub async fn set_volume(&self, percent: u16) -> Result<PlayerTransition, PlayerError> {
        self.request(|reply| PlayerCommand::SetVolume { percent, reply })
            .await?
    }

    /// Randomizes only the pending queue using the actor's private random-number generator.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has stopped.
    pub async fn shuffle(&self) -> Result<PlayerTransition, PlayerError> {
        self.request(|reply| PlayerCommand::Shuffle { reply }).await
    }

    /// Returns an immutable copy of the current guild state.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has stopped.
    pub async fn snapshot(&self) -> Result<PlayerSnapshot, PlayerError> {
        self.request(|reply| PlayerCommand::Snapshot { reply })
            .await
    }

    /// Applies a completion only if it belongs to the currently playing item.
    ///
    /// Stale callbacks are intentionally ignored, which prevents a late completion from a
    /// skipped track from advancing the replacement track.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has stopped.
    pub async fn track_finished(&self, queue_id: Uuid) -> Result<PlayerTransition, PlayerError> {
        self.request(|reply| PlayerCommand::TrackFinished { queue_id, reply })
            .await
    }

    /// Stops the actor after clearing its state.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has already stopped.
    pub async fn shutdown(&self) -> Result<PlayerTransition, PlayerError> {
        self.request(|reply| PlayerCommand::Shutdown { reply })
            .await
    }

    async fn request<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<T>) -> PlayerCommand,
    ) -> Result<T, PlayerError> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(command(reply))
            .await
            .map_err(|_| PlayerError::Stopped)?;
        response.await.map_err(|_| PlayerError::ResponseDropped)
    }
}

/// Starts one serialized player state machine for a guild.
///
/// `shuffle_seed` is injected so tests can reproduce a shuffle. A production supervisor should
/// derive a fresh seed at process start and keep it private to the guild actor.
///
/// # Panics
///
/// Panics if either bound is zero. Configuration validation prevents those values in normal use.
#[must_use]
pub fn spawn_guild_player(
    guild_id: u64,
    max_tracks: usize,
    mailbox_capacity: usize,
    shuffle_seed: u64,
    idle_timeout: Duration,
    volume_percent: u16,
) -> (
    GuildPlayerHandle,
    mpsc::Receiver<PlayerTransition>,
    JoinHandle<()>,
) {
    assert!(max_tracks > 0, "max_tracks must be positive");
    assert!(mailbox_capacity > 0, "mailbox_capacity must be positive");
    assert!(!idle_timeout.is_zero(), "idle_timeout must be positive");
    let (commands, receiver) = mpsc::channel(mailbox_capacity);
    let (transitions, transition_receiver) = mpsc::channel(mailbox_capacity);
    let actor = GuildPlayer {
        max_tracks,
        current: None,
        pending: VecDeque::new(),
        voice_channel_id: None,
        text_channel_id: None,
        paused: false,
        volume_percent,
        idle_deadline: None,
        idle_timeout,
        random: StdRng::seed_from_u64(shuffle_seed),
        receiver,
        transitions,
    };
    let task = tokio::spawn(actor.run());
    (
        GuildPlayerHandle { guild_id, commands },
        transition_receiver,
        task,
    )
}

#[derive(Debug)]
enum PlayerCommand {
    Enqueue {
        item: Box<QueueItem>,
        voice_channel_id: u64,
        reply: oneshot::Sender<Result<PlayerTransition, PlayerError>>,
    },
    Skip {
        reply: oneshot::Sender<SkipOutcome>,
    },
    Stop {
        reply: oneshot::Sender<PlayerTransition>,
    },
    EnqueueAll {
        items: Vec<QueueItem>,
        voice_channel_id: u64,
        reply: oneshot::Sender<Result<BulkEnqueue, PlayerError>>,
    },
    SetPaused {
        paused: bool,
        reply: oneshot::Sender<Result<PlayerTransition, PlayerError>>,
    },
    SetVolume {
        percent: u16,
        reply: oneshot::Sender<Result<PlayerTransition, PlayerError>>,
    },
    Shuffle {
        reply: oneshot::Sender<PlayerTransition>,
    },
    Snapshot {
        reply: oneshot::Sender<PlayerSnapshot>,
    },
    TrackFinished {
        queue_id: Uuid,
        reply: oneshot::Sender<PlayerTransition>,
    },
    Shutdown {
        reply: oneshot::Sender<PlayerTransition>,
    },
}

#[derive(Debug)]
struct GuildPlayer {
    max_tracks: usize,
    current: Option<QueueItem>,
    pending: VecDeque<QueueItem>,
    voice_channel_id: Option<u64>,
    text_channel_id: Option<u64>,
    paused: bool,
    volume_percent: u16,
    idle_deadline: Option<Instant>,
    idle_timeout: Duration,
    random: StdRng,
    receiver: mpsc::Receiver<PlayerCommand>,
    transitions: mpsc::Sender<PlayerTransition>,
}

impl GuildPlayer {
    async fn run(mut self) {
        loop {
            let command = if let Some(deadline) = self.idle_deadline {
                tokio::select! {
                    command = self.receiver.recv() => command,
                    () = time::sleep_until(deadline) => {
                        // A full clear, because the hold can now be reached with
                        // a track still held: leaving with one paused would keep
                        // a current track and a paused flag for a session that
                        // no longer has a voice channel to play them in.
                        self.clear();
                        if !self.emit(self.transition(PlaybackDirective::IdleDisconnect)).await {
                            break;
                        }
                        continue;
                    }
                }
            } else {
                self.receiver.recv().await
            };

            let Some(command) = command else {
                break;
            };
            match command {
                PlayerCommand::Enqueue {
                    item,
                    voice_channel_id,
                    reply,
                } => {
                    let result = self.enqueue(*item, voice_channel_id);
                    if let Ok(transition) = &result {
                        self.emit(transition.clone()).await;
                    }
                    let _ = reply.send(result);
                }
                PlayerCommand::Skip { reply } => {
                    let outcome = self.skip();
                    self.emit(outcome.transition.clone()).await;
                    let _ = reply.send(outcome);
                }
                PlayerCommand::Stop { reply } => {
                    let transition = self.stop();
                    self.emit(transition.clone()).await;
                    let _ = reply.send(transition);
                }
                PlayerCommand::EnqueueAll {
                    items,
                    voice_channel_id,
                    reply,
                } => {
                    let result = self.enqueue_all(items, voice_channel_id);
                    if let Ok(bulk) = &result {
                        self.emit(bulk.transition.clone()).await;
                    }
                    let _ = reply.send(result);
                }
                PlayerCommand::SetPaused { paused, reply } => {
                    let result = self.set_paused(paused);
                    if let Ok(transition) = &result {
                        self.emit(transition.clone()).await;
                    }
                    let _ = reply.send(result);
                }
                PlayerCommand::SetVolume { percent, reply } => {
                    let result = self.set_volume(percent);
                    if let Ok(transition) = &result {
                        self.emit(transition.clone()).await;
                    }
                    let _ = reply.send(result);
                }
                PlayerCommand::Shuffle { reply } => {
                    self.pending.make_contiguous().shuffle(&mut self.random);
                    let _ = reply.send(self.transition(PlaybackDirective::None));
                }
                PlayerCommand::Snapshot { reply } => {
                    let _ = reply.send(self.snapshot());
                }
                PlayerCommand::TrackFinished { queue_id, reply } => {
                    let transition = self.track_finished(queue_id);
                    self.emit(transition.clone()).await;
                    let _ = reply.send(transition);
                }
                PlayerCommand::Shutdown { reply } => {
                    let transition = self.stop();
                    self.emit(transition.clone()).await;
                    let _ = reply.send(transition);
                    break;
                }
            }
        }
    }

    async fn emit(&self, transition: PlayerTransition) -> bool {
        if transition.directive == PlaybackDirective::None {
            return true;
        }
        self.transitions.send(transition).await.is_ok()
    }

    fn enqueue(
        &mut self,
        item: QueueItem,
        voice_channel_id: u64,
    ) -> Result<PlayerTransition, PlayerError> {
        if self.len() >= self.max_tracks {
            return Err(PlayerError::QueueFull {
                max_tracks: self.max_tracks,
            });
        }
        self.check_voice_channel(voice_channel_id)?;
        self.text_channel_id = Some(item.response_channel_id);
        if self.current.is_none() {
            // Only a track that actually starts cancels the countdown. Adding
            // to a queue that is paused leaves the held session held, and its
            // hold running.
            self.idle_deadline = None;
            self.voice_channel_id = Some(voice_channel_id);
            self.current = Some(item.clone());
            Ok(self.transition(PlaybackDirective::Play(item)))
        } else {
            self.pending.push_back(item);
            Ok(self.transition(PlaybackDirective::None))
        }
    }

    fn enqueue_all(
        &mut self,
        items: Vec<QueueItem>,
        voice_channel_id: u64,
    ) -> Result<BulkEnqueue, PlayerError> {
        let offered = items.len();
        let room = self.max_tracks.saturating_sub(self.len());
        if room == 0 {
            return Err(PlayerError::QueueFull {
                max_tracks: self.max_tracks,
            });
        }
        self.check_voice_channel(voice_channel_id)?;

        let mut items = items;
        items.truncate(room);
        let accepted = items.len();
        let mut directive = PlaybackDirective::None;
        for item in items {
            // Reusing the single-item path keeps one description of what
            // enqueueing means, including which addition starts playback and
            // what that does to the idle countdown.
            let transition = self.enqueue(item, voice_channel_id)?;
            // Only the first item of a batch can start playback; every one
            // after it joins a queue that is no longer empty. Pairing that
            // directive with the snapshot taken after the whole batch is what
            // makes this one addition rather than fifty.
            if directive == PlaybackDirective::None {
                directive = transition.directive;
            }
        }
        Ok(BulkEnqueue {
            accepted,
            refused: offered - accepted,
            transition: self.transition(directive),
        })
    }

    fn check_voice_channel(&self, voice_channel_id: u64) -> Result<(), PlayerError> {
        if self.current.is_some()
            && self
                .voice_channel_id
                .is_some_and(|current| current != voice_channel_id)
        {
            return Err(PlayerError::VoiceChannelConflict {
                channel_id: self.voice_channel_id.expect("checked above"),
            });
        }
        Ok(())
    }

    fn skip(&mut self) -> SkipOutcome {
        let Some(skipped) = self.current.take() else {
            // Nothing was playing, so there is nothing to skip and no reason to
            // give up the channel. Any idle countdown already under way keeps
            // running, because a command that changed nothing should not
            // shorten or extend the hold.
            return SkipOutcome {
                skipped: None,
                transition: self.transition(PlaybackDirective::None),
            };
        };
        let directive = self
            .advance()
            .map_or(PlaybackDirective::Stop, PlaybackDirective::Replace);
        SkipOutcome {
            skipped: Some(skipped),
            transition: self.transition(directive),
        }
    }

    fn stop(&mut self) -> PlayerTransition {
        let had_tracks = !self.is_empty();
        self.clear();
        let directive = if had_tracks {
            PlaybackDirective::StopAndDisconnect
        } else {
            PlaybackDirective::Disconnect
        };
        self.transition(directive)
    }

    /// Drops everything about a session but the level it was playing at.
    ///
    /// Volume is the one piece of state a server sets once and means for the
    /// evening rather than for a track, so ending a session keeps it.
    fn clear(&mut self) {
        self.current = None;
        self.pending.clear();
        self.voice_channel_id = None;
        self.idle_deadline = None;
        self.paused = false;
    }

    fn set_paused(&mut self, paused: bool) -> Result<PlayerTransition, PlayerError> {
        if self.current.is_none() {
            return Err(PlayerError::NothingPlaying);
        }
        if self.paused == paused {
            return Err(if paused {
                PlayerError::AlreadyPaused
            } else {
                PlayerError::NotPaused
            });
        }
        self.paused = paused;
        // A held session is playing nothing to a channel people may have left,
        // which is the position an exhausted queue is in. Counting it down is
        // what keeps a forgotten pause from holding the channel all night; the
        // hold is long enough that a pause anybody comes back from survives it.
        self.idle_deadline = paused.then(|| Instant::now() + self.idle_timeout);
        Ok(self.transition(if paused {
            PlaybackDirective::Pause
        } else {
            PlaybackDirective::Resume
        }))
    }

    fn set_volume(&mut self, percent: u16) -> Result<PlayerTransition, PlayerError> {
        if percent == 0 || percent > MAX_VOLUME_PERCENT {
            return Err(PlayerError::VolumeOutOfRange {
                max_percent: MAX_VOLUME_PERCENT,
            });
        }
        self.volume_percent = percent;
        Ok(self.transition(PlaybackDirective::SetVolume))
    }

    fn track_finished(&mut self, queue_id: Uuid) -> PlayerTransition {
        if self.current.as_ref().map(|item| item.queue_id) != Some(queue_id) {
            return self.transition(PlaybackDirective::None);
        }
        let directive = self
            .advance()
            .map_or(PlaybackDirective::Stop, PlaybackDirective::Play);
        self.transition(directive)
    }

    /// Takes the next queued item, or starts the idle countdown when none is left.
    ///
    /// Running out of tracks is not a reason to leave: the channel is held for
    /// the whole idle timeout, and each track that begins playing cancels the
    /// countdown, so a queue kept fed never reaches it.
    fn advance(&mut self) -> Option<QueueItem> {
        self.current = self.pending.pop_front();
        // A hold belongs to the track it was placed on, so moving off that
        // track releases it rather than carrying it to the next one.
        self.paused = false;
        self.idle_deadline = if self.current.is_some() {
            None
        } else {
            Some(Instant::now() + self.idle_timeout)
        };
        self.current.clone()
    }

    fn transition(&self, directive: PlaybackDirective) -> PlayerTransition {
        PlayerTransition {
            directive,
            snapshot: self.snapshot(),
        }
    }

    fn snapshot(&self) -> PlayerSnapshot {
        PlayerSnapshot {
            current: self.current.clone(),
            pending: self.pending.iter().cloned().collect(),
            voice_channel_id: self.voice_channel_id,
            text_channel_id: self.text_channel_id,
            paused: self.paused,
            volume_percent: self.volume_percent,
        }
    }

    fn len(&self) -> usize {
        usize::from(self.current.is_some()) + self.pending.len()
    }

    fn is_empty(&self) -> bool {
        self.current.is_none() && self.pending.is_empty()
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PlayerError {
    #[error("guild queue has reached its {max_tracks}-track limit")]
    QueueFull { max_tracks: usize },
    #[error("guild player has stopped")]
    Stopped,
    #[error("guild player dropped a command response")]
    ResponseDropped,
    #[error("guild player is already active in voice channel {channel_id}")]
    VoiceChannelConflict { channel_id: u64 },
    #[error("nothing is playing")]
    NothingPlaying,
    #[error("playback is already paused")]
    AlreadyPaused,
    #[error("playback is not paused")]
    NotPaused,
    #[error("volume must be between 1 and {max_percent}")]
    VolumeOutOfRange { max_percent: u16 },
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use url::Url;

    use super::*;

    fn item(id: u128) -> QueueItem {
        QueueItem {
            queue_id: Uuid::from_u128(id),
            track: TrackMetadata {
                source_id: format!("source-{id}"),
                canonical_url: Url::parse(&format!("https://example.test/{id}")).unwrap(),
                title: format!("Track {id}"),
                channel: None,
                duration: Duration::from_secs(60),
                thumbnail_url: None,
            },
            requested_by: 42,
            response_channel_id: 99,
        }
    }

    #[tokio::test]
    async fn serializes_enqueue_and_advancement() {
        let (player, mut transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_secs(30), 50);
        let first = item(1);
        let second = item(2);

        let transition = player.enqueue(first.clone(), 10).await.unwrap();
        assert_eq!(transition.directive, PlaybackDirective::Play(first.clone()));
        assert_eq!(transitions.recv().await.unwrap(), transition);
        assert_eq!(
            player
                .enqueue(second.clone(), 10)
                .await
                .unwrap()
                .snapshot
                .len(),
            2
        );

        let transition = player.track_finished(first.queue_id).await.unwrap();
        assert_eq!(
            transition.directive,
            PlaybackDirective::Play(second.clone())
        );
        assert_eq!(transition.snapshot.current, Some(second));
        assert_eq!(transitions.recv().await.unwrap(), transition);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn a_batch_starts_playing_and_lands_as_one_addition() {
        let (player, mut transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        let items = (1..=3).map(item).collect::<Vec<_>>();

        let bulk = player.enqueue_all(items, 10).await.unwrap();
        assert_eq!(bulk.accepted, 3);
        assert_eq!(bulk.refused, 0);
        // Only the first of a batch can start playback, and the snapshot beside
        // it has to describe the whole batch rather than the moment that one
        // item landed.
        assert_eq!(bulk.transition.directive, PlaybackDirective::Play(item(1)));
        assert_eq!(bulk.transition.snapshot.len(), 3);
        assert_eq!(transitions.recv().await.unwrap(), bulk.transition);
        assert!(
            time::timeout(Duration::from_millis(50), transitions.recv())
                .await
                .is_err(),
            "a batch produced more than one directive"
        );

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn a_batch_takes_what_fits_and_reports_the_rest() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 2, 8, 1, Duration::from_secs(30), 50);
        let bulk = player
            .enqueue_all((1..=5).map(item).collect(), 10)
            .await
            .unwrap();
        assert_eq!(bulk.accepted, 2);
        assert_eq!(bulk.refused, 3);
        assert_eq!(bulk.transition.snapshot.len(), 2);

        // With no room at all there is nothing partial to report, so this is
        // the same refusal a single track gets.
        assert_eq!(
            player.enqueue_all(vec![item(9)], 10).await,
            Err(PlayerError::QueueFull { max_tracks: 2 })
        );

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_tracks_at_the_total_queue_bound() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 2, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.enqueue(item(2), 10).await.unwrap();
        assert_eq!(
            player.enqueue(item(3), 10).await,
            Err(PlayerError::QueueFull { max_tracks: 2 })
        );
        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn ignores_stale_track_completion_after_skip() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_secs(30), 50);
        let first = item(1);
        let second = item(2);
        player.enqueue(first.clone(), 10).await.unwrap();
        player.enqueue(second.clone(), 10).await.unwrap();

        let skipped = player.skip().await.unwrap();
        assert_eq!(skipped.skipped, Some(first.clone()));
        assert_eq!(
            skipped.transition.directive,
            PlaybackDirective::Replace(second.clone())
        );
        let stale = player.track_finished(first.queue_id).await.unwrap();
        assert_eq!(stale.directive, PlaybackDirective::None);
        assert_eq!(stale.snapshot.current, Some(second));

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn shuffle_never_moves_the_current_track() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 6, 8, 9, Duration::from_secs(30), 50);
        let current = item(1);
        player.enqueue(current.clone(), 10).await.unwrap();
        for id in 2..=6 {
            player.enqueue(item(id), 10).await.unwrap();
        }

        let shuffled = player.shuffle().await.unwrap().snapshot;
        assert_eq!(shuffled.current, Some(current));
        assert_eq!(shuffled.pending.len(), 5);
        let mut ids = shuffled
            .pending
            .iter()
            .map(|queued| queued.queue_id)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, (2..=6).map(Uuid::from_u128).collect::<Vec<_>>());

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn stop_clears_everything_atomically() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.enqueue(item(2), 10).await.unwrap();
        let transition = player.stop().await.unwrap();
        assert_eq!(transition.directive, PlaybackDirective::StopAndDisconnect);
        assert!(transition.snapshot.is_empty());

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn an_exhausted_queue_holds_the_channel_until_the_idle_deadline() {
        let (player, mut transitions, task) =
            spawn_guild_player(7, 2, 8, 1, Duration::from_millis(20), 50);
        let first = item(1);
        player.enqueue(first.clone(), 10).await.unwrap();
        assert!(matches!(
            transitions.recv().await.unwrap().directive,
            PlaybackDirective::Play(_)
        ));

        let completed = player.track_finished(first.queue_id).await.unwrap();
        assert_eq!(completed.directive, PlaybackDirective::Stop);
        assert_eq!(completed.snapshot.voice_channel_id, Some(10));
        assert_eq!(transitions.recv().await.unwrap(), completed);

        let idle = time::timeout(Duration::from_secs(1), transitions.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(idle.directive, PlaybackDirective::IdleDisconnect);
        assert_eq!(idle.snapshot.voice_channel_id, None);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn every_played_track_restarts_the_idle_countdown() {
        let idle_timeout = Duration::from_millis(200);
        let (player, mut transitions, task) = spawn_guild_player(7, 3, 8, 1, idle_timeout, 50);
        let first = item(1);
        player.enqueue(first.clone(), 10).await.unwrap();
        transitions.recv().await.unwrap();
        player.track_finished(first.queue_id).await.unwrap();
        transitions.recv().await.unwrap();

        // Queueing inside the idle window has to cancel the pending disconnect
        // rather than leave it armed, or the channel would drop mid-track.
        let second = item(2);
        player.enqueue(second.clone(), 10).await.unwrap();
        assert!(matches!(
            transitions.recv().await.unwrap().directive,
            PlaybackDirective::Play(_)
        ));
        assert!(
            time::timeout(idle_timeout * 2, transitions.recv())
                .await
                .is_err(),
            "the disconnect armed by the first track survived the second"
        );

        player.track_finished(second.queue_id).await.unwrap();
        assert_eq!(
            transitions.recv().await.unwrap().directive,
            PlaybackDirective::Stop
        );
        let idle = time::timeout(Duration::from_secs(1), transitions.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(idle.directive, PlaybackDirective::IdleDisconnect);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn skipping_the_last_track_holds_the_channel() {
        let (player, mut transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_secs(30), 50);
        let current = item(1);
        player.enqueue(current.clone(), 10).await.unwrap();
        assert!(matches!(
            transitions.recv().await.unwrap().directive,
            PlaybackDirective::Play(_)
        ));

        let outcome = player.skip().await.unwrap();
        assert_eq!(outcome.skipped, Some(current));
        assert_eq!(outcome.transition.directive, PlaybackDirective::Stop);
        assert_eq!(outcome.transition.snapshot.voice_channel_id, Some(10));
        assert_eq!(transitions.recv().await.unwrap(), outcome.transition);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn a_held_track_counts_down_and_a_resumed_one_stops_counting() {
        let idle_timeout = Duration::from_millis(200);
        let (player, mut transitions, task) = spawn_guild_player(7, 3, 8, 1, idle_timeout, 50);
        let current = item(1);
        player.enqueue(current.clone(), 10).await.unwrap();
        transitions.recv().await.unwrap();

        // Holding a track leaves nothing playing to a channel people may have
        // walked out of, so it has to be counted down like an empty queue.
        let paused = player.set_paused(true).await.unwrap();
        assert_eq!(paused.directive, PlaybackDirective::Pause);
        assert!(paused.snapshot.paused);
        assert_eq!(transitions.recv().await.unwrap(), paused);

        let resumed = player.set_paused(false).await.unwrap();
        assert_eq!(resumed.directive, PlaybackDirective::Resume);
        assert!(!resumed.snapshot.paused);
        assert_eq!(transitions.recv().await.unwrap(), resumed);
        assert!(
            time::timeout(idle_timeout * 2, transitions.recv())
                .await
                .is_err(),
            "the countdown armed by the pause outlived the resume"
        );

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn a_forgotten_pause_ends_the_session() {
        let (player, mut transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_millis(50), 50);
        let current = item(1);
        player.enqueue(current.clone(), 10).await.unwrap();
        transitions.recv().await.unwrap();
        player.set_paused(true).await.unwrap();
        transitions.recv().await.unwrap();

        let idle = time::timeout(Duration::from_secs(1), transitions.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(idle.directive, PlaybackDirective::IdleDisconnect);
        // Leaving with a track still held would keep a current track and a
        // paused flag for a session with no voice channel to play them in.
        assert!(idle.snapshot.is_empty());
        assert!(!idle.snapshot.paused);
        assert_eq!(idle.snapshot.voice_channel_id, None);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn queueing_beside_a_held_track_leaves_it_held() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.set_paused(true).await.unwrap();

        let queued = player.enqueue(item(2), 10).await.unwrap();
        assert!(queued.snapshot.paused, "adding to a queue resumed it");
        assert_eq!(queued.snapshot.pending.len(), 1);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn a_hold_belongs_to_the_track_it_was_placed_on() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.enqueue(item(2), 10).await.unwrap();
        player.set_paused(true).await.unwrap();

        let skipped = player.skip().await.unwrap();
        assert!(!skipped.transition.snapshot.paused);
        assert_eq!(skipped.transition.snapshot.current, Some(item(2)));

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn pausing_refuses_the_states_it_is_already_in() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_secs(30), 50);
        assert_eq!(
            player.set_paused(true).await,
            Err(PlayerError::NothingPlaying)
        );
        player.enqueue(item(1), 10).await.unwrap();
        assert_eq!(player.set_paused(false).await, Err(PlayerError::NotPaused));
        player.set_paused(true).await.unwrap();
        assert_eq!(
            player.set_paused(true).await,
            Err(PlayerError::AlreadyPaused)
        );

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn volume_is_bounded_and_outlives_the_session_that_set_it() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_secs(30), 50);
        assert_eq!(player.snapshot().await.unwrap().volume_percent, 50);
        for level in [0, MAX_VOLUME_PERCENT + 1] {
            assert_eq!(
                player.set_volume(level).await,
                Err(PlayerError::VolumeOutOfRange {
                    max_percent: MAX_VOLUME_PERCENT
                })
            );
        }

        let set = player.set_volume(30).await.unwrap();
        assert_eq!(set.directive, PlaybackDirective::SetVolume);
        assert_eq!(set.snapshot.volume_percent, 30);
        // A level is set once and meant for the evening, so ending a session
        // does not take it back to the configured default.
        player.stop().await.unwrap();
        assert_eq!(player.snapshot().await.unwrap().volume_percent, 30);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn skipping_an_idle_session_changes_nothing() {
        let (player, mut transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_secs(30), 50);
        let current = item(1);
        player.enqueue(current.clone(), 10).await.unwrap();
        transitions.recv().await.unwrap();
        player.track_finished(current.queue_id).await.unwrap();
        transitions.recv().await.unwrap();

        let outcome = player.skip().await.unwrap();
        assert_eq!(outcome.skipped, None);
        assert_eq!(outcome.transition.directive, PlaybackDirective::None);
        assert_eq!(outcome.transition.snapshot.voice_channel_id, Some(10));

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_cross_channel_enqueue_while_active() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 2, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        assert_eq!(
            player.enqueue(item(2), 11).await,
            Err(PlayerError::VoiceChannelConflict { channel_id: 10 })
        );
        player.shutdown().await.unwrap();
        task.await.unwrap();
    }
}
