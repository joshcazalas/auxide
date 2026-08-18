use std::{
    collections::{BTreeSet, VecDeque},
    time::Duration,
};

use rand::{Rng, SeedableRng, rngs::StdRng, seq::SliceRandom};
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

    /// Why the session is not playing, if it is not.
    pub hold: Option<Hold>,
    pub repeat: RepeatMode,

    /// Pick the next track at random instead of in order.
    ///
    /// Distinct from the one-shot reorder, which rearranges what is waiting
    /// once and leaves anything queued afterwards to land in order behind it.
    pub shuffle: bool,

    /// Playback level as whole percent of the source's own.
    ///
    /// Percent rather than the configuration's float so that a transition stays
    /// exactly comparable, which is what every test in this module relies on.
    pub volume_percent: u16,
}

impl PlayerSnapshot {
    #[must_use]
    pub const fn paused(&self) -> bool {
        self.hold.is_some()
    }

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
    /// Take a voice channel without playing anything through it yet.
    Join(u64),
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
    /// Let go of a queue `/leave` asked to be kept, now that the hold has run out.
    ///
    /// The same countdown as [`PlaybackDirective::IdleDisconnect`], reached from
    /// the other kind of idle session: this one gave up its voice channel when
    /// it was parked, so there is nothing left to leave. Kept apart because the
    /// two need different things said about them, and announcing a departure
    /// from a channel Auxide walked out of a quarter of an hour earlier
    /// describes something that did not happen.
    IdleExpired,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlayerTransition {
    pub directive: PlaybackDirective,
    pub snapshot: PlayerSnapshot,
    /// Whether the command that caused this is already reporting the result.
    ///
    /// A track that starts the moment somebody asks for it gets named twice:
    /// once by the reply to the command that asked, and once by the
    /// announcement made as it starts. Both are the same card, in the same
    /// channel, seconds apart. The reply is the one that cannot go missing —
    /// it travels on the interaction's own token and needs no permission to
    /// post — so the announcement is the one that gives way.
    ///
    /// Only the player can tell the two apart. A track beginning because
    /// somebody just queued it and a track beginning because the queue moved on
    /// reach the worker as the same directive, so the difference is recorded
    /// here, where it is still visible.
    ///
    /// It belongs on the transition rather than on the queued track: it
    /// describes this one occasion of something starting, not the track, which
    /// may come round again under a repeat mode with nobody waiting to hear.
    pub start_is_answered: bool,
}

/// Why a session is not playing.
///
/// Telling these apart is what lets somebody coming back resume a session the
/// room emptying paused, without also undoing a pause somebody asked for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Hold {
    /// Somebody asked for it, and only somebody can undo it.
    Requested,
    /// Everybody left, so it lifts when anybody comes back.
    Abandoned,
}

/// Which holds a release applies to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Release {
    /// Continue whatever the reason it stopped.
    Anything,
    /// Continue only what the room emptying paused.
    OnlyAbandoned,
}

/// What a queue does when a track ends.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RepeatMode {
    /// Move on, and stop when nothing is left.
    #[default]
    Off,
    /// Play the same track again.
    Single,
    /// Send each finished track to the back, so the queue cycles.
    All,
}

impl RepeatMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Single => "single",
            Self::All => "all",
        }
    }
}

/// Why a track is being moved off.
///
/// Repeat modes have to tell these apart: a track that ended may be owed
/// another play, and a track somebody skipped past may not.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Departure {
    Finished,
    Skipped,
}

/// What a shuffle request asks for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShuffleChange {
    Enable,
    Disable,
    Toggle,
    /// Rearrange what is waiting, without changing how the next is chosen.
    Reorder,
}

/// How many played tracks a session remembers.
///
/// Ephemeral like the queue itself, and bounded for the same reason: a session
/// left running all week should not grow without limit.
pub const HISTORY_LENGTH: usize = 50;

/// Waiting tracks that were taken out together.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RemovedTracks {
    pub removed: Vec<QueueItem>,
    pub transition: PlayerTransition,
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

/// The result of a [`GuildPlayerHandle::clear`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClearOutcome {
    /// How many waiting tracks were dropped.
    pub removed: usize,
    pub transition: PlayerTransition,
}

/// The result of a [`GuildPlayerHandle::remove`], carrying what was taken out.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveOutcome {
    pub removed: QueueItem,
    pub transition: PlayerTransition,
}

/// What came of asking to skip the current track.
///
/// A shared queue needs somebody else to agree before one person can cut off
/// what everybody is listening to, so asking is not the same as it happening.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SkipVerdict {
    /// Enough people wanted it, or it was the asker's own track.
    Skipped(Box<SkipOutcome>),
    /// Counted, and waiting for others.
    Pending { have: usize, needed: usize },
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

    /// Counts a vote to skip the current track, and skips once enough agree.
    ///
    /// `needed` comes from how many people are listening, which the actor
    /// cannot see. Voting for a track twice is not an error and does not count
    /// twice.
    ///
    /// # Errors
    ///
    /// Returns [`PlayerError::NothingPlaying`] when there is nothing to skip,
    /// or a channel error if the actor has stopped.
    pub async fn vote_skip(&self, user_id: u64, needed: usize) -> Result<SkipVerdict, PlayerError> {
        self.request(|reply| PlayerCommand::VoteSkip {
            user_id,
            needed,
            reply,
        })
        .await?
    }

    /// Drops a run of waiting tracks, counting from one.
    ///
    /// # Errors
    ///
    /// Returns [`PlayerError::NoSuchPosition`] when the run starts past the end
    /// of the queue, or a channel error if the actor has stopped.
    pub async fn remove_range(&self, from: usize, to: usize) -> Result<RemovedTracks, PlayerError> {
        self.request(|reply| PlayerCommand::RemoveRange { from, to, reply })
            .await?
    }

    /// Drops every waiting track one person queued.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has stopped.
    pub async fn remove_requester(&self, user_id: u64) -> Result<RemovedTracks, PlayerError> {
        self.request(|reply| PlayerCommand::RemoveRequester { user_id, reply })
            .await
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

    /// Drops everything waiting, and leaves the current track playing.
    ///
    /// The difference from [`GuildPlayerHandle::stop`] is the whole point:
    /// throwing out a queue somebody filled with the wrong thing should not
    /// also mean ending the session.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has stopped.
    pub async fn clear(&self) -> Result<ClearOutcome, PlayerError> {
        self.request(|reply| PlayerCommand::Clear { reply }).await
    }

    /// Takes one waiting track out of the queue, counting from one.
    ///
    /// Positions are the ones `/queue` prints, and they only ever refer to
    /// waiting tracks — the current one is `/skip`'s business.
    ///
    /// # Errors
    ///
    /// Returns [`PlayerError::NoSuchPosition`] when nothing is waiting there,
    /// or a channel error if the actor has stopped.
    pub async fn remove(&self, position: usize) -> Result<RemoveOutcome, PlayerError> {
        self.request(|reply| PlayerCommand::Remove { position, reply })
            .await?
    }

    /// Stops the current track where it is, for the given reason.
    ///
    /// Holding starts the idle countdown. A held session is playing nothing to
    /// people who may well have wandered off, which is the same situation an
    /// exhausted queue is in, and it should end the same way.
    ///
    /// # Errors
    ///
    /// Returns [`PlayerError::NothingPlaying`] when there is no current track,
    /// [`PlayerError::AlreadyPaused`] when it is already held, or a channel
    /// error if the actor has stopped.
    pub async fn hold(&self, reason: Hold) -> Result<PlayerTransition, PlayerError> {
        self.request(|reply| PlayerCommand::Hold { reason, reply })
            .await?
    }

    /// Continues a held track.
    ///
    /// # Errors
    ///
    /// Returns [`PlayerError::NotPaused`] when a [`Release::Anything`] finds
    /// nothing held, or a channel error if the actor has stopped. A narrower
    /// release finding nothing of its kind is not an error — somebody walking
    /// back in should not fail because nobody had left.
    pub async fn release(&self, scope: Release) -> Result<PlayerTransition, PlayerError> {
        self.request(|reply| PlayerCommand::Release { scope, reply })
            .await?
    }

    /// Gives up the voice channel while keeping the queue.
    ///
    /// The difference from [`GuildPlayerHandle::stop`] is what makes leaving
    /// worth saying separately: coming back finds the queue as it was. The
    /// current track restarts rather than resuming, because the connection it
    /// was playing through is gone.
    ///
    /// # Errors
    ///
    /// Returns [`PlayerError::NotConnected`] when there is no channel to give
    /// up, or a channel error if the actor has stopped.
    pub async fn park(&self) -> Result<PlayerTransition, PlayerError> {
        self.request(|reply| PlayerCommand::Park { reply }).await?
    }

    /// Takes a voice channel, and picks a parked queue back up.
    ///
    /// # Errors
    ///
    /// Returns [`PlayerError::VoiceChannelConflict`] when a session is already
    /// running elsewhere, or a channel error if the actor has stopped.
    pub async fn join(&self, voice_channel_id: u64) -> Result<PlayerTransition, PlayerError> {
        self.request(|reply| PlayerCommand::Join {
            voice_channel_id,
            reply,
        })
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

    /// Turns shuffle on or off, or reorders the waiting tracks once.
    ///
    /// Both use the actor's private random-number generator, so a run is
    /// reproducible from the seed it was started with.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has stopped.
    pub async fn shuffle(&self, change: ShuffleChange) -> Result<PlayerTransition, PlayerError> {
        self.request(|reply| PlayerCommand::Shuffle { change, reply })
            .await
    }

    /// Sets what the queue does when a track ends.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has stopped.
    pub async fn set_repeat(&self, repeat: RepeatMode) -> Result<PlayerTransition, PlayerError> {
        self.request(|reply| PlayerCommand::SetRepeat { repeat, reply })
            .await
    }

    /// Returns what has already played, most recent first.
    ///
    /// Off the snapshot deliberately: a snapshot is cloned onto every
    /// transition, and carrying fifty played tracks through each one would cost
    /// far more than the two commands that ask for them.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has stopped.
    pub async fn history(&self) -> Result<Vec<QueueItem>, PlayerError> {
        self.request(|reply| PlayerCommand::History { reply }).await
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
        hold: None,
        votes: BTreeSet::new(),
        history: VecDeque::new(),
        repeat: RepeatMode::Off,
        shuffle: false,
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
    VoteSkip {
        user_id: u64,
        needed: usize,
        reply: oneshot::Sender<Result<SkipVerdict, PlayerError>>,
    },
    RemoveRange {
        from: usize,
        to: usize,
        reply: oneshot::Sender<Result<RemovedTracks, PlayerError>>,
    },
    RemoveRequester {
        user_id: u64,
        reply: oneshot::Sender<RemovedTracks>,
    },
    Stop {
        reply: oneshot::Sender<PlayerTransition>,
    },
    Clear {
        reply: oneshot::Sender<ClearOutcome>,
    },
    Remove {
        position: usize,
        reply: oneshot::Sender<Result<RemoveOutcome, PlayerError>>,
    },
    EnqueueAll {
        items: Vec<QueueItem>,
        voice_channel_id: u64,
        reply: oneshot::Sender<Result<BulkEnqueue, PlayerError>>,
    },
    Hold {
        reason: Hold,
        reply: oneshot::Sender<Result<PlayerTransition, PlayerError>>,
    },
    Release {
        scope: Release,
        reply: oneshot::Sender<Result<PlayerTransition, PlayerError>>,
    },
    Park {
        reply: oneshot::Sender<Result<PlayerTransition, PlayerError>>,
    },
    Join {
        voice_channel_id: u64,
        reply: oneshot::Sender<Result<PlayerTransition, PlayerError>>,
    },
    SetVolume {
        percent: u16,
        reply: oneshot::Sender<Result<PlayerTransition, PlayerError>>,
    },
    Shuffle {
        change: ShuffleChange,
        reply: oneshot::Sender<PlayerTransition>,
    },
    SetRepeat {
        repeat: RepeatMode,
        reply: oneshot::Sender<PlayerTransition>,
    },
    History {
        reply: oneshot::Sender<Vec<QueueItem>>,
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
    hold: Option<Hold>,
    /// Who has asked to cut the current track short.
    votes: BTreeSet<u64>,
    /// What has already played, oldest first.
    history: VecDeque<QueueItem>,
    repeat: RepeatMode,
    shuffle: bool,
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
                        // Two different sessions run this same countdown, and
                        // only one of them is in a voice channel. A session
                        // holding a channel with nothing queued is leaving it,
                        // and the room is owed an explanation. A session `/leave`
                        // parked is only letting go of the queue it was asked to
                        // keep — it walked out a quarter of an hour ago, so
                        // announcing a departure would describe something that
                        // did not happen. Which one this is has to be read
                        // before the clear, because the clear is what erases the
                        // difference.
                        let held_a_channel = self.voice_channel_id.is_some();
                        let kept_a_queue = !self.is_empty();
                        // A full clear, because the hold can now be reached with
                        // a track still held: leaving with one paused would keep
                        // a current track and a paused flag for a session that
                        // no longer has a voice channel to play them in.
                        self.clear();
                        let directive = if held_a_channel {
                            PlaybackDirective::IdleDisconnect
                        } else if kept_a_queue {
                            PlaybackDirective::IdleExpired
                        } else {
                            // Parked with an empty queue, so `/leave` promised
                            // to keep nothing and nothing has been lost. There
                            // is no event here to tell anybody about, and
                            // inventing one would be the same mistake in the
                            // other direction.
                            PlaybackDirective::None
                        };
                        if !self.emit(self.transition(directive)).await {
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
            if !self.handle(command).await {
                break;
            }
        }
    }

    /// Applies one command, and reports whether the actor should keep running.
    async fn handle(&mut self, command: PlayerCommand) -> bool {
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
            PlayerCommand::Clear { reply } => {
                let removed = self.pending.len();
                self.pending.clear();
                // No directive: what is playing is untouched, and the channel
                // is kept. Only the waiting list changed.
                let _ = reply.send(ClearOutcome {
                    removed,
                    transition: self.transition(PlaybackDirective::None),
                });
            }
            PlayerCommand::Remove { position, reply } => {
                let _ = reply.send(self.remove(position));
            }
            PlayerCommand::VoteSkip {
                user_id,
                needed,
                reply,
            } => {
                let result = self.vote_skip(user_id, needed);
                if let Ok(SkipVerdict::Skipped(outcome)) = &result {
                    self.emit(outcome.transition.clone()).await;
                }
                let _ = reply.send(result);
            }
            PlayerCommand::RemoveRange { from, to, reply } => {
                let _ = reply.send(self.remove_range(from, to));
            }
            PlayerCommand::RemoveRequester { user_id, reply } => {
                let _ = reply.send(self.remove_requester(user_id));
            }
            other => return self.handle_playback(other).await,
        }
        true
    }

    /// Applies the commands about what is playing, rather than what is queued.
    async fn handle_playback(&mut self, command: PlayerCommand) -> bool {
        match command {
            PlayerCommand::Hold { reason, reply } => {
                let result = self.hold(reason);
                if let Ok(transition) = &result {
                    self.emit(transition.clone()).await;
                }
                let _ = reply.send(result);
            }
            PlayerCommand::Release { scope, reply } => {
                let result = self.release(scope);
                if let Ok(transition) = &result {
                    self.emit(transition.clone()).await;
                }
                let _ = reply.send(result);
            }
            PlayerCommand::Park { reply } => {
                let result = self.park();
                if let Ok(transition) = &result {
                    self.emit(transition.clone()).await;
                }
                let _ = reply.send(result);
            }
            PlayerCommand::Join {
                voice_channel_id,
                reply,
            } => {
                let result = self.join(voice_channel_id);
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
            PlayerCommand::Shuffle { change, reply } => {
                let _ = reply.send(self.change_shuffle(change));
            }
            PlayerCommand::SetRepeat { repeat, reply } => {
                let _ = reply.send(self.set_repeat(repeat));
            }
            PlayerCommand::History { reply } => {
                let _ = reply.send(self.history.iter().rev().cloned().collect());
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
                return false;
            }
            forwarded => unreachable!("{forwarded:?} is handled before this point"),
        }
        true
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
        // What decides whether this starts anything is whether the session is
        // playing, and a session can fail to be playing in two ways. The queue
        // may be empty, or `/leave` may have parked it: a parked session still
        // holds the track it was on, but it gave up the channel, so asking
        // whether a track exists would answer that one is already playing when
        // nothing is. It would then queue behind a track nobody can hear, on a
        // connection nobody holds, and report a cheerful position in a queue
        // that would never advance again.
        if self.voice_channel_id.is_none() {
            if let Some(current) = self.current.clone() {
                // `/leave` promised to keep this queue, so coming back to it
                // resumes what was held and the new track waits its turn.
                self.pending.push_back(item);
                return Ok(self.resume_held(voice_channel_id, current));
            }
        }
        if self.current.is_none() {
            // Only a track that actually starts cancels the countdown. Adding
            // to a queue that is paused leaves the held session held, and its
            // hold running.
            self.idle_deadline = None;
            self.voice_channel_id = Some(voice_channel_id);
            self.current = Some(item.clone());
            Ok(self.answered(PlaybackDirective::Play(item)))
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
            .advance(Some(skipped.clone()), Departure::Skipped)
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
    /// Volume is about the room's ears and survives; repeat and shuffle are
    /// about a particular queue, and that queue is what this throws away.
    fn clear(&mut self) {
        self.current = None;
        self.pending.clear();
        self.voice_channel_id = None;
        self.idle_deadline = None;
        self.hold = None;
        self.votes.clear();
        self.repeat = RepeatMode::Off;
        self.shuffle = false;
    }

    fn remove(&mut self, position: usize) -> Result<RemoveOutcome, PlayerError> {
        let index = position
            .checked_sub(1)
            .filter(|index| *index < self.pending.len())
            .ok_or(PlayerError::NoSuchPosition {
                position,
                waiting: self.pending.len(),
            })?;
        let removed = self
            .pending
            .remove(index)
            .expect("the index was just bounds-checked");
        Ok(RemoveOutcome {
            removed,
            transition: self.transition(PlaybackDirective::None),
        })
    }

    /// Counts one vote, and skips once enough people have asked.
    ///
    /// Somebody may always skip what they queued themselves. That is what keeps
    /// a vote from being a way to trap the room with your own choice, and it
    /// means the rule only ever gets in the way of cutting off somebody else.
    fn vote_skip(&mut self, user_id: u64, needed: usize) -> Result<SkipVerdict, PlayerError> {
        let current = self.current.as_ref().ok_or(PlayerError::NothingPlaying)?;
        if current.requested_by == user_id {
            return Ok(SkipVerdict::Skipped(Box::new(self.skip())));
        }
        self.votes.insert(user_id);
        let have = self.votes.len();
        if have >= needed {
            return Ok(SkipVerdict::Skipped(Box::new(self.skip())));
        }
        Ok(SkipVerdict::Pending { have, needed })
    }

    fn remove_range(&mut self, from: usize, to: usize) -> Result<RemovedTracks, PlayerError> {
        let first = from
            .checked_sub(1)
            .filter(|first| *first < self.pending.len());
        let Some(first) = first else {
            return Err(PlayerError::NoSuchPosition {
                position: from,
                waiting: self.pending.len(),
            });
        };
        // A run reaching past the end takes what is there rather than failing.
        // Asking to clear "everything from ten onwards" should not depend on
        // knowing how long the queue is.
        let last = to.max(from).min(self.pending.len());
        let removed = self.pending.drain(first..last).collect();
        Ok(RemovedTracks {
            removed,
            transition: self.transition(PlaybackDirective::None),
        })
    }

    /// Drops every waiting track one person queued, leaving everyone else's.
    ///
    /// The current track is deliberately untouched: cutting off what the room
    /// is listening to is what the vote is for.
    fn remove_requester(&mut self, user_id: u64) -> RemovedTracks {
        let mut removed = Vec::new();
        let mut kept = VecDeque::with_capacity(self.pending.len());
        for item in std::mem::take(&mut self.pending) {
            if item.requested_by == user_id {
                removed.push(item);
            } else {
                kept.push_back(item);
            }
        }
        self.pending = kept;
        RemovedTracks {
            removed,
            transition: self.transition(PlaybackDirective::None),
        }
    }

    fn hold(&mut self, reason: Hold) -> Result<PlayerTransition, PlayerError> {
        if self.current.is_none() {
            return Err(PlayerError::NothingPlaying);
        }
        if self.hold.is_some() {
            return Err(PlayerError::AlreadyPaused);
        }
        self.hold = Some(reason);
        // A held session is playing nothing to a channel people may have left,
        // which is the position an exhausted queue is in. Counting it down is
        // what keeps a forgotten pause, or a room nobody came back to, from
        // holding the channel all night; the hold is long enough that anything
        // somebody returns from survives it.
        self.idle_deadline = Some(Instant::now() + self.idle_timeout);
        Ok(self.transition(PlaybackDirective::Pause))
    }

    fn release(&mut self, scope: Release) -> Result<PlayerTransition, PlayerError> {
        match (self.hold, scope) {
            (Some(_), Release::Anything) | (Some(Hold::Abandoned), Release::OnlyAbandoned) => {}
            // Walking back into a room where nobody had left, or where somebody
            // deliberately paused, is not a failure — it just changes nothing.
            (Some(Hold::Requested) | None, Release::OnlyAbandoned) => {
                return Ok(self.transition(PlaybackDirective::None));
            }
            (None, Release::Anything) => return Err(PlayerError::NotPaused),
        }
        self.hold = None;
        self.idle_deadline = None;
        Ok(self.transition(PlaybackDirective::Resume))
    }

    /// Gives up the channel and keeps everything queued for it.
    fn park(&mut self) -> Result<PlayerTransition, PlayerError> {
        if self.voice_channel_id.take().is_none() {
            return Err(PlayerError::NotConnected);
        }
        // Held rather than merely stopped, so coming back knows to start the
        // current track again rather than treating it as already playing.
        if self.current.is_some() {
            self.hold = Some(Hold::Requested);
        }
        self.idle_deadline = Some(Instant::now() + self.idle_timeout);
        Ok(self.transition(PlaybackDirective::Disconnect))
    }

    /// Takes a channel, and starts a parked queue playing again.
    fn join(&mut self, voice_channel_id: u64) -> Result<PlayerTransition, PlayerError> {
        self.check_voice_channel(voice_channel_id)?;
        // Refused rather than repeated, and the mirror of what parking an
        // already-parked session does. Coming back restarts the current track
        // from the beginning, which is right when the connection it was playing
        // through is gone and wrong when it is still playing: `/join` typed by
        // somebody who is already listening would otherwise cut the song off
        // and start it over for the whole room.
        if self.voice_channel_id == Some(voice_channel_id) {
            return Err(PlayerError::AlreadyConnected);
        }
        let Some(current) = self.current.clone() else {
            // Nothing to play, so this is somebody asking Auxide to be present.
            // It waits exactly as long as an exhausted queue would.
            self.voice_channel_id = Some(voice_channel_id);
            self.idle_deadline = Some(Instant::now() + self.idle_timeout);
            return Ok(self.transition(PlaybackDirective::Join(voice_channel_id)));
        };
        Ok(self.resume_held(voice_channel_id, current))
    }

    /// Takes a channel and starts the track a parked session kept, from the top.
    ///
    /// The connection that track was playing through is gone, so there is no
    /// position left to resume to and it begins again rather than continuing.
    /// That makes it a different performance from the one the room was voting
    /// on, so the votes cast against the last one do not carry into it — this
    /// is the one way what is playing changes without going through `advance`
    /// or `clear`, which is why it has to say so itself.
    fn resume_held(&mut self, voice_channel_id: u64, current: QueueItem) -> PlayerTransition {
        self.voice_channel_id = Some(voice_channel_id);
        self.hold = None;
        self.idle_deadline = None;
        self.votes.clear();
        // Both ways back into a parked session — `/join`, and `/play` queueing
        // into one — reply by naming the track they are starting again, so the
        // announcement would be the second time the room was told.
        self.answered(PlaybackDirective::Play(current))
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
        let finished = self.current.take();
        let directive = self
            .advance(finished, Departure::Finished)
            .map_or(PlaybackDirective::Stop, PlaybackDirective::Play);
        self.transition(directive)
    }

    /// Takes the next queued item, or starts the idle countdown when none is left.
    ///
    /// Running out of tracks is not a reason to leave: the channel is held for
    /// the whole idle timeout, and each track that begins playing cancels the
    /// countdown, so a queue kept fed never reaches it. A repeating queue never
    /// runs out at all, so it never reaches the countdown either.
    ///
    /// `left` is the track being moved off, which a repeat mode may put back.
    fn advance(&mut self, left: Option<QueueItem>, reason: Departure) -> Option<QueueItem> {
        // A hold, and the votes to cut a track short, both belong to the track
        // they were placed on rather than to the session.
        self.hold = None;
        self.votes.clear();
        if let Some(left) = &left {
            self.remember(left.clone());
        }
        if let Some(left) = left {
            match (self.repeat, reason) {
                // Asking to move on has to move on. Replaying the same track
                // would make the skip do nothing, which is a worse answer than
                // ignoring the mode for one command.
                (RepeatMode::Single, Departure::Finished) => {
                    self.current = Some(left);
                    self.idle_deadline = None;
                    return self.current.clone();
                }
                // Repeating everything means the rotation survives a skip too,
                // so a skipped track comes round again rather than being lost.
                (RepeatMode::All, _) => self.pending.push_back(left),
                _ => {}
            }
        }

        self.current = if self.shuffle {
            self.take_random()
        } else {
            self.pending.pop_front()
        };
        self.idle_deadline = if self.current.is_some() {
            None
        } else {
            Some(Instant::now() + self.idle_timeout)
        };
        self.current.clone()
    }

    /// Files a track under what has already played.
    ///
    /// A repeated track is not filed twice in a row: under `single` it would
    /// otherwise fill the whole history with one title.
    fn remember(&mut self, item: QueueItem) {
        if self.history.back().map(|last| last.queue_id) == Some(item.queue_id) {
            return;
        }
        if self.history.len() == HISTORY_LENGTH {
            self.history.pop_front();
        }
        self.history.push_back(item);
    }

    fn take_random(&mut self) -> Option<QueueItem> {
        if self.pending.is_empty() {
            return None;
        }
        let index = self.random.random_range(0..self.pending.len());
        self.pending.remove(index)
    }

    fn set_repeat(&mut self, repeat: RepeatMode) -> PlayerTransition {
        self.repeat = repeat;
        self.transition(PlaybackDirective::None)
    }

    /// Applies a shuffle request, and reports whether the mode ended up on.
    fn change_shuffle(&mut self, change: ShuffleChange) -> PlayerTransition {
        match change {
            ShuffleChange::Enable => self.shuffle = true,
            ShuffleChange::Disable => self.shuffle = false,
            ShuffleChange::Toggle => self.shuffle = !self.shuffle,
            // The one-shot reorder, which rearranges what is already waiting
            // and leaves how the next track gets chosen alone.
            ShuffleChange::Reorder => self.pending.make_contiguous().shuffle(&mut self.random),
        }
        self.transition(PlaybackDirective::None)
    }

    fn transition(&self, directive: PlaybackDirective) -> PlayerTransition {
        PlayerTransition {
            directive,
            snapshot: self.snapshot(),
            start_is_answered: false,
        }
    }

    /// A transition whose command is already telling the room what happened.
    ///
    /// Used by the two commands that start a track and then name it in their
    /// own reply: queueing into a session with nothing playing, and coming back
    /// to a parked one. Everything else reaches a listener only if the
    /// announcement makes it.
    fn answered(&self, directive: PlaybackDirective) -> PlayerTransition {
        PlayerTransition {
            start_is_answered: true,
            ..self.transition(directive)
        }
    }

    fn snapshot(&self) -> PlayerSnapshot {
        PlayerSnapshot {
            current: self.current.clone(),
            pending: self.pending.iter().cloned().collect(),
            voice_channel_id: self.voice_channel_id,
            text_channel_id: self.text_channel_id,
            hold: self.hold,
            repeat: self.repeat,
            shuffle: self.shuffle,
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
    #[error("Auxide is not in a voice channel")]
    NotConnected,
    #[error("Auxide is already in that voice channel")]
    AlreadyConnected,
    #[error("volume must be between 1 and {max_percent}")]
    VolumeOutOfRange { max_percent: u16 },
    #[error("there is no track waiting at position {position}; {waiting} are queued")]
    NoSuchPosition { position: usize, waiting: usize },
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
    async fn clearing_empties_the_queue_and_leaves_the_track_playing() {
        let (player, mut transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        transitions.recv().await.unwrap();
        for id in 2..=4 {
            player.enqueue(item(id), 10).await.unwrap();
        }

        let cleared = player.clear().await.unwrap();
        assert_eq!(cleared.removed, 3);
        // The difference from stop, which is the whole point of the command.
        assert_eq!(cleared.transition.snapshot.current, Some(item(1)));
        assert_eq!(cleared.transition.snapshot.voice_channel_id, Some(10));
        assert!(cleared.transition.snapshot.pending.is_empty());
        assert_eq!(
            cleared.transition.directive,
            PlaybackDirective::None,
            "clearing the queue disturbed playback"
        );

        // Clearing again has nothing to drop, and still does not stop anything.
        assert_eq!(player.clear().await.unwrap().removed, 0);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn removing_takes_the_track_the_printed_position_names() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        for id in 1..=4 {
            player.enqueue(item(id), 10).await.unwrap();
        }

        // Position one is the first *waiting* track, because the current one is
        // what /skip is for.
        let removed = player.remove(1).await.unwrap();
        assert_eq!(removed.removed, item(2));
        assert_eq!(
            removed.transition.snapshot.pending,
            vec![item(3), item(4)],
            "removing renumbered the wrong way"
        );
        assert_eq!(removed.transition.snapshot.current, Some(item(1)));

        // And the positions shift up, so the same number now names a new track.
        assert_eq!(player.remove(1).await.unwrap().removed, item(3));

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn removing_refuses_a_position_that_is_not_there() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.enqueue(item(2), 10).await.unwrap();

        for position in [0, 2, 99] {
            assert_eq!(
                player.remove(position).await,
                Err(PlayerError::NoSuchPosition {
                    position,
                    waiting: 1
                }),
                "accepted position {position}"
            );
        }

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn skipping_your_own_track_never_needs_anybody_else() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.enqueue(item(2), 10).await.unwrap();

        // item()'s requester is 42, and a threshold of five would be
        // unreachable — which is the point: your own track ignores it.
        let verdict = player.vote_skip(42, 5).await.unwrap();
        let SkipVerdict::Skipped(outcome) = verdict else {
            panic!("a requester could not skip their own track: {verdict:?}");
        };
        assert_eq!(outcome.skipped, Some(item(1)));

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn cutting_somebody_elses_track_short_waits_for_agreement() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();

        assert_eq!(
            player.vote_skip(100, 2).await.unwrap(),
            SkipVerdict::Pending { have: 1, needed: 2 }
        );
        // Voting twice is not an error, and does not count twice.
        assert_eq!(
            player.vote_skip(100, 2).await.unwrap(),
            SkipVerdict::Pending { have: 1, needed: 2 }
        );
        assert!(matches!(
            player.vote_skip(200, 2).await.unwrap(),
            SkipVerdict::Skipped(_)
        ));

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn votes_belong_to_the_track_they_were_cast_against() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.enqueue(item(2), 10).await.unwrap();
        player.vote_skip(100, 3).await.unwrap();

        // The track it was cast against is gone, so the vote goes with it —
        // otherwise a track could arrive already part-way to being skipped.
        player.track_finished(item(1).queue_id).await.unwrap();
        assert_eq!(
            player.vote_skip(200, 3).await.unwrap(),
            SkipVerdict::Pending { have: 1, needed: 3 }
        );

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn a_run_of_waiting_tracks_comes_out_together() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 9, 8, 1, Duration::from_secs(30), 50);
        for id in 1..=6 {
            player.enqueue(item(id), 10).await.unwrap();
        }

        let removed = player.remove_range(2, 4).await.unwrap();
        assert_eq!(removed.removed, vec![item(3), item(4), item(5)]);
        assert_eq!(removed.transition.snapshot.pending, vec![item(2), item(6)]);
        // The current track is never in range; cutting that short is the vote's
        // business.
        assert_eq!(removed.transition.snapshot.current, Some(item(1)));

        // A run reaching past the end takes what is there, so clearing
        // "everything from here" does not need the queue's length.
        let rest = player.remove_range(1, 99).await.unwrap();
        assert_eq!(rest.removed.len(), 2);
        assert!(rest.transition.snapshot.pending.is_empty());

        assert_eq!(
            player.remove_range(1, 2).await,
            Err(PlayerError::NoSuchPosition {
                position: 1,
                waiting: 0
            })
        );

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn everything_one_person_queued_comes_out_and_nobody_elses() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 9, 8, 1, Duration::from_secs(30), 50);
        let mine = |id| QueueItem {
            requested_by: 500,
            ..item(id)
        };
        player.enqueue(item(1), 10).await.unwrap();
        player.enqueue(mine(2), 10).await.unwrap();
        player.enqueue(item(3), 10).await.unwrap();
        player.enqueue(mine(4), 10).await.unwrap();

        let removed = player.remove_requester(500).await.unwrap();
        assert_eq!(removed.removed, vec![mine(2), mine(4)]);
        assert_eq!(removed.transition.snapshot.pending, vec![item(3)]);
        assert_eq!(removed.transition.snapshot.current, Some(item(1)));

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn what_played_is_remembered_newest_first_and_bounded() {
        // Enough room for every transition these tracks produce. A full
        // channel stalls the actor, which is the right production behaviour and
        // the wrong thing for a test that never listens to it.
        let (player, _transitions, task) =
            spawn_guild_player(7, 200, 256, 1, Duration::from_secs(30), 50);
        assert!(player.history().await.unwrap().is_empty());

        for id in 1..=(HISTORY_LENGTH as u128 + 5) {
            player.enqueue(item(id), 10).await.unwrap();
            player.track_finished(item(id).queue_id).await.unwrap();
        }

        let history = player.history().await.unwrap();
        assert_eq!(history.len(), HISTORY_LENGTH);
        // Newest first, because the thing worth finding is usually the thing
        // that just played.
        assert_eq!(history[0], item(HISTORY_LENGTH as u128 + 5));
        assert_eq!(history[HISTORY_LENGTH - 1], item(6));

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn a_repeated_track_is_not_filed_twice_in_a_row() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.set_repeat(RepeatMode::Single).await.unwrap();

        for _ in 0..4 {
            player.track_finished(item(1).queue_id).await.unwrap();
        }
        // Otherwise repeating one track would fill the whole history with it.
        assert_eq!(player.history().await.unwrap(), vec![item(1)]);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn a_skipped_track_is_remembered_too() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.enqueue(item(2), 10).await.unwrap();

        player.skip().await.unwrap();
        // Cutting a track short is still having played it, and it is exactly
        // the case somebody wants to find again.
        assert_eq!(player.history().await.unwrap(), vec![item(1)]);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn leaving_gives_up_the_channel_and_keeps_the_queue() {
        let (player, mut transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.enqueue(item(2), 10).await.unwrap();
        transitions.recv().await.unwrap();

        let parked = player.park().await.unwrap();
        assert_eq!(parked.directive, PlaybackDirective::Disconnect);
        // The whole difference from stopping: the queue is still there.
        assert_eq!(parked.snapshot.len(), 2);
        assert_eq!(parked.snapshot.voice_channel_id, None);
        assert!(parked.snapshot.paused());
        assert_eq!(transitions.recv().await.unwrap(), parked);

        // Coming back starts the current track again rather than resuming it:
        // the connection it was playing through is gone, so there is no
        // position left to resume to.
        let rejoined = player.join(11).await.unwrap();
        assert_eq!(rejoined.directive, PlaybackDirective::Play(item(1)));
        assert_eq!(rejoined.snapshot.voice_channel_id, Some(11));
        assert!(!rejoined.snapshot.paused());

        assert_eq!(player.park().await.unwrap().snapshot.len(), 2);
        assert_eq!(player.park().await, Err(PlayerError::NotConnected));

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn joining_with_nothing_queued_waits_like_an_emptied_queue() {
        let (player, mut transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_millis(80), 50);

        let joined = player.join(10).await.unwrap();
        assert_eq!(joined.directive, PlaybackDirective::Join(10));
        assert_eq!(transitions.recv().await.unwrap(), joined);

        // Being asked to be present is not a reason to stay for ever.
        let idle = time::timeout(Duration::from_secs(1), transitions.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(idle.directive, PlaybackDirective::IdleDisconnect);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn playing_something_brings_a_parked_queue_back_instead_of_queueing_into_silence() {
        let (player, mut transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        let held = item(1);
        player.enqueue(held.clone(), 10).await.unwrap();
        transitions.recv().await.unwrap();

        let parked = player.park().await.unwrap();
        assert_eq!(parked.directive, PlaybackDirective::Disconnect);
        assert_eq!(transitions.recv().await.unwrap(), parked);

        // A parked session still holds the track it was on, so asking whether
        // one exists answers that something is already playing when nothing is.
        // /play then queued behind a track nobody could hear, on a connection
        // nobody held, and reported a position in a queue that would never
        // advance — no directive reached the worker, so Auxide simply never
        // came back and never said why.
        let added = item(2);
        let resumed = player.enqueue(added.clone(), 10).await.unwrap();
        assert_eq!(
            resumed.directive,
            PlaybackDirective::Play(held.clone()),
            "/play left the session parked"
        );
        assert_eq!(transitions.recv().await.unwrap(), resumed);
        // /leave promised to keep the queue, so what was held resumes and the
        // track just asked for waits its turn rather than displacing it.
        assert_eq!(resumed.snapshot.current, Some(held));
        assert_eq!(resumed.snapshot.len(), 2);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn a_parked_queue_expiring_never_claims_to_have_left_a_channel() {
        let idle_timeout = Duration::from_millis(200);
        let (player, mut transitions, task) = spawn_guild_player(7, 5, 8, 1, idle_timeout, 50);
        player.enqueue(item(1), 10).await.unwrap();
        transitions.recv().await.unwrap();
        player.park().await.unwrap();
        transitions.recv().await.unwrap();

        // The countdown /leave arms is the same one an idle channel runs, and
        // the two used to end identically — so a quarter of an hour after
        // somebody dismissed Auxide, the room was told it had just left a voice
        // channel it had not been in since. Letting go of a kept queue is what
        // actually happened, and it is what gets said.
        let expired = time::timeout(Duration::from_secs(1), transitions.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired.directive, PlaybackDirective::IdleExpired);
        assert_eq!(expired.snapshot.len(), 0);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn a_parked_session_that_kept_nothing_has_nothing_to_report() {
        let idle_timeout = Duration::from_millis(200);
        let (player, mut transitions, task) = spawn_guild_player(7, 5, 8, 1, idle_timeout, 50);
        player.join(10).await.unwrap();
        transitions.recv().await.unwrap();
        player.park().await.unwrap();
        transitions.recv().await.unwrap();

        // Dismissed with an empty queue, so nothing was promised and nothing
        // was lost. Announcing an expiry here would be the same mistake as
        // announcing a departure — inventing an event, only in the other
        // direction.
        assert!(
            time::timeout(idle_timeout * 4, transitions.recv())
                .await
                .is_err(),
            "an empty parked session announced something expiring"
        );

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn joining_a_channel_auxide_is_already_in_leaves_the_track_alone() {
        let (player, mut transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        let playing = item(1);
        player.enqueue(playing.clone(), 10).await.unwrap();
        transitions.recv().await.unwrap();

        // Coming back restarts the current track, which is right when the
        // connection it was playing through is gone and wrong when everybody is
        // still listening to it. Typed by somebody already in the channel, this
        // used to cut the song off and start it again for the whole room.
        assert!(matches!(
            player.join(10).await,
            Err(PlayerError::AlreadyConnected)
        ));
        assert!(
            time::timeout(Duration::from_millis(100), transitions.recv())
                .await
                .is_err(),
            "/join into the channel it already holds disturbed playback"
        );

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn votes_against_a_track_do_not_survive_it_starting_over() {
        let (player, mut transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        transitions.recv().await.unwrap();
        assert_eq!(
            player.vote_skip(1, 3).await.unwrap(),
            SkipVerdict::Pending { have: 1, needed: 3 }
        );

        player.park().await.unwrap();
        transitions.recv().await.unwrap();
        player.join(11).await.unwrap();
        transitions.recv().await.unwrap();

        // Parking and coming back is the one way what is playing changes
        // without going through advance or clear, and it restarts the track
        // from zero. A vote banked against the performance that is now over
        // would count towards cutting short the one that just began.
        assert_eq!(
            player.vote_skip(2, 3).await.unwrap(),
            SkipVerdict::Pending { have: 1, needed: 3 },
            "a vote survived the track it was cast against"
        );

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn only_a_start_nobody_asked_for_is_left_to_the_announcement() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);

        // Queueing into a silent session starts the track at once, and the
        // reply to that command names it. Announcing it as well put the same
        // card in the same channel twice, seconds apart, every single time
        // somebody started playing something.
        let first = item(1);
        let started = player.enqueue(first.clone(), 10).await.unwrap();
        assert!(matches!(started.directive, PlaybackDirective::Play(_)));
        assert!(
            started.start_is_answered,
            "the reply already named this one"
        );

        // A track queued behind it is added, not started, so nothing has named
        // it yet — and when the queue reaches it, the announcement is the only
        // thing that will.
        let queued = player.enqueue(item(2), 10).await.unwrap();
        assert_eq!(queued.directive, PlaybackDirective::None);
        let advanced = player.track_finished(first.queue_id).await.unwrap();
        assert!(matches!(advanced.directive, PlaybackDirective::Play(_)));
        assert!(
            !advanced.start_is_answered,
            "the queue moving on was left unannounced"
        );

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn coming_back_to_a_parked_queue_is_answered_by_the_command() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.park().await.unwrap();

        // Both ways back in reply by naming the track they are starting again,
        // so the announcement would be the second time the room was told.
        let rejoined = player.join(10).await.unwrap();
        assert!(matches!(rejoined.directive, PlaybackDirective::Play(_)));
        assert!(rejoined.start_is_answered);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn a_track_coming_round_again_is_announced_like_any_other() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        let only = item(1);
        let started = player.enqueue(only.clone(), 10).await.unwrap();
        assert!(started.start_is_answered);
        player.set_repeat(RepeatMode::Single).await.unwrap();

        // The same track, but a different occasion of it starting, and this
        // time nobody ran a command and is waiting to be told. Had this been
        // recorded against the track rather than against the transition, a
        // repeating queue would have gone silent after its first play.
        let again = player.track_finished(only.queue_id).await.unwrap();
        assert!(matches!(again.directive, PlaybackDirective::Play(_)));
        assert!(
            !again.start_is_answered,
            "a repeat kept the first play's answer"
        );

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn somebody_returning_lifts_only_the_hold_the_empty_room_put_there() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 5, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();

        // The room emptied, so anybody walking back in continues it.
        player.hold(Hold::Abandoned).await.unwrap();
        let returned = player.release(Release::OnlyAbandoned).await.unwrap();
        assert_eq!(returned.directive, PlaybackDirective::Resume);
        assert!(!returned.snapshot.paused());

        // A pause somebody asked for survives being alone and coming back: it
        // is theirs to undo, not the room's.
        player.hold(Hold::Requested).await.unwrap();
        let ignored = player.release(Release::OnlyAbandoned).await.unwrap();
        assert_eq!(ignored.directive, PlaybackDirective::None);
        assert!(
            ignored.snapshot.paused(),
            "somebody walking in undid a pause"
        );
        assert_eq!(
            player.release(Release::Anything).await.unwrap().directive,
            PlaybackDirective::Resume
        );

        // And walking into a room nobody had left changes nothing at all.
        assert_eq!(
            player
                .release(Release::OnlyAbandoned)
                .await
                .unwrap()
                .directive,
            PlaybackDirective::None
        );
        assert_eq!(
            player.release(Release::Anything).await,
            Err(PlayerError::NotPaused)
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

        let shuffled = player
            .shuffle(ShuffleChange::Reorder)
            .await
            .unwrap()
            .snapshot;
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
    async fn repeating_one_track_replays_it_but_never_blocks_a_skip() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.enqueue(item(2), 10).await.unwrap();
        player.set_repeat(RepeatMode::Single).await.unwrap();

        let replayed = player.track_finished(item(1).queue_id).await.unwrap();
        assert_eq!(replayed.directive, PlaybackDirective::Play(item(1)));
        assert_eq!(replayed.snapshot.pending.len(), 1);

        // Asking to move on has to move on, or the command would do nothing at
        // all for as long as the mode is set.
        let skipped = player.skip().await.unwrap();
        assert_eq!(
            skipped.transition.directive,
            PlaybackDirective::Replace(item(2))
        );

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn repeating_everything_cycles_and_never_runs_dry() {
        let (player, mut transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_millis(50), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.enqueue(item(2), 10).await.unwrap();
        transitions.recv().await.unwrap();
        player.set_repeat(RepeatMode::All).await.unwrap();

        let second = player.track_finished(item(1).queue_id).await.unwrap();
        assert_eq!(second.directive, PlaybackDirective::Play(item(2)));
        let first_again = player.track_finished(item(2).queue_id).await.unwrap();
        assert_eq!(first_again.directive, PlaybackDirective::Play(item(1)));
        assert_eq!(first_again.snapshot.len(), 2);

        // A cycling queue never empties, so the idle countdown that an emptied
        // one arms is never reached.
        assert!(
            time::timeout(Duration::from_millis(200), transitions.recv())
                .await
                .unwrap()
                .is_some_and(|transition| transition.directive
                    != PlaybackDirective::IdleDisconnect)
        );

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn shuffle_mode_outlives_the_tracks_it_reorders() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 6, 8, 9, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        let enabled = player.shuffle(ShuffleChange::Enable).await.unwrap();
        assert!(enabled.snapshot.shuffle);

        for id in 2..=5 {
            player.enqueue(item(id), 10).await.unwrap();
        }
        // The one-shot reorder rearranges what is waiting; the mode decides how
        // the next track is picked, so anything queued later is still shuffled.
        let mut seen = Vec::new();
        for _ in 0..4 {
            let ending = player.snapshot().await.unwrap().current.unwrap().queue_id;
            let next = player.track_finished(ending).await.unwrap();
            seen.push(next.snapshot.current.unwrap().queue_id);
        }
        seen.sort_unstable();
        assert_eq!(seen, (2..=5).map(Uuid::from_u128).collect::<Vec<_>>());

        assert!(
            !player
                .shuffle(ShuffleChange::Toggle)
                .await
                .unwrap()
                .snapshot
                .shuffle
        );

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn ending_a_session_forgets_how_that_queue_was_being_played() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.set_repeat(RepeatMode::All).await.unwrap();
        player.shuffle(ShuffleChange::Enable).await.unwrap();
        player.set_volume(30).await.unwrap();

        let stopped = player.stop().await.unwrap();
        assert_eq!(stopped.snapshot.repeat, RepeatMode::Off);
        assert!(!stopped.snapshot.shuffle);
        // Volume is about the room's ears rather than about this queue, so it
        // is the one setting that survives.
        assert_eq!(stopped.snapshot.volume_percent, 30);

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
        let paused = player.hold(Hold::Requested).await.unwrap();
        assert_eq!(paused.directive, PlaybackDirective::Pause);
        assert!(paused.snapshot.paused());
        assert_eq!(transitions.recv().await.unwrap(), paused);

        let resumed = player.release(Release::Anything).await.unwrap();
        assert_eq!(resumed.directive, PlaybackDirective::Resume);
        assert!(!resumed.snapshot.paused());
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
        player.hold(Hold::Requested).await.unwrap();
        transitions.recv().await.unwrap();

        let idle = time::timeout(Duration::from_secs(1), transitions.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(idle.directive, PlaybackDirective::IdleDisconnect);
        // Leaving with a track still held would keep a current track and a
        // paused flag for a session with no voice channel to play them in.
        assert!(idle.snapshot.is_empty());
        assert!(!idle.snapshot.paused());
        assert_eq!(idle.snapshot.voice_channel_id, None);

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn queueing_beside_a_held_track_leaves_it_held() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_secs(30), 50);
        player.enqueue(item(1), 10).await.unwrap();
        player.hold(Hold::Requested).await.unwrap();

        let queued = player.enqueue(item(2), 10).await.unwrap();
        assert!(queued.snapshot.paused(), "adding to a queue resumed it");
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
        player.hold(Hold::Requested).await.unwrap();

        let skipped = player.skip().await.unwrap();
        assert!(!skipped.transition.snapshot.paused());
        assert_eq!(skipped.transition.snapshot.current, Some(item(2)));

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn pausing_refuses_the_states_it_is_already_in() {
        let (player, _transitions, task) =
            spawn_guild_player(7, 3, 8, 1, Duration::from_secs(30), 50);
        assert_eq!(
            player.hold(Hold::Requested).await,
            Err(PlayerError::NothingPlaying)
        );
        player.enqueue(item(1), 10).await.unwrap();
        assert_eq!(
            player.release(Release::Anything).await,
            Err(PlayerError::NotPaused)
        );
        player.hold(Hold::Requested).await.unwrap();
        assert_eq!(
            player.hold(Hold::Requested).await,
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
