use std::collections::VecDeque;

use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::source::TrackMetadata;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueItem {
    pub queue_id: Uuid,
    pub track: TrackMetadata,
    pub requested_by: u64,
}

impl QueueItem {
    #[must_use]
    pub fn new(track: TrackMetadata, requested_by: u64) -> Self {
        Self {
            queue_id: Uuid::new_v4(),
            track,
            requested_by,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlayerSnapshot {
    pub current: Option<QueueItem>,
    pub pending: Vec<QueueItem>,
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
    StopAndDisconnect,
    Disconnect,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlayerTransition {
    pub directive: PlaybackDirective,
    pub snapshot: PlayerSnapshot,
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
    pub async fn enqueue(&self, item: QueueItem) -> Result<PlayerTransition, PlayerError> {
        self.request(|reply| PlayerCommand::Enqueue {
            item: Box::new(item),
            reply,
        })
        .await?
    }

    /// Removes the current item and advances atomically to the next item, if any.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has stopped.
    pub async fn skip(&self) -> Result<PlayerTransition, PlayerError> {
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
) -> (GuildPlayerHandle, JoinHandle<()>) {
    assert!(max_tracks > 0, "max_tracks must be positive");
    assert!(mailbox_capacity > 0, "mailbox_capacity must be positive");
    let (commands, receiver) = mpsc::channel(mailbox_capacity);
    let actor = GuildPlayer {
        max_tracks,
        current: None,
        pending: VecDeque::new(),
        random: StdRng::seed_from_u64(shuffle_seed),
        receiver,
    };
    let task = tokio::spawn(actor.run());
    (GuildPlayerHandle { guild_id, commands }, task)
}

#[derive(Debug)]
enum PlayerCommand {
    Enqueue {
        item: Box<QueueItem>,
        reply: oneshot::Sender<Result<PlayerTransition, PlayerError>>,
    },
    Skip {
        reply: oneshot::Sender<PlayerTransition>,
    },
    Stop {
        reply: oneshot::Sender<PlayerTransition>,
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
    random: StdRng,
    receiver: mpsc::Receiver<PlayerCommand>,
}

impl GuildPlayer {
    async fn run(mut self) {
        while let Some(command) = self.receiver.recv().await {
            match command {
                PlayerCommand::Enqueue { item, reply } => {
                    let _ = reply.send(self.enqueue(*item));
                }
                PlayerCommand::Skip { reply } => {
                    let _ = reply.send(self.skip());
                }
                PlayerCommand::Stop { reply } => {
                    let _ = reply.send(self.stop());
                }
                PlayerCommand::Shuffle { reply } => {
                    self.pending.make_contiguous().shuffle(&mut self.random);
                    let _ = reply.send(self.transition(PlaybackDirective::None));
                }
                PlayerCommand::Snapshot { reply } => {
                    let _ = reply.send(self.snapshot());
                }
                PlayerCommand::TrackFinished { queue_id, reply } => {
                    let _ = reply.send(self.track_finished(queue_id));
                }
                PlayerCommand::Shutdown { reply } => {
                    let _ = reply.send(self.stop());
                    break;
                }
            }
        }
    }

    fn enqueue(&mut self, item: QueueItem) -> Result<PlayerTransition, PlayerError> {
        if self.len() >= self.max_tracks {
            return Err(PlayerError::QueueFull {
                max_tracks: self.max_tracks,
            });
        }
        if self.current.is_none() {
            self.current = Some(item.clone());
            Ok(self.transition(PlaybackDirective::Play(item)))
        } else {
            self.pending.push_back(item);
            Ok(self.transition(PlaybackDirective::None))
        }
    }

    fn skip(&mut self) -> PlayerTransition {
        if self.current.is_none() {
            return self.transition(PlaybackDirective::None);
        }
        self.current = self.pending.pop_front();
        let directive = self.current.clone().map_or(
            PlaybackDirective::StopAndDisconnect,
            PlaybackDirective::Replace,
        );
        self.transition(directive)
    }

    fn stop(&mut self) -> PlayerTransition {
        let had_tracks = !self.is_empty();
        self.current = None;
        self.pending.clear();
        let directive = if had_tracks {
            PlaybackDirective::StopAndDisconnect
        } else {
            PlaybackDirective::Disconnect
        };
        self.transition(directive)
    }

    fn track_finished(&mut self, queue_id: Uuid) -> PlayerTransition {
        if self.current.as_ref().map(|item| item.queue_id) != Some(queue_id) {
            return self.transition(PlaybackDirective::None);
        }
        self.current = self.pending.pop_front();
        let directive = self
            .current
            .clone()
            .map_or(PlaybackDirective::Disconnect, PlaybackDirective::Play);
        self.transition(directive)
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
        }
    }

    #[tokio::test]
    async fn serializes_enqueue_and_advancement() {
        let (player, task) = spawn_guild_player(7, 3, 8, 1);
        let first = item(1);
        let second = item(2);

        let transition = player.enqueue(first.clone()).await.unwrap();
        assert_eq!(transition.directive, PlaybackDirective::Play(first.clone()));
        assert_eq!(
            player.enqueue(second.clone()).await.unwrap().snapshot.len(),
            2
        );

        let transition = player.track_finished(first.queue_id).await.unwrap();
        assert_eq!(
            transition.directive,
            PlaybackDirective::Play(second.clone())
        );
        assert_eq!(transition.snapshot.current, Some(second));

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_tracks_at_the_total_queue_bound() {
        let (player, task) = spawn_guild_player(7, 2, 8, 1);
        player.enqueue(item(1)).await.unwrap();
        player.enqueue(item(2)).await.unwrap();
        assert_eq!(
            player.enqueue(item(3)).await,
            Err(PlayerError::QueueFull { max_tracks: 2 })
        );
        player.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn ignores_stale_track_completion_after_skip() {
        let (player, task) = spawn_guild_player(7, 3, 8, 1);
        let first = item(1);
        let second = item(2);
        player.enqueue(first.clone()).await.unwrap();
        player.enqueue(second.clone()).await.unwrap();

        assert_eq!(
            player.skip().await.unwrap().directive,
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
        let (player, task) = spawn_guild_player(7, 6, 8, 9);
        let current = item(1);
        player.enqueue(current.clone()).await.unwrap();
        for id in 2..=6 {
            player.enqueue(item(id)).await.unwrap();
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
        let (player, task) = spawn_guild_player(7, 3, 8, 1);
        player.enqueue(item(1)).await.unwrap();
        player.enqueue(item(2)).await.unwrap();
        let transition = player.stop().await.unwrap();
        assert_eq!(transition.directive, PlaybackDirective::StopAndDisconnect);
        assert!(transition.snapshot.is_empty());

        player.shutdown().await.unwrap();
        task.await.unwrap();
    }
}
