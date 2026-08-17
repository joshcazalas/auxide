mod youtube;

use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

pub use youtube::YouTubeResolver;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TrackMetadata {
    pub source_id: String,
    pub canonical_url: Url,
    pub title: String,
    pub channel: Option<String>,
    pub duration: Duration,
    pub thumbnail_url: Option<Url>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedAudio {
    pub metadata: TrackMetadata,
    pub stream_url: Url,
    pub headers: BTreeMap<String, String>,
    pub protocol: Option<String>,
}

/// A list of tracks a single URL turned out to name.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Playlist {
    pub title: Option<String>,
    pub tracks: Vec<TrackMetadata>,

    /// How many entries the playlist holds in total.
    ///
    /// Expansion stops at a configured ceiling, and `tracks.len()` therefore
    /// says how many were taken rather than how many exist. Reporting both is
    /// what lets the answer say what it left behind instead of quietly
    /// dropping it.
    pub total: usize,
}

#[async_trait]
pub trait SourceResolver: Send + Sync {
    async fn search(&self, query: &str) -> Result<Vec<TrackMetadata>, SourceError>;
    async fn inspect(&self, url: &Url) -> Result<TrackMetadata, SourceError>;
    async fn resolve(&self, track: &TrackMetadata) -> Result<ResolvedAudio, SourceError>;

    /// Expands a URL that names a list of tracks, or reports that it does not.
    ///
    /// Returning `None` is the ordinary answer for an ordinary link, and it is
    /// what lets one command accept both without asking the caller to tell them
    /// apart.
    async fn playlist(&self, url: &Url) -> Result<Option<Playlist>, SourceError>;
}

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("this source is disabled")]
    Disabled,
    #[error("invalid source request: {0}")]
    InvalidRequest(String),
    #[error("source resolver process failed: {0}")]
    Process(#[from] crate::process::ProcessError),
    #[error("source resolver exited unsuccessfully: {0}")]
    ResolverFailed(String),
    #[error("source returned invalid data: {0}")]
    InvalidResponse(String),
    #[error("track exceeds the configured maximum duration")]
    DurationLimit,
    #[error("live streams are not supported")]
    LiveStream,
}
