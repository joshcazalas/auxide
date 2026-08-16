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

    /// Total size of the media, when the source reports one.
    ///
    /// `YouTube` answers a request carrying no range, or an open-ended
    /// `bytes=N-`, with 403 Forbidden; only a bounded `bytes=N-M` is served.
    /// Songbird can only build a bounded range if it knows where the media
    /// ends, so this is what makes playback possible rather than a
    /// nicety for seeking.
    pub content_length: Option<u64>,
}

#[async_trait]
pub trait SourceResolver: Send + Sync {
    async fn search(&self, query: &str) -> Result<Vec<TrackMetadata>, SourceError>;
    async fn inspect(&self, url: &Url) -> Result<TrackMetadata, SourceError>;
    async fn resolve(&self, track: &TrackMetadata) -> Result<ResolvedAudio, SourceError>;
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
