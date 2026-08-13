use std::{collections::BTreeMap, ffi::OsString, time::Duration};

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::Semaphore;
use url::Url;

use crate::{
    config::{PlaybackConfig, YouTubeConfig},
    process::CommandSpec,
    source::{ResolvedAudio, SourceError, SourceResolver, TrackMetadata},
};

const AUDIO_FORMAT: &str = "bestaudio[abr>0][vcodec=none]/bestaudio";

#[derive(Debug)]
pub struct YouTubeResolver {
    config: YouTubeConfig,
    max_duration: Duration,
    permits: Semaphore,
}

impl YouTubeResolver {
    #[must_use]
    pub fn new(config: YouTubeConfig, playback: &PlaybackConfig) -> Self {
        Self {
            permits: Semaphore::new(playback.max_concurrent_resolutions),
            max_duration: Duration::from_secs(playback.max_track_duration_seconds),
            config,
        }
    }

    fn validate_youtube_url(url: &Url) -> Result<(), SourceError> {
        if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
            return Err(SourceError::InvalidRequest(
                "YouTube URLs must use HTTPS and must not contain credentials".to_owned(),
            ));
        }
        let Some(host) = url.host_str() else {
            return Err(SourceError::InvalidRequest(
                "YouTube URL has no host".to_owned(),
            ));
        };
        let allowed = host == "youtu.be"
            || host == "youtube.com"
            || host.ends_with(".youtube.com")
            || host == "youtube-nocookie.com"
            || host.ends_with(".youtube-nocookie.com");
        if !allowed {
            return Err(SourceError::InvalidRequest(format!(
                "unsupported YouTube host: {host}"
            )));
        }
        Ok(())
    }

    async fn query(&self, target: &str) -> Result<Vec<YtDlpEntry>, SourceError> {
        if !self.config.enabled {
            return Err(SourceError::Disabled);
        }

        let _permit = self.permits.acquire().await.map_err(|_| {
            SourceError::ResolverFailed("source resolver is shutting down".to_owned())
        })?;
        let runtime = format!("deno:{}", self.config.deno_path.display());
        let args = [
            OsString::from("--dump-json"),
            OsString::from("--skip-download"),
            OsString::from("--no-playlist"),
            OsString::from("--no-warnings"),
            OsString::from("--format"),
            OsString::from(AUDIO_FORMAT),
            OsString::from("--js-runtimes"),
            OsString::from(runtime),
            OsString::from("--"),
            OsString::from(target),
        ];
        let output = CommandSpec::new(
            &self.config.yt_dlp_path,
            self.config.resolution_timeout(),
            self.config.max_output_bytes,
        )
        .args(args)
        .run()
        .await?;

        if !output.status.success() {
            return Err(SourceError::ResolverFailed(sanitize_stderr(&output.stderr)));
        }

        let entries = output
            .stdout
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| {
                serde_json::from_slice(line).map_err(|error| {
                    SourceError::InvalidResponse(format!("invalid yt-dlp JSON: {error}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if entries.is_empty() {
            return Err(SourceError::InvalidResponse(
                "yt-dlp returned no results".to_owned(),
            ));
        }
        Ok(entries)
    }

    fn metadata(&self, entry: &YtDlpEntry) -> Result<TrackMetadata, SourceError> {
        if entry.is_live.unwrap_or(false)
            || matches!(
                entry.live_status.as_deref(),
                Some("is_live" | "is_upcoming")
            )
        {
            return Err(SourceError::LiveStream);
        }

        let duration = entry
            .duration
            .filter(|duration| duration.is_finite() && *duration > 0.0)
            .ok_or_else(|| {
                SourceError::InvalidResponse("track has no finite positive duration".to_owned())
            })?;
        let duration = Duration::from_secs_f64(duration);
        if duration > self.max_duration {
            return Err(SourceError::DurationLimit);
        }

        let mut canonical_url =
            Url::parse("https://www.youtube.com/watch").expect("the static YouTube URL is valid");
        canonical_url.query_pairs_mut().append_pair("v", &entry.id);
        let thumbnail_url = entry
            .thumbnail
            .as_deref()
            .and_then(|value| Url::parse(value).ok())
            .filter(|url| url.scheme() == "https");

        Ok(TrackMetadata {
            source_id: entry.id.clone(),
            canonical_url,
            title: entry.title.clone(),
            channel: entry.channel.clone().or_else(|| entry.uploader.clone()),
            duration,
            thumbnail_url,
        })
    }
}

#[async_trait]
impl SourceResolver for YouTubeResolver {
    async fn search(&self, query: &str) -> Result<Vec<TrackMetadata>, SourceError> {
        let query = query.trim();
        if query.is_empty() || query.len() > 200 {
            return Err(SourceError::InvalidRequest(
                "search query must contain between 1 and 200 characters".to_owned(),
            ));
        }
        let target = format!("ytsearch{}:{query}", self.config.search_results);
        self.query(&target)
            .await?
            .iter()
            .map(|entry| self.metadata(entry))
            .collect()
    }

    async fn inspect(&self, url: &Url) -> Result<TrackMetadata, SourceError> {
        Self::validate_youtube_url(url)?;
        let entries = self.query(url.as_str()).await?;
        self.metadata(&entries[0])
    }

    async fn resolve(&self, track: &TrackMetadata) -> Result<ResolvedAudio, SourceError> {
        Self::validate_youtube_url(&track.canonical_url)?;
        let entries = self.query(track.canonical_url.as_str()).await?;
        let entry = &entries[0];
        let fresh_metadata = self.metadata(entry)?;
        if fresh_metadata.source_id != track.source_id {
            return Err(SourceError::InvalidResponse(
                "resolved video identity changed".to_owned(),
            ));
        }
        let stream_url = Url::parse(entry.url.as_deref().ok_or_else(|| {
            SourceError::InvalidResponse("yt-dlp returned no media URL".to_owned())
        })?)
        .map_err(|error| {
            SourceError::InvalidResponse(format!("yt-dlp returned an invalid media URL: {error}"))
        })?;
        if stream_url.scheme() != "https"
            || !stream_url.username().is_empty()
            || stream_url.password().is_some()
        {
            return Err(SourceError::InvalidResponse(
                "resolved media URL is not credential-free HTTPS".to_owned(),
            ));
        }

        Ok(ResolvedAudio {
            metadata: fresh_metadata,
            stream_url,
            headers: entry.http_headers.clone().unwrap_or_default(),
            protocol: entry.protocol.clone(),
        })
    }
}

#[derive(Debug, Deserialize)]
struct YtDlpEntry {
    id: String,
    title: String,
    duration: Option<f64>,
    channel: Option<String>,
    uploader: Option<String>,
    thumbnail: Option<String>,
    is_live: Option<bool>,
    live_status: Option<String>,
    url: Option<String>,
    protocol: Option<String>,
    http_headers: Option<BTreeMap<String, String>>,
}

fn sanitize_stderr(stderr: &[u8]) -> String {
    let value = String::from_utf8_lossy(stderr);
    let value = value.trim();
    if value.is_empty() {
        return "yt-dlp did not provide an error message".to_owned();
    }

    value
        .split_whitespace()
        .map(|part| {
            if let Ok(mut url) = Url::parse(part) {
                url.set_query(None);
                url.set_fragment(None);
                url.to_string()
            } else {
                part.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(2048)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolver(max_seconds: u64) -> YouTubeResolver {
        YouTubeResolver::new(
            YouTubeConfig::default(),
            &PlaybackConfig {
                max_track_duration_seconds: max_seconds,
                ..PlaybackConfig::default()
            },
        )
    }

    #[test]
    fn accepts_only_youtube_https_urls() {
        assert!(
            YouTubeResolver::validate_youtube_url(&Url::parse("https://youtu.be/abc").unwrap())
                .is_ok()
        );
        assert!(
            YouTubeResolver::validate_youtube_url(
                &Url::parse("https://music.youtube.com/watch?v=abc").unwrap()
            )
            .is_ok()
        );
        assert!(
            YouTubeResolver::validate_youtube_url(
                &Url::parse("http://youtube.com/watch?v=abc").unwrap()
            )
            .is_err()
        );
        assert!(
            YouTubeResolver::validate_youtube_url(
                &Url::parse("https://example.com/watch?v=abc").unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn parses_metadata_and_enforces_duration() {
        let entry: YtDlpEntry = serde_json::from_str(
            r#"{
                "id": "video-id",
                "title": "Example",
                "duration": 120.5,
                "channel": "Example Channel",
                "thumbnail": "https://i.ytimg.com/example.jpg",
                "is_live": false
            }"#,
        )
        .unwrap();
        let metadata = resolver(121).metadata(&entry).unwrap();
        assert_eq!(metadata.source_id, "video-id");
        assert_eq!(metadata.duration, Duration::from_millis(120_500));
        assert!(resolver(120).metadata(&entry).is_err());
    }

    #[test]
    fn rejects_live_content() {
        let entry: YtDlpEntry = serde_json::from_str(
            r#"{
                "id": "video-id",
                "title": "Live",
                "duration": 120,
                "is_live": true
            }"#,
        )
        .unwrap();
        assert!(matches!(
            resolver(121).metadata(&entry),
            Err(SourceError::LiveStream)
        ));
    }

    #[test]
    fn redacts_query_parameters_from_resolver_errors() {
        let error = sanitize_stderr(b"failed https://example.com/audio?token=secret now");
        assert!(!error.contains("secret"));
        assert!(error.contains("https://example.com/audio"));
    }
}
