use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Context as _, Result, bail};
use reqwest::{
    Client as HttpClient,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use songbird::input::{HlsRequest, HttpRequest, Input};

use crate::source::{SourceResolver, TrackMetadata};

/// Converts stable source metadata into a fresh, streaming Songbird input.
///
/// The resolver is called immediately before playback so temporary media URLs are never queued or
/// persisted. The HTTP client accepts only credential-free HTTPS redirects.
#[derive(Clone)]
pub struct AudioPipeline {
    resolver: Arc<dyn SourceResolver>,
    http: HttpClient,
}

impl std::fmt::Debug for AudioPipeline {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AudioPipeline")
            .finish_non_exhaustive()
    }
}

impl AudioPipeline {
    /// Builds a bounded HTTP pipeline for resolved media streams.
    ///
    /// # Errors
    ///
    /// Returns an error when the HTTP client cannot be configured.
    pub fn new(resolver: Arc<dyn SourceResolver>) -> Result<Self> {
        let http = HttpClient::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                let target = attempt.url();
                if attempt.previous().len() >= 5 {
                    attempt.error("media redirect limit exceeded")
                } else if target.scheme() != "https"
                    || !target.username().is_empty()
                    || target.password().is_some()
                {
                    attempt.error("media redirect was not credential-free HTTPS")
                } else {
                    attempt.follow()
                }
            }))
            .user_agent(concat!("auxide/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build the media HTTP client")?;
        Ok(Self { resolver, http })
    }

    /// Resolves a queued track and creates its streaming input.
    ///
    /// # Errors
    ///
    /// Returns an error when the source cannot be refreshed, supplies invalid headers, or selects
    /// a protocol outside Auxide's allowlist.
    pub async fn prepare(&self, track: &TrackMetadata) -> Result<Input> {
        let audio = self.resolver.resolve(track).await?;
        let headers = convert_headers(&audio.headers)?;
        match audio.protocol.as_deref() {
            None | Some("https") => Ok(HttpRequest::new_with_headers(
                self.http.clone(),
                audio.stream_url.to_string(),
                headers,
            )
            .into()),
            Some("m3u8" | "m3u8_native") => Ok(HlsRequest::new_with_headers(
                self.http.clone(),
                audio.stream_url.to_string(),
                headers,
            )
            .into()),
            Some(protocol) => bail!("source selected unsupported media protocol {protocol:?}"),
        }
    }
}

fn convert_headers(headers: &BTreeMap<String, String>) -> Result<HeaderMap> {
    let mut converted = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("source returned an invalid HTTP header name: {name:?}"))?;
        let value = HeaderValue::from_str(value)
            .with_context(|| format!("source returned an invalid value for header {name}"))?;
        converted.insert(name, value);
    }
    Ok(converted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_valid_resolver_headers() {
        let headers = BTreeMap::from([
            ("User-Agent".to_owned(), "test-agent".to_owned()),
            ("Referer".to_owned(), "https://www.youtube.com/".to_owned()),
        ]);
        let converted = convert_headers(&headers).unwrap();
        assert_eq!(converted["user-agent"], "test-agent");
    }

    #[test]
    fn rejects_header_injection() {
        let headers = BTreeMap::from([("X-Test".to_owned(), "ok\r\nevil: true".to_owned())]);
        assert!(convert_headers(&headers).is_err());
    }
}
