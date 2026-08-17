//! The real runtime, against a Discord that does not exist.
//!
//! Everything here is decided before Auxide does any work, and cannot be
//! changed afterwards: Discord fixes an interaction's audience when it is first
//! acknowledged. A wrong answer is therefore unrecoverable at runtime and, until
//! this existed, invisible in the test suite.

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use tokio::time;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{
    config::Config,
    discord::{Overrides, run_with},
    fake_discord::{FakeDiscord, string_option},
    source::{Playlist, ResolvedAudio, SourceError, SourceResolver, TrackMetadata},
    voice::fake::FakeVoice,
};

/// A source that answers from memory, so no test reaches yt-dlp.
struct StubSource;

impl StubSource {
    fn track(id: &str, title: &str) -> TrackMetadata {
        TrackMetadata {
            source_id: id.to_owned(),
            canonical_url: Url::parse(&format!("https://www.youtube.com/watch?v={id}")).unwrap(),
            title: title.to_owned(),
            channel: Some("Example Channel".to_owned()),
            duration: Duration::from_secs(210),
            thumbnail_url: None,
        }
    }
}

#[async_trait]
impl SourceResolver for StubSource {
    async fn search(&self, query: &str) -> Result<Vec<TrackMetadata>, SourceError> {
        Ok(vec![
            Self::track("aaa", &format!("{query} (first)")),
            Self::track("bbb", &format!("{query} (second)")),
        ])
    }

    async fn inspect(&self, url: &Url) -> Result<TrackMetadata, SourceError> {
        if url.as_str().contains("broken") {
            return Err(SourceError::LiveStream);
        }
        Ok(Self::track("aaa", "A Resolved Track"))
    }

    async fn resolve(&self, _track: &TrackMetadata) -> Result<ResolvedAudio, SourceError> {
        Err(SourceError::Disabled)
    }

    async fn playlist(&self, _url: &Url) -> Result<Option<Playlist>, SourceError> {
        Ok(None)
    }
}

/// A configuration with a token file that exists and says nothing real.
///
/// The token is never checked by anything: the fake answers every request
/// regardless of what is presented.
fn config(token: &std::path::Path) -> Config {
    toml::from_str(&format!(
        r#"
[discord]
token_file = "{}"

[observability]
listen_address = "127.0.0.1:0"
"#,
        token.display()
    ))
    .expect("the test configuration parses")
}

/// Starts the real runtime against the fake, and hands back both.
async fn start() -> (Arc<FakeDiscord>, CancellationToken, tempfile::NamedTempFile) {
    let mut token = tempfile::NamedTempFile::new().expect("a token file");
    std::io::Write::write_all(&mut token, b"not-a-real-token").expect("the token is written");
    let fake = FakeDiscord::start().await.expect("the fake Discord starts");
    let cancellation = CancellationToken::new();
    let overrides = Overrides {
        source: Some(Arc::new(StubSource)),
        voice: Some(Arc::new(FakeVoice::new())),
        api_base: Some(fake.api_base.clone()),
    };

    let running = cancellation.clone();
    let config = config(token.path());
    tokio::spawn(async move {
        tokio::select! {
            result = run_with(config, overrides) => {
                if let Err(error) = result {
                    eprintln!("the runtime stopped: {error:?}");
                }
            }
            () = running.cancelled() => {}
        }
    });
    fake.wait_until_ready()
        .await
        .expect("the runtime identified against the fake gateway");
    (fake, cancellation, token)
}

/// Runs one command and returns how it was acknowledged.
///
/// A fresh runtime per command, because two interactions sharing one keeps the
/// assertions racing each other's follow-up traffic.
async fn deferral_for(
    name: &str,
    options: Vec<serde_json::Value>,
) -> crate::fake_discord::Recorded {
    let (fake, cancellation, _token) = start().await;
    let started = fake.requests().len();
    fake.dispatch("INTERACTION_CREATE", fake.command(name, options));
    fake.settle(started + 1)
        .await
        .expect("the command was acknowledged");
    let deferral = fake
        .requests_since(started)
        .into_iter()
        .find(|request| request.callback_type() == Some(5))
        .unwrap_or_else(|| panic!("no deferral among {:#?}", fake.requests_since(started)));
    cancellation.cancel();
    time::sleep(Duration::from_millis(50)).await;
    deferral
}

#[tokio::test]
async fn a_url_answers_the_room() {
    // A URL is played outright, so the answer is the track and the room sees
    // it. Ephemerality is fixed the moment this acknowledgement is sent and
    // cannot be revisited, which is why it is decided before any work happens.
    let deferral = deferral_for(
        "play",
        vec![string_option(
            "query",
            "https://www.youtube.com/watch?v=aaa",
        )],
    )
    .await;
    assert!(
        !deferral.is_ephemeral(),
        "a URL answered only the requester"
    );
}

#[tokio::test]
async fn a_search_answers_only_the_requester() {
    // A search answers with candidates nobody has chosen between yet, so
    // publishing it would paste five of them into the channel to play one.
    let deferral = deferral_for("play", vec![string_option("query", "some song")]).await;
    assert!(
        deferral.is_ephemeral(),
        "a search was published to the room"
    );
}

#[tokio::test]
async fn a_refused_public_command_withdraws_its_placeholder_and_answers_privately() {
    let (fake, cancellation, _token) = start().await;

    // `/skip` answers the room, so its placeholder is already public by the
    // time it fails — and it fails, because nothing is playing.
    let started = fake.requests().len();
    fake.dispatch("INTERACTION_CREATE", fake.command("skip", Vec::new()));
    fake.settle(started + 3)
        .await
        .expect("the refusal was reported");

    let requests = fake.requests_since(started);
    assert!(
        requests.iter().any(|request| request.method == "DELETE"),
        "the public placeholder was left in the channel: {requests:?}"
    );
    let followup = requests
        .iter()
        .rfind(|request| request.method == "POST" && !request.path.ends_with("/callback"))
        .expect("a follow-up was sent");
    assert!(
        followup.is_ephemeral(),
        "the reason a command was refused was published to the room"
    );
    assert!(
        followup.body["content"]
            .as_str()
            .unwrap_or_default()
            .contains("Unable to complete"),
        "unexpected refusal: {followup:?}"
    );

    cancellation.cancel();
    time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn a_lookup_never_answers_the_room() {
    let (fake, cancellation, _token) = start().await;

    let started = fake.requests().len();
    fake.dispatch("INTERACTION_CREATE", fake.command("queue", Vec::new()));
    fake.settle(started + 1)
        .await
        .expect("the deferral arrived");
    let deferral = fake
        .requests_since(started)
        .into_iter()
        .find(|request| request.callback_type() == Some(5))
        .unwrap_or_else(|| panic!("no deferral among {:#?}", fake.requests_since(started)));
    assert!(
        deferral.is_ephemeral(),
        "a queue listing was published to the room"
    );

    cancellation.cancel();
    time::sleep(Duration::from_millis(50)).await;
}
