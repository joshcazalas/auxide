//! A Discord that does not exist.
//!
//! Nothing can drive Auxide the way a person does — a bot cannot invoke another
//! bot's slash command, and automating a user account is a bannable offence
//! Discord does not exempt for testing. So the closest a test can get to
//! watching the real thing work is to run the real runtime, with its real
//! gateway handling and real command dispatch, against a Discord made of two
//! loopback sockets.
//!
//! Serenity needs no patching for this. It asks where the gateway lives over
//! the same HTTP it can be told to send elsewhere, so redirecting its API is
//! enough to redirect its websocket too.

// A test double answers to different rules than an API: a poisoned lock means a
// test panicked while holding it, which the panic already reports, and JSON
// builders read better taking the values they assemble.
#![allow(
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    clippy::unused_self
)]

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context as _, Result};
use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::{Notify, broadcast},
    time,
};

/// One request Auxide made, reduced to what an assertion cares about.
#[derive(Clone, Debug)]
pub struct Recorded {
    pub method: String,
    pub path: String,
    pub body: Value,
}

impl Recorded {
    /// Reports whether this is a response to an interaction, and of which type.
    ///
    /// Type 5 is a deferred reply; the ephemeral flag rides in `data.flags`.
    #[must_use]
    pub fn callback_type(&self) -> Option<u64> {
        self.path
            .ends_with("/callback")
            .then(|| self.body.get("type")?.as_u64())
            .flatten()
    }

    /// Reports whether the message this carries was marked ephemeral.
    ///
    /// Discord's ephemeral flag is bit six, and it is the whole difference
    /// between an answer the room sees and one only the requester does.
    #[must_use]
    pub fn is_ephemeral(&self) -> bool {
        let flags = self
            .body
            .get("data")
            .and_then(|data| data.get("flags"))
            .or_else(|| self.body.get("flags"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        flags & 64 != 0
    }
}

#[derive(Default)]
struct Recordings {
    requests: Vec<Recorded>,
}

/// A stand-in for Discord's HTTP API and gateway, on two loopback ports.
pub struct FakeDiscord {
    /// What to hand [`crate::discord::Overrides::api_base`].
    pub api_base: String,
    pub application_id: u64,
    pub bot_user_id: u64,
    recordings: Arc<Mutex<Recordings>>,
    recorded: Arc<Notify>,
    /// Payloads to push down the gateway, once a shard is listening.
    events: broadcast::Sender<String>,
    ready: Arc<Notify>,
}

impl FakeDiscord {
    /// Binds both sockets and starts serving.
    ///
    /// # Errors
    ///
    /// Returns an error if either loopback listener cannot be bound.
    pub async fn start() -> Result<Arc<Self>> {
        let api = TcpListener::bind("127.0.0.1:0").await?;
        let gateway = TcpListener::bind("127.0.0.1:0").await?;
        let api_address: SocketAddr = api.local_addr()?;
        let gateway_address: SocketAddr = gateway.local_addr()?;
        let (events, _) = broadcast::channel(64);

        let fake = Arc::new(Self {
            api_base: format!("http://{api_address}"),
            application_id: 1_000,
            bot_user_id: 1_000,
            recordings: Arc::new(Mutex::new(Recordings::default())),
            recorded: Arc::new(Notify::new()),
            events,
            ready: Arc::new(Notify::new()),
        });

        let rest = Arc::clone(&fake);
        let gateway_url = format!("ws://{gateway_address}");
        tokio::spawn(async move {
            while let Ok((stream, _)) = api.accept().await {
                let rest = Arc::clone(&rest);
                let gateway_url = gateway_url.clone();
                tokio::spawn(async move {
                    let _ = rest.serve_api(stream, &gateway_url).await;
                });
            }
        });

        let socket = Arc::clone(&fake);
        tokio::spawn(async move {
            while let Ok((stream, _)) = gateway.accept().await {
                let socket = Arc::clone(&socket);
                tokio::spawn(async move {
                    let _ = socket.serve_gateway(stream).await;
                });
            }
        });
        Ok(fake)
    }

    #[must_use]
    pub fn requests(&self) -> Vec<Recorded> {
        self.recordings
            .lock()
            .expect("the recordings lock is not poisoned")
            .requests
            .clone()
    }

    /// Waits until a shard has identified and been given a guild.
    ///
    /// # Errors
    ///
    /// Returns an error if no shard connects in time, which means the runtime
    /// failed to start rather than that it was slow.
    pub async fn wait_until_ready(&self) -> Result<()> {
        time::timeout(Duration::from_secs(20), self.ready.notified())
            .await
            .with_context(|| {
                let seen = self
                    .requests()
                    .iter()
                    .map(|request| format!("{} {}", request.method, request.path))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("no shard identified against the fake gateway; saw [{seen}]")
            })
    }

    /// Everything recorded after `from`, which is how a test ignores startup.
    #[must_use]
    pub fn requests_since(&self, from: usize) -> Vec<Recorded> {
        self.requests().split_off(from.min(self.requests().len()))
    }

    /// Waits until at least `count` requests have been recorded.
    ///
    /// # Errors
    ///
    /// Returns an error if they do not arrive, with what did arrive attached.
    pub async fn settle(&self, count: usize) -> Result<()> {
        let wait = async {
            while self.requests().len() < count {
                self.recorded.notified().await;
            }
        };
        time::timeout(Duration::from_secs(20), wait)
            .await
            .with_context(|| {
                let seen = self
                    .requests()
                    .iter()
                    .map(|request| format!("{} {}", request.method, request.path))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("expected {count} request(s); saw [{seen}]")
            })
    }

    /// Pushes a gateway event to whichever shard is connected.
    pub fn dispatch(&self, name: &str, data: Value) {
        let _ = self
            .events
            .send(json!({"op": 0, "t": name, "s": 1, "d": data}).to_string());
    }

    fn record(&self, request: Recorded) {
        self.recordings
            .lock()
            .expect("the recordings lock is not poisoned")
            .requests
            .push(request);
        self.recorded.notify_waiters();
    }

    async fn serve_api(&self, mut stream: TcpStream, gateway_url: &str) -> Result<()> {
        let mut raw = Vec::new();
        let mut headers_end = None;
        // Read until the headers are complete, then until Content-Length says
        // the body is too.
        loop {
            let mut chunk = [0_u8; 4096];
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            raw.extend_from_slice(&chunk[..read]);
            if headers_end.is_none() {
                headers_end = raw
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|at| at + 4);
            }
            if let Some(start) = headers_end {
                let head = String::from_utf8_lossy(&raw[..start]).to_lowercase();
                let expected = head
                    .split("content-length:")
                    .nth(1)
                    .and_then(|rest| rest.split("\r\n").next())
                    .and_then(|value| value.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                if raw.len() >= start + expected {
                    break;
                }
            }
        }

        let start = headers_end.unwrap_or(raw.len());
        let head = String::from_utf8_lossy(&raw[..start]).to_string();
        let mut parts = head.split_whitespace();
        let method = parts.next().unwrap_or_default().to_owned();
        let path = parts.next().unwrap_or_default().to_owned();
        let body = serde_json::from_slice(&raw[start..]).unwrap_or(Value::Null);

        let response = self.answer(&method, &path, gateway_url);
        self.record(Recorded { method, path, body });

        let payload = response.to_string();
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                    payload.len()
                )
                .as_bytes(),
            )
            .await?;
        stream.shutdown().await?;
        Ok(())
    }

    fn answer(&self, method: &str, path: &str, gateway_url: &str) -> Value {
        if path.ends_with("/gateway") || path.contains("/gateway/bot") {
            return json!({"url": gateway_url, "shards": 1,
                "session_start_limit": {"total": 1000, "remaining": 999, "reset_after": 0, "max_concurrency": 1}});
        }
        if path.ends_with("/users/@me") {
            return user(self.bot_user_id, "auxide", true);
        }
        if path.ends_with("/oauth2/applications/@me") {
            return json!({
                "id": self.application_id.to_string(),
                "name": "Auxide under test",
                "icon": null,
                "description": "",
                // The boundary check refuses a public application, so the
                // runtime only starts at all when this is false.
                "bot_public": false,
                "bot_require_code_grant": false,
                "verify_key": "0".repeat(64),
                "owner": user(2_000, "owner", false),
                "team": null,
                "summary": "",
                "flags": 0
            });
        }
        if method == "DELETE" {
            return Value::Null;
        }
        // Everything else is a message being sent; the shape of the reply does
        // not matter to any assertion, only that it deserialises.
        json!({
            "id": "9001",
            "channel_id": "99",
            "author": user(self.bot_user_id, "auxide", true),
            "content": "",
            "timestamp": "2026-08-17T00:00:00.000000+00:00",
            "edited_timestamp": null,
            "tts": false,
            "mention_everyone": false,
            "mentions": [],
            "mention_roles": [],
            "attachments": [],
            "embeds": [],
            "pinned": false,
            "type": 0
        })
    }

    async fn serve_gateway(&self, stream: TcpStream) -> Result<()> {
        let socket = tokio_tungstenite::accept_async(stream).await?;
        let (mut sink, mut source) = socket.split();
        let mut events = self.events.subscribe();

        sink.send(
            json!({"op": 10, "d": {"heartbeat_interval": 45_000}})
                .to_string()
                .into(),
        )
        .await?;

        loop {
            tokio::select! {
                incoming = source.next() => {
                    let Some(Ok(message)) = incoming else { return Ok(()) };
                    let Ok(text) = message.into_text() else { continue };
                    let Ok(payload) = serde_json::from_str::<Value>(&text) else { continue };
                    match payload.get("op").and_then(Value::as_u64) {
                        // Identify: answer with a session and a guild, so the
                        // cache knows the channel a requester is sitting in.
                        Some(2) => {
                            sink.send(self.ready_payload().to_string().into()).await?;
                            sink.send(json!({"op": 0, "t": "GUILD_CREATE", "s": 2, "d": self.guild()})
                                .to_string().into()).await?;
                            self.ready.notify_waiters();
                        }
                        Some(1) => sink.send(json!({"op": 11}).to_string().into()).await?,
                        _ => {}
                    }
                }
                pushed = events.recv() => {
                    let Ok(pushed) = pushed else { continue };
                    sink.send(pushed.into()).await?;
                }
            }
        }
    }

    fn ready_payload(&self) -> Value {
        json!({"op": 0, "t": "READY", "s": 1, "d": {
            "v": 10,
            "user": user(self.bot_user_id, "auxide", true),
            "guilds": [],
            "session_id": "fake-session",
            "resume_gateway_url": "ws://127.0.0.1:1",
            "shard": [0, 1],
            "application": {"id": self.application_id.to_string(), "flags": 0}
        }})
    }

    /// One guild, one voice channel, and one person already sitting in it.
    fn guild(&self) -> Value {
        json!({
            "id": GUILD_ID.to_string(),
            "name": "Test Server",
            "icon": null,
            "splash": null,
            "discovery_splash": null,
            "owner_id": REQUESTER_ID.to_string(),
            "afk_channel_id": null,
            "afk_timeout": 300,
            "verification_level": 0,
            "roles": [],
            "emojis": [],
            "features": [],
            "mfa_level": 0,
            "application_id": null,
            "system_channel_id": null,
            "system_channel_flags": 0,
            "rules_channel_id": null,
            "explicit_content_filter": 0,
            "default_message_notifications": 0,
            "premium_tier": 0,
            "preferred_locale": "en-US",
            "public_updates_channel_id": null,
            "nsfw_level": 0,
            "stickers": [],
            "premium_progress_bar_enabled": false,
            "joined_at": "2026-08-17T00:00:00.000000+00:00",
            "large": false,
            "unavailable": false,
            "member_count": 2,
            "voice_states": [{
                "channel_id": VOICE_CHANNEL_ID.to_string(),
                "user_id": REQUESTER_ID.to_string(),
                "session_id": "requester-session",
                "deaf": false, "mute": false, "self_deaf": false,
                "self_mute": false, "self_video": false, "suppress": false,
                "request_to_speak_timestamp": null
            }],
            "members": [],
            "channels": [],
            "threads": [],
            "presences": [],
            "guild_scheduled_events": []
        })
    }

    /// An interaction as Discord would deliver it.
    pub fn command(&self, name: &str, options: Vec<Value>) -> Value {
        json!({
            "id": "500",
            "application_id": self.application_id.to_string(),
            "type": 2,
            "token": "interaction-token",
            "version": 1,
            "guild_id": GUILD_ID.to_string(),
            "channel_id": TEXT_CHANNEL_ID.to_string(),
            "locale": "en-US",
            "guild_locale": null,
            "app_permissions": "0",
            // Required outright by serenity's model, and absent from the
            // payload Discord's own documentation shows.
            "entitlements": [],
            "attachment_size_limit": 26_214_400,
            "channel": null,
            "context": null,
            "user": user(REQUESTER_ID, "requester", false),
            "member": {
                "user": user(REQUESTER_ID, "requester", false),
                "roles": [],
                "joined_at": "2026-08-17T00:00:00.000000+00:00",
                "deaf": false, "mute": false, "flags": 0
            },
            "data": {"id": "600", "name": name, "type": 1, "options": options}
        })
    }
}

fn user(id: u64, name: &str, bot: bool) -> Value {
    json!({
        "id": id.to_string(),
        "username": name,
        "discriminator": "0001",
        "avatar": null,
        "bot": bot
    })
}

pub const GUILD_ID: u64 = 730_675_093_197_422_623;
pub const TEXT_CHANNEL_ID: u64 = 111;
pub const VOICE_CHANNEL_ID: u64 = 222;
pub const REQUESTER_ID: u64 = 333;

/// A string option, as Discord sends one.
#[must_use]
pub fn string_option(name: &str, value: &str) -> Value {
    json!({"name": name, "type": 3, "value": value})
}

/// Groups recorded requests by path, for assertions that do not care about order.
#[must_use]
pub fn by_path(requests: &[Recorded]) -> HashMap<String, Vec<Recorded>> {
    let mut grouped: HashMap<String, Vec<Recorded>> = HashMap::new();
    for request in requests {
        grouped
            .entry(request.path.clone())
            .or_default()
            .push(request.clone());
    }
    grouped
}
