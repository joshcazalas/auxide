# discord-music-bot

A self-hosted Discord music bot being rebuilt in Rust for one or more explicitly allowlisted
guilds. Serenity handles the Discord gateway and interactions; Songbird handles current Discord
voice, DAVE, and Opus transport. See [ADR 0001](docs/adr/0001-rust-serenity-songbird.md).

This is an active migration. The old Go prototype remains in the repository as behavioral history
until live Rust voice and YouTube playback pass in a private test guild. The production slash-command
bot is not implemented yet.

## Implemented foundation

- Fail-closed guild configuration and token-file loading with bounded secret size.
- Public YouTube search/inspection through a time-, concurrency-, and output-bounded yt-dlp child.
- Fresh audio-only YouTube URL resolution immediately before playback; no durable media download.
- A one-track Serenity/Songbird voice spike for an existing voice channel. It creates no Discord
  resources and registers no application commands.
- A bounded actor/state machine per guild with deterministic queue transitions and stale-callback
  protection.
- Structured logs and a cancellable, bounded HTTP listener for liveness, readiness, and Prometheus
  text metrics.
- A pinned `x86_64-linux` Nix development and package environment containing Rust, libopus, yt-dlp,
  Deno, and FFmpeg.

Spotify is intentionally out of scope. YouTube support is limited to public, unauthenticated,
non-live videos. It does not use cookies, accounts, DRM workarounds, ad bypass instructions, or
cached media files. Set `youtube.enabled = false` to disable the adapter.

## Development

Enter the pinned environment and run the local gates:

```console
nix develop
cargo fmt --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

Copy `config.example.toml` to the ignored `config.toml` and replace its example snowflakes. Store
the Discord token in the configured runtime secret file with permissions limited to the service
account. Do not put the token in TOML, an environment variable, a command argument, or Git.

These commands do not read the Discord token or contact Discord:

```console
cargo run -- --config config.toml check-config
cargo run -- --config config.toml youtube-search "artist and track"
cargo run -- --config config.toml youtube-inspect 'https://www.youtube.com/watch?v=VIDEO_ID'
```

## Private-guild voice gate

The bot application must already be installed in the configured guild and have View Channel,
Connect, and Speak permissions in an existing voice channel. The spike accepts explicit IDs so it
cannot accidentally join another configured guild.

First prove Discord voice with a short local audio file:

```console
cargo run --release -- --config config.toml voice-spike \
  --guild-id GUILD_ID --channel-id VOICE_CHANNEL_ID file ./short-test-audio.opus
```

Then prove the complete YouTube-to-Discord path:

```console
cargo run --release -- --config config.toml voice-spike \
  --guild-id GUILD_ID --channel-id VOICE_CHANNEL_ID youtube \
  'https://www.youtube.com/watch?v=VIDEO_ID'
```

Acceptance criteria:

1. The gateway reports ready and the bot joins only the requested existing channel.
2. A listener hears clean, continuous audio. The YouTube case resolves an audio-only stream and
   does not create a complete media file in the repository.
3. The bot leaves after natural completion and the process exits successfully.
4. Repeating the test while sending SIGINT and SIGTERM stops playback, leaves voice, and exits
   without a lingering process.
5. An unallowlisted guild ID, invalid channel ID, disabled YouTube adapter, over-duration video,
   live stream, and resolver timeout all fail closed with no secret in logs.

Passing this gate is the prerequisite for wiring the player actors to slash commands. It is also
the point after which the obsolete Go implementation can be removed in a separate review.

## Planned MVP

The next implementation stages are:

1. A Discord adapter that registers guild commands only through an explicit admin command and
   acknowledges every interaction exactly once.
2. Authorization against configured guild, command channel, requester, role, and requester voice
   channel.
3. Player coordination for `/play`, `/queue`, `/skip`, `/stop`, `/shuffle`, and `/now-playing`,
   including just-in-time source resolution and idle disconnect.
4. Full graceful shutdown and health-state integration.
5. An unprivileged OCI image, NixOS service module, SBOM, provenance, and release attestations.

Queue state is ephemeral in the MVP. Complete media is not cached; only canonical source identity
and public metadata live in memory.
