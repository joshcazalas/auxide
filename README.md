# Auxide

Auxide is a self-hosted Discord music bot for explicitly allowlisted servers. It is written in
Rust with Serenity for Discord interactions and Songbird for current encrypted voice.
Its first real source is public, unauthenticated YouTube audio resolved just in time with yt-dlp;
Spotify is deliberately out of scope.

The production path now includes:

- `/play`, `/queue`, `/skip`, `/stop`, `/shuffle`, `/now-playing`, `/pause`, `/resume`, and
  `/volume` guild commands;
- channel-visible results for what was queued, skipped, or stopped, with private search
  pickers and private refusals;
- track cards carrying artwork, uploader, duration, and who asked for it;
- playlist links expanded into the queue in one step, bounded by its track limit;
- unprompted notices for the events nobody asked for — a track that could not be played, and
  why Auxide left a voice channel — plus optional per-track announcements;
- one bounded actor/state machine per guild with stale-completion protection;
- requester, role, guild, command-channel, and voice-channel authorization;
- bounded yt-dlp/Deno subprocesses and fresh audio URL resolution before playback;
- Songbird voice joining, playback, reconnect/session handling, and DAVE support;
- a fifteen-minute hold on an emptied queue, an immediate departure from an emptied voice
  channel, and coordinated SIGINT/SIGTERM shutdown;
- structured logs and loopback-only liveness, readiness, and Prometheus endpoints;
- a pinned `x86_64-linux` Nix build, hardened NixOS service, and unprivileged OCI image.

The Go prototype this replaced was deleted once the Rust voice stack played in a private test
guild; `git log` still has it. See [ADR 0001](docs/adr/0001-rust-serenity-songbird.md) for the
decision and [the operator guide](docs/operator-guide.md) for setup and deployment.

## Source policy

YouTube support is limited to public, unauthenticated, non-live videos. Auxide does not accept
cookies, log into Google accounts, bypass DRM or access controls, or keep complete media files.
The adapter can be disabled independently with `youtube.enabled = false`. You are responsible for
using sources and content you are permitted to play; this repository does not claim that technical
extractability grants permission.

## Development

Enter the pinned environment and run all local gates:

```console
nix develop
./scripts/check.sh
./scripts/secret-scan.sh
nix flake check --print-build-logs
```

`config.example.toml` documents every MVP setting. A local `config.toml`, tokens, build outputs,
and release bundles are ignored by Git. These checks do not need a Discord token; only command
registration, the long-running bot, and the explicit voice spike contact Discord.

Useful offline commands:

```console
cargo run -- --config config.toml check-config
cargo run -- --config config.toml youtube-search "artist and track"
cargo run -- --config config.toml youtube-inspect \
  'https://www.youtube.com/watch?v=VIDEO_ID'
cargo run -- --config config.toml youtube-playlist \
  'https://www.youtube.com/playlist?list=PLAYLIST_ID'
```

## Runtime model

The queue is intentionally ephemeral. Each configured guild has an independent serialized actor,
so concurrent interactions cannot mutate a queue out of order. The actor emits playback directives
to a supervised voice worker; source resolution lives behind an interface and never owns Discord
state. There is no database and no durable YouTube cache in the MVP.

At runtime the NixOS service reads two server-local systemd credentials:

- `/var/lib/auxide/config.toml`, a root-owned non-secret configuration that releases never
  overwrite; and
- `/var/lib/auxide/discord-token`, a host-encrypted credential created or rotated only by the
  `auxide-credential` administrator command.

The bot makes outbound Discord, YouTube, and media-CDN connections. It requires no public inbound
port. Observability defaults to `127.0.0.1:9090`.

## Current validation boundary

Unit tests and Nix/OCI builds can be completed without the application token. Everything that
depends on a real gateway, real voice, and real YouTube is proven in a private test guild
instead; the exact procedure is in the operator guide, and it is what a release should be cut
against.

## License

MIT. See [LICENSE](LICENSE).
