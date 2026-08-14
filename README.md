# Auxide

Auxide is a self-hosted Discord music bot for explicitly allowlisted servers. It is being
rebuilt in Rust with Serenity for Discord interactions and Songbird for current encrypted voice.
Its first real source is public, unauthenticated YouTube audio resolved just in time with yt-dlp;
Spotify is deliberately out of scope.

The production path now includes:

- `/play`, `/queue`, `/skip`, `/stop`, `/shuffle`, and `/now-playing` guild commands;
- ephemeral, single-ack interaction responses and user-bound search-result buttons;
- one bounded actor/state machine per guild with stale-completion protection;
- requester, role, guild, command-channel, and voice-channel authorization;
- bounded yt-dlp/Deno subprocesses and fresh audio URL resolution before playback;
- Songbird voice joining, playback, reconnect/session handling, and DAVE support;
- idle disconnect plus coordinated SIGINT/SIGTERM shutdown;
- structured logs and loopback-only liveness, readiness, and Prometheus endpoints;
- a pinned `x86_64-linux` Nix build, hardened NixOS service, and unprivileged OCI image.

The obsolete Go prototype remains as behavioral history until the Rust voice stack passes in a
private test guild. See [ADR 0001](docs/adr/0001-rust-serenity-songbird.md) for the decision and
[the operator guide](docs/operator-guide.md) for setup and deployment.

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

Unit tests and Nix/OCI builds can be completed without the application token. The remaining
environment-dependent acceptance gate is a private-guild test proving real Discord voice and real
YouTube playback. That test should happen before deleting the Go implementation or treating the
first release as production-ready. The exact procedure is in the operator guide.

## License

MIT. See [LICENSE](LICENSE).
