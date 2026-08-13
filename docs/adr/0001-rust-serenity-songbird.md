# ADR 0001: Rust, Serenity, Songbird, and isolated media sources

- Status: accepted
- Date: 2026-08-13

## Context

Discord requires the DAVE end-to-end encryption protocol for ordinary voice calls. The existing Go prototype depends on the 2021 `dgvoice` proof of concept and cannot meet that requirement. The bot also needs deterministic per-guild queues, bounded resource use, supervised media processes, and an optional source architecture that does not couple playback state to yt-dlp.

YouTube playback is a Phase 1 product requirement. Spotify playback is explicitly out of scope because its official APIs do not provide audio bytes for relay to Discord.

## Decision

The replacement is written in Rust using Serenity for Discord gateway, REST, and interactions, and Songbird for DAVE-capable Discord voice and Opus playback.

Each active guild will have one actor task that exclusively owns its queue, current track, voice-channel identity, cancellation state, and idle timer. Shared maps contain actor handles only.

Audio sources implement source-domain interfaces and do not depend on Discord types. Phase 1 sources are:

1. A local file used for deterministic voice diagnostics.
2. Public, unauthenticated YouTube videos resolved by a pinned yt-dlp, matching yt-dlp-ejs, and Deno toolchain.

YouTube queues retain canonical video identity and metadata, never temporary media URLs. A fresh audio-only URL is resolved immediately before playback and streamed through bounded buffers. Complete YouTube media files are not cached.

Discord command registration is an explicit administrative operation. Normal startup will not create channels or register, replace, or delete commands.

## Consequences

- The Go prototype remains available as behavioral history until the DAVE and YouTube spikes pass.
- Native libopus and the yt-dlp/Deno toolchain increase image size and build complexity.
- YouTube extraction is expected to require regular dependency updates and may fail independently of the bot.
- The player remains usable with local or future authorized sources if YouTube extraction breaks.
- Queue state is intentionally ephemeral for the MVP.
