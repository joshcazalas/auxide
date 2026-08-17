# Auxide operator guide

This guide separates durable server setup from the final Discord voice test. Running a NixOS
release never writes or rotates Auxide's local configuration or Discord token.

## 1. Finish the Discord application

Open the application in the [Discord Developer Portal](https://discord.com/developers/applications),
then:

1. On **Bot**, create the bot user if the portal still offers that button. Under **Token**, choose
   **Reset Token**, confirm, and copy the newly displayed value. Discord does not show an existing
   token again; resetting is the normal way to obtain a replacement.
2. Leave privileged intents disabled. Auxide uses only the `Guilds` and `Guild Voice States`
   gateway intents.
3. On **Bot**, turn **Public Bot** off. Auxide serves every server it is installed in, so
   installation is the authorization boundary and this checkbox is the control that holds it:
   with it on, anyone could add Auxide and drive it. Auxide refuses to start in that combination
   rather than let the two settings drift apart, so this is a requirement and not advice.
4. On **Installation**, enable **Guild Install**. Include the `applications.commands` and `bot`
   scopes and grant only **View Channels**, **Connect**, and **Speak** in the intended voice
   channel, plus **Send Messages** in the text channel Auxide should post to.

   **Send Messages** is the only one of the four that is optional. Replies to commands travel on
   the interaction's own token and never need it; it covers the messages nobody asked for — a
   track Auxide had to skip, and why it left a voice channel. Without it those events are visible
   only in the journal, which is how every version before this one behaved.
5. Use the generated installation link while logged into Discord and select the private test
   server. Do not create a public installation or paste the token into chat, Git, TOML, shell
   history, or the Nix store.

Adding Auxide to another server later needs nothing beyond this installation link. No identifier
has to be recorded and no file has to be edited.

Copy IDs only if you intend to *restrict* a server, or for the voice-channel argument the
acceptance test takes. Enable Discord Developer Mode and use **Copy ID**; these snowflakes are
identifiers, not credentials.

## 2. Import the NixOS module

Add Auxide as a flake input and import its module in the homeserver configuration:

```nix
inputs.auxide = {
  url = "github:joshcazalas/auxide";
  inputs.nixpkgs.follows = "nixpkgs";
};

modules = [ inputs.auxide.nixosModules.default ];
```

Enable it when you are ready to provision the two local runtime files from the next section:

```nix
services.auxide = {
  enable = true;
  memoryMax = "1G";
};
```

The service is unprivileged, has no public listener, receives read-only credentials through
systemd, and gets a private runtime/cache directory. YouTube is streamed rather than downloaded,
so the dying `/srv` disk is not involved. A future NAS source can be mounted read-only from
`/var/lib/homelab/media` without changing the YouTube path.

## 3. Create persistent server-local state

Create the configuration before the first switch if you already have the package available, or
immediately after the first switch. The initial service start is expected to fail closed while a
credential is absent; provisioning it and restarting the unit is sufficient.

```console
sudo install -d -m 0700 -o root -g root /var/lib/auxide
sudoedit /var/lib/auxide/config.toml
sudo chmod 0600 /var/lib/auxide/config.toml
sudo chown root:root /var/lib/auxide/config.toml
```

Start from `config.example.toml`. The only line that must be right is the token path:

```toml
[discord]
token_file = "/run/credentials/auxide.service/discord-token"
```

No server needs to be listed. `allow_all_guilds` defaults to true, so Auxide answers every server
it is installed in. Add a `[[discord.guilds]]` block only to narrow one particular server, which
leaves every other server on the permissive defaults; set `allow_all_guilds = false` to make those
blocks the complete allowlist instead.

Editing this file has no effect on a running service. `LoadCredential=` copies it into the unit's
credential directory at start, so a change needs `sudo systemctl restart auxide.service`.

Then run the credential helper. It prompts through `systemd-ask-password`, encrypts with this
machine's systemd host key, verifies the ciphertext, and atomically installs it. The token is never
placed in an environment variable, command argument, Nix expression, or plaintext disk file.

```console
sudo auxide-credential set
sudo auxide-credential status
```

Use `sudo auxide-credential rotate` only after deliberately resetting the token in Discord.
Host-encrypted credentials are machine-bound; keep the application recovery process, not a copy of
the plaintext token, as the disaster-recovery path.

## 4. Validate without joining voice

Validate the server-local configuration as the same systemd credentials the service will receive:

```console
sudo systemd-run --wait --pipe --collect \
  --unit=auxide-admin \
  --property=LoadCredential=auxide-config:/var/lib/auxide/config.toml \
  --property=LoadCredentialEncrypted=discord-token:/var/lib/auxide/discord-token \
  auxide --config /run/credentials/auxide-admin.service/auxide-config check-config
```

Register the commands once, then start the service. Registration is global, so a server added
later needs no repeat:

```console
sudo systemd-run --wait --pipe --collect \
  --unit=auxide-admin \
  --property=LoadCredential=auxide-config:/var/lib/auxide/config.toml \
  --property=LoadCredentialEncrypted=discord-token:/var/lib/auxide/discord-token \
  auxide --config /run/credentials/auxide-admin.service/auxide-config register-commands
sudo systemctl enable --now auxide.service
```

Normal startup never creates or mutates commands. Repeat `register-commands` only after command
definitions change; Discord can take up to an hour to propagate such a change, though a newly
installed server receives the current set at once.

If a server previously carried per-server commands, they survive alongside the global set and
appear as duplicates. `register-commands` clears them for every server still named in
configuration; pass `--guild-id ID` for one that is not.

Check the service and its private observability listener:

```console
systemctl status auxide.service
journalctl -u auxide.service -f
curl --fail http://127.0.0.1:9090/health/live
curl --fail http://127.0.0.1:9090/health/ready
curl --fail http://127.0.0.1:9090/metrics
```

## 5. Private-guild acceptance gate

Run this against a private test server after any change to resolution, voice, or the session
lifecycle. Start with a short known-good local audio file as a control:

```console
auxide --config config.toml voice-spike \
  --guild-id GUILD_ID --channel-id VOICE_CHANNEL_ID file ./short-test-audio.opus
```

Then prove just-in-time YouTube resolution and encrypted Discord playback:

```console
auxide --config config.toml voice-spike \
  --guild-id GUILD_ID --channel-id VOICE_CHANNEL_ID youtube \
  'https://www.youtube.com/watch?v=VIDEO_ID'
```

Finally exercise `/play` with both a URL and search terms, choose an interactive result, enqueue
concurrently, and run every control command. Acceptance requires:

1. the bot joins only the requester's channel and unauthorized guilds/channels/users fail closed;
2. audio is continuous and no complete media or orphaned yt-dlp/FFmpeg process remains;
3. queue order is deterministic, stale completion events do not skip replacements, and each
   interaction receives one response;
4. what was queued, skipped, stopped, or shuffled is visible to the whole channel, while search
   results and refusals reach only the person who asked, and a queued track shows its artwork,
   uploader, duration, and requester;
5. a track that cannot be resolved names itself and its reason in the channel rather than only in
   the journal, and revoking **Send Messages** degrades that to a logged warning without
   interrupting playback;
6. an emptied queue holds the voice channel for `playback.idle_timeout_seconds` and any track
   queued inside that window cancels the pending disconnect, while `/stop` and the departure of
   the last person in the voice channel disconnect immediately, and each of the two departures
   Auxide decides on for itself says why in the channel;
7. `/pause` holds a track and starts the same countdown, `/resume` cancels it, and `/volume`
   changes the level of what is playing and of everything queued behind it;
8. a playlist link queues its tracks in one step and one message, stops at
   `playback.max_queue_length`, says how many it left out, and a `watch?v=…&list=…` link still
   queues only the one video;
9. `/repeat single` replays a track but still lets `/skip` move on, `/repeat all` cycles without
   ever reaching the idle hold, `/shuffle` picks at random from then on while `/shuffle once`
   only reorders what is waiting, and `/stop` forgets both;
10. `/clear` and `/remove` change only what is waiting — the current track keeps playing and the
    channel is kept — and a position printed on any page of `/queue` is the position `/remove`
    takes;
11. `/skip` alone needs half the channel to agree before cutting somebody else's track short,
    while the person who queued it may always skip their own, and `/skip tracks:` and
    `/skip requester:` take waiting tracks out without touching what is playing; and
12. `/history` lists what already played newest first and `/history replay:` queues one again
    without resolving anything, while `/export` and `/import` round-trip a queue through a file
    whose links are re-checked on the way back in; and
13. SIGINT/SIGTERM, Discord reconnects, resolver errors, over-duration videos, and live videos leave
   the process in a clean, usable state.
