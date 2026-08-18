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

### The proof-of-origin token provider

Enabling Auxide also starts a small container beside it, bound to `127.0.0.1:4416`. This is not
optional in practice. YouTube hands almost any client the first megabyte of a track and refuses
the rest; without a token to present, a song stops about a minute in, the journal fills with
`403 Forbidden` on media chunks, and the channel is told nothing. A megabyte is roughly a minute
of Opus, so short clips play to the end and everything else does not — which is what makes the
failure look intermittent rather than total.

It needs a container backend. `virtualisation.oci-containers.backend` defaults to `podman`; set
it to `docker` if that is what the host already runs.

```nix
services.auxide.poTokenProvider = {
  enable = true;          # the default
  port = 4416;            # loopback only, never published
};
```

Two settings in Auxide's own configuration pair with it, and **neither works alone** — see
`youtube.player_clients` and `youtube.po_token_base_url` in `config.example.toml`. Both have
working defaults, so an existing configuration file needs no edit.

When tracks start stopping partway again, this pairing is the first thing to check, and
`youtube.player_clients` is the setting to change. YouTube decides per client what it will serve
and what it demands first; when it tightens the one named here, naming a different one and
restarting is the repair. Confirm what yt-dlp is doing before changing anything else:

```bash
yt-dlp -v --js-runtimes deno --skip-download -- VIDEO_ID 2>&1 | grep -i 'pot\]'
```

`PO Token Providers: none` means the plugin is not being found. `PO Token Providers:
bgutil:http-…` means it is, and the next suspect is the provider itself:

```bash
curl -s http://127.0.0.1:4416/ping
```

These lines are `[debug]`, so they appear only under `-v`. Their absence without that flag says
nothing at all — a point that cost a full afternoon of misdiagnosis once already.

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

### Who may run which command

There are two layers here, and they answer different questions. Using the wrong one is the usual
reason somebody cannot work out why a command was refused.

**Discord decides who in a server may run what.** In **Server Settings → Integrations → Auxide**,
a server administrator can restrict any command to particular roles, particular members, or
particular channels — `/stop` to a DJ role, everything to one channel, whatever suits that
server. This needs nothing from you: Auxide registers its commands open precisely so that this
editor is the thing that narrows them. It takes effect immediately, it is per command, and
Discord keeps the audit log.

**This file decides which servers and which people may drive the bot at all.** It is the host
owner's boundary, not the server's, and it is deliberately blunt: `authorized_role_ids` and
`authorized_user_ids` gate *every* command at once, and changing them needs a root edit and
`systemctl restart auxide.service`.

So in almost every case, leave `authorized_role_ids` and `authorized_user_ids` empty and let each
server narrow itself. Reach for them only when you want a limit a server administrator cannot lift
— which is also why a refusal from this layer says so explicitly, rather than leaving somebody
looking for a Discord setting that was never involved.

The same applies to `command_channel_ids`. Discord can already restrict a command to a channel;
this exists for the case where you want that guaranteed from outside the server.

Editing this file has no effect on a running service. `LoadCredential=` copies it into the unit's
credential directory at start, so a change needs `sudo systemctl restart auxide.service`. There is
no reload path, and that is deliberate: a running process should not have its credentials change
underneath it.

The cost of that is a failure mode worth knowing, because it looks like a bug in a file that is
correct. A command refused with *"Auxide's own configuration does not list you as permitted"*, or
a setting that appears to do nothing, is usually the service still running on an older copy. So
Auxide opens its journal with what it actually loaded:

```console
journalctl -u auxide.service | grep "loaded configuration" | tail -2
```

```
loaded configuration configuration=/run/credentials/auxide.service/auxide-config
  allow_all_guilds=true configured_guilds=1 max_guilds=50 idle_timeout_seconds=900
  starting_volume_percent=50 max_queue_length=100 youtube_enabled=true
loaded settings for one server guild_id=730675093197422623 command_channels=1
  authorized_roles=0 authorized_users=0 announce_channel_id=None announce_tracks=true
```

The path on that first line is the copy, not the file you edited. If what follows disagrees with
what is on disk, the answer is a restart rather than a change. Every value there is a setting or a
snowflake; the token is read separately and never appears.

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

That prints the same description the service logs, read from the same credential, so comparing the
two is how you confirm a running service is on the configuration you think it is.

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
   the last person leaving pauses rather than ending the session — lifting when anybody comes
   back, and expiring into a departure that says why if nobody does;
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
13. `/seek`, `/forward`, `/rewind`, and `/restart` are not offered — Discord's picker does not
    list them and `/help` does not describe them. They are built and withdrawn: moving the
    playhead could trip an assertion inside the Matroska reader, on a mixer thread, and take
    the process down with it. A queue that always plays is worth more than a playhead that
    sometimes moves; and
14. typing into `/play` offers suggestions without ever making a keystroke wait, choosing one
    queues it as a link rather than a fresh search, and a burst of typing never leaves a track
    waiting behind it; and
15. `/join` brings Auxide in and picks a parked queue back up, restarting its current track
    because the connection it was playing through is gone, and `/leave` gives up the channel
    while keeping the queue for the length of the idle hold; and
16. `/help` lists every command Discord was given, grouped, with the rules a newcomer would
    otherwise have to discover — who sees which answer, that an emptied queue waits, and that
    skipping somebody else's track needs agreement; and
17. restricting a command to a role in **Server Settings → Integrations** takes effect without a
    restart, a refusal from the host's own allowlist says that Discord was not involved, and no
    command is offered in direct messages; and
18. SIGINT/SIGTERM, Discord reconnects, resolver errors, over-duration videos, and live videos leave
   the process in a clean, usable state.
