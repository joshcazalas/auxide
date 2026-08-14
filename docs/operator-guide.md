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
3. On **Installation**, enable **Guild Install**. Include the `applications.commands` and `bot`
   scopes and grant only **View Channels**, **Connect**, and **Speak** in the intended voice channel.
4. Use the generated installation link while logged into Discord and select the private test
   server. Do not create a public installation or paste the token into chat, Git, TOML, shell
   history, or the Nix store.

Record the server, command-channel, voice-channel, authorized-user, and optional role IDs by
enabling Discord Developer Mode and choosing **Copy ID**. These snowflakes are identifiers, not
credentials.

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

Start from `config.example.toml`, replace the example Discord IDs, and keep this exact token path:

```toml
[discord]
token_file = "/run/credentials/auxide.service/discord-token"
```

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

Register the guild-scoped commands once, then start the service:

```console
sudo systemd-run --wait --pipe --collect \
  --unit=auxide-admin \
  --property=LoadCredential=auxide-config:/var/lib/auxide/config.toml \
  --property=LoadCredentialEncrypted=discord-token:/var/lib/auxide/discord-token \
  auxide --config /run/credentials/auxide-admin.service/auxide-config register-commands
sudo systemctl enable --now auxide.service
```

Normal startup never creates or mutates commands. Repeat `register-commands` only after command
definitions change.

Check the service and its private observability listener:

```console
systemctl status auxide.service
journalctl -u auxide.service -f
curl --fail http://127.0.0.1:9090/health/live
curl --fail http://127.0.0.1:9090/health/ready
curl --fail http://127.0.0.1:9090/metrics
```

## 5. Deferred private-guild acceptance gate

When you are ready to test, first use a short known-good local audio file as a control:

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
4. natural completion idles before disconnect, while stop and an empty final skip disconnect
   immediately; and
5. SIGINT/SIGTERM, Discord reconnects, resolver errors, over-duration videos, and live videos leave
   the process in a clean, usable state.

Do not remove the Go prototype until this gate passes.
