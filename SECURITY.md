# Security policy

Auxide is a small self-hosted project and has no bug-bounty program. Please report a suspected
vulnerability privately through GitHub's **Report a vulnerability** feature rather than a public
issue. Do not include a real Discord token, private media URL, server log containing credentials,
or exploit against infrastructure you do not own.

Only the newest release and current `main` branch receive security fixes. A compromised Discord
token should be reset immediately in the Discord Developer Portal and replaced with
`auxide-credential rotate`; removing it from the latest commit is not sufficient if it entered
Git history.

The supported threat model assumes a trusted NixOS administrator, an unprivileged bot service,
configured Discord allowlists, no public observability port, and public unauthenticated YouTube
sources. Cookies, Google/Spotify accounts, DRM-protected sources, and arbitrary executables are not
supported inputs.
