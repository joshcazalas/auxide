# Security policy

Auxide is a small self-hosted project and has no bug-bounty program. Please report a suspected
vulnerability privately through GitHub's **Report a vulnerability** feature rather than a public
issue. Do not include a real Discord token, private media URL, server log containing credentials,
or exploit against infrastructure you do not own.

Only the newest release and current `main` branch receive security fixes. A compromised Discord
token should be reset immediately in the Discord Developer Portal and replaced with
`auxide-credential rotate`; removing it from the latest commit is not sufficient if it entered
Git history.

## Advisories against dependencies

`cargo deny check advisories` runs on every push, as part of the lint job, and reads `Cargo.lock`
against the RustSec database. A new advisory fails the build.

Advisories that cannot be resolved by updating are listed in `deny.toml`, each with what holds the
vulnerable crate at its version, what would release it, and why it is tolerable meanwhile. That
file is the record of what has been weighed up; anything absent from it has not been, which is why
its absence is what breaks the build rather than its presence. Auxide pins `serenity` and
`songbird` exactly, so most of what appears there is waiting on one of those two.

Treat an entry that names a released fix as work to do, not as a settled decision.

The supported threat model assumes a trusted NixOS administrator, an unprivileged bot service,
configured Discord allowlists, no public observability port, and public unauthenticated YouTube
sources. Cookies, Google/Spotify accounts, DRM-protected sources, and arbitrary executables are not
supported inputs.
