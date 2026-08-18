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

Dependabot watches `Cargo.lock` and is the alerting path. Nothing else gates on it: Auxide pins
`serenity` and `songbird` exactly, and most advisories that appear are held by one of those two
with no published version to move to, so a build that failed on them would be failing on somebody
else's release schedule.

`cargo deny check advisories` is in the development shell for when a fuller picture is wanted —
it reads the RustSec database directly and reports more than Dependabot does, including
unmaintained crates. It is not wired into CI and there is no ignore list to keep.

When an alert cannot be resolved by updating, dismiss it on the repository's Security tab with the
reason and the dependency that holds it. That keeps the record next to the alert rather than in a
file that has to be pruned, and a later Dependabot alert for the same crate still arrives.

The supported threat model assumes a trusted NixOS administrator, an unprivileged bot service,
configured Discord allowlists, no public observability port, and public unauthenticated YouTube
sources. Cookies, Google/Spotify accounts, DRM-protected sources, and arbitrary executables are not
supported inputs.
