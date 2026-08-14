#!/usr/bin/env bash

set -Eeuo pipefail

repo_root="$(git rev-parse --show-toplevel)"
scan_root="$(mktemp -d)"
cleanup() {
  rm -rf -- "${scan_root}"
}
trap cleanup EXIT

echo "==> Scanning the current worktree with Gitleaks"
git -C "${repo_root}" ls-files --cached --others --exclude-standard -z \
  | tar --directory="${repo_root}" --null --files-from=- --create --file=- \
  | tar --extract --file=- --directory="${scan_root}"
gitleaks dir \
  --no-banner \
  --redact=100 \
  --config "${repo_root}/.gitleaks.toml" \
  "${scan_root}"

echo "==> Scanning every reachable commit with Gitleaks"
gitleaks git \
  --no-banner \
  --redact=100 \
  --config "${repo_root}/.gitleaks.toml" \
  --log-opts='--all' \
  "${repo_root}"
