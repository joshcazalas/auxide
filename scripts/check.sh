#!/usr/bin/env bash

set -Eeuo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

usage() {
  cat >&2 <<'EOF'
usage: check.sh [all|lint|rust]

  all   every check (default)
  lint  Rust and Nix formatting, Nix, shell, and workflow linters
  rust  Rust tests and Clippy

CI runs lint and rust as separate jobs so a four-second linter failure does not
wait behind a four-minute Rust build. Run it with no argument locally.
EOF
}

check_lint() {
  echo "==> Checking Rust formatting"
  cargo fmt --check

  echo "==> Checking Nix formatting"
  nix fmt -- --ci .

  echo "==> Checking Nix for dead code"
  deadnix --fail .

  echo "==> Linting Nix"
  statix check .

  echo "==> Linting shell scripts"
  mapfile -d '' shell_files < <(
    find scripts nix .githooks -type f \( -name '*.sh' -o -perm -u+x \) -print0
  )
  shellcheck "${shell_files[@]}"

  echo "==> Linting GitHub Actions workflows"
  actionlint

  # Here rather than with the Rust job because it needs no build: it reads
  # Cargo.lock against the advisory database, in about a second. What it is for
  # is the advisory nobody has looked at yet — the ones already weighed up are
  # listed in deny.toml with what holds them and what would release them.
  echo "==> Checking dependencies against the advisory database"
  cargo deny check advisories
}

check_rust() {
  echo "==> Running Rust tests"
  cargo test --all-targets

  echo "==> Running strict Rust lints"
  cargo clippy --all-targets -- -D warnings
}

if (($# > 1)); then
  usage
  exit 2
fi

case "${1:-all}" in
all)
  check_lint
  check_rust
  ;;
lint)
  check_lint
  ;;
rust)
  check_rust
  ;;
*)
  usage
  exit 2
  ;;
esac
