#!/usr/bin/env bash

set -Eeuo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

echo "==> Checking Rust formatting"
cargo fmt --check

echo "==> Running Rust tests"
cargo test --all-targets

echo "==> Running strict Rust lints"
cargo clippy --all-targets -- -D warnings

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
