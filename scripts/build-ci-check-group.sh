#!/usr/bin/env bash

set -Eeuo pipefail

if [[ $# != 1 || ! "$1" =~ ^(validate|nix|container|nixos|release)$ ]]; then
  echo "usage: $0 {validate|nix|container|nixos|release}" >&2
  exit 2
fi

requested_group="$1"
check_names="$(nix eval --raw .#checks.x86_64-linux --apply 'checks: builtins.concatStringsSep "\n" (builtins.attrNames checks)')"
selected=()
declare -A found=()

while IFS= read -r check_name; do
  case "${check_name}" in
    credential-helper | module-evaluation) check_group=nix ;;
    auxide | oci-image) check_group=container ;;
    nixos-service) check_group=nixos ;;
    release-helpers) check_group=release ;;
    *)
      echo "Flake check '${check_name}' has no CI group." >&2
      exit 1
      ;;
  esac
  found["${check_group}"]=true
  if [[ "${requested_group}" == "${check_group}" ]]; then
    selected+=(".#checks.x86_64-linux.${check_name}")
  fi
done <<< "${check_names}"

for check_group in nix container nixos release; do
  if [[ -z "${found[${check_group}]:-}" ]]; then
    echo "CI group '${check_group}' has no declared checks." >&2
    exit 1
  fi
done

if [[ "${requested_group}" == validate ]]; then
  echo "Every flake check has a CI group."
else
  nix build --no-link --print-build-logs "${selected[@]}"
fi
