#!/usr/bin/env bash

set -Eeuo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 RELEASE_TAG ARTIFACT_DIRECTORY" >&2
  exit 2
fi

release_tag="$1"
artifact_directory="$2"

if [[ ! -d "${artifact_directory}" ]]; then
  echo "Artifact directory does not exist: ${artifact_directory}" >&2
  exit 1
fi

if [[ ! -f "${artifact_directory}/SHA256SUMS" ]]; then
  echo "Release checksum manifest is missing: ${artifact_directory}/SHA256SUMS" >&2
  exit 1
fi

echo "==> Verifying the release bundle before publication"
(
  cd "${artifact_directory}"
  sha256sum --check SHA256SUMS
)

mapfile -d '' assets < <(
  find "${artifact_directory}" -maxdepth 1 -type f -print0 | sort -z
)

if [[ ${#assets[@]} -eq 0 ]]; then
  echo "No release artifacts found in ${artifact_directory}" >&2
  exit 1
fi

if gh release view "${release_tag}" >/dev/null 2>&1; then
  is_draft="$(gh release view "${release_tag}" --json isDraft --jq .isDraft)"
  if [[ "${is_draft}" != true ]]; then
    echo "Release ${release_tag} is already published; leaving it immutable."
    exit 0
  fi

  gh release upload "${release_tag}" "${assets[@]}" --clobber
else
  gh release create "${release_tag}" \
    "${assets[@]}" \
    --draft \
    --target "${GITHUB_SHA}" \
    --title "${release_tag}" \
    --notes-file "${artifact_directory}/RELEASE_NOTES.md"
fi

gh release edit "${release_tag}" --draft=false --latest
