#!/usr/bin/env bash

set -Eeuo pipefail

usage() {
  echo "usage: $0 RELEASE_TAG [OUTPUT_DIRECTORY]" >&2
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 2
fi

release_tag="$1"
repo_root="$(git rev-parse --show-toplevel)"
output_directory="${2:-${repo_root}/.release/${release_tag}}"

if [[ "${output_directory}" != /* ]]; then
  output_directory="${repo_root}/${output_directory}"
fi

if [[ -n "$(git -C "${repo_root}" status --porcelain --untracked-files=no)" ]]; then
  echo "Refusing to describe a release from a dirty tracked worktree." >&2
  exit 1
fi

mkdir -p "${output_directory}"
cd "${repo_root}"

package_installable=".#auxide"
image_installable=".#oci-image"

echo "==> Building exact Nix outputs"
package_store_path="$(nix build --no-link --print-out-paths "${package_installable}")"
image_store_path="$(nix build --no-link --print-out-paths "${image_installable}")"
package_derivation="$(nix path-info --derivation "${package_store_path}")"
image_derivation="$(nix path-info --derivation "${image_store_path}")"

echo "==> Recording the flake, lockfile, image, and closure metadata"
nix flake metadata --json >"${output_directory}/flake-metadata.json"
nix path-info --json --json-format 1 --recursive --closure-size "${package_store_path}" \
  >"${output_directory}/auxide-closure.json"
cp Cargo.lock flake.lock "${output_directory}/"
cp --dereference "${image_store_path}" "${output_directory}/auxide-oci.tar.gz"

echo "==> Generating SBOMs"
sbomnix "${package_installable}" \
  --cdx "${output_directory}/auxide.cdx.json" \
  --spdx "${output_directory}/auxide.spdx.json" \
  --csv "${output_directory}/auxide.csv"

echo "==> Generating Nix-derived SLSA provenance"
provenance "${package_store_path}" \
  --out "${output_directory}/auxide-provenance.json"

commit_sha="$(git rev-parse HEAD)"
created_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

jq -n \
  --arg release "${release_tag}" \
  --arg commit "${commit_sha}" \
  --arg createdAt "${created_at}" \
  --arg packageInstallable "${package_installable}" \
  --arg packageStorePath "${package_store_path}" \
  --arg packageDerivation "${package_derivation}" \
  --arg imageInstallable "${image_installable}" \
  --arg imageStorePath "${image_store_path}" \
  --arg imageDerivation "${image_derivation}" \
  '{
    schemaVersion: 1,
    release: $release,
    source: { commit: $commit, createdAt: $createdAt },
    outputs: {
      package: {
        installable: $packageInstallable,
        storePath: $packageStorePath,
        derivation: $packageDerivation
      },
      ociImage: {
        installable: $imageInstallable,
        storePath: $imageStorePath,
        derivation: $imageDerivation
      }
    }
  }' >"${output_directory}/manifest.json"

cat >"${output_directory}/RELEASE_NOTES.md" <<EOF
# ${release_tag}

Immutable Auxide build from commit \`${commit_sha}\`.

The attached bundle contains the unprivileged OCI image, exact Cargo and Nix lockfiles,
CycloneDX and SPDX SBOMs, Nix closure metadata, and Nix-derived SLSA provenance.

Verify downloaded files with:

\`\`\`console
sha256sum --check SHA256SUMS
\`\`\`
EOF

echo "==> Hashing every release artifact"
checksum_file="$(mktemp)"
trap 'rm -f "${checksum_file}"' EXIT
(
  cd "${output_directory}"
  find . -maxdepth 1 -type f ! -name SHA256SUMS -printf '%P\0' \
    | sort -z \
    | xargs -0 sha256sum >"${checksum_file}"
)
mv "${checksum_file}" "${output_directory}/SHA256SUMS"
trap - EXIT

echo "Release artifacts written to ${output_directory}"
