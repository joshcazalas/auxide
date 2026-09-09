#!/usr/bin/env bash

set -Eeuo pipefail

if [[ $# != 1 ]]; then
  echo "usage: $0 OCI_ARCHIVE" >&2
  exit 2
fi

test_directory="$(mktemp -d)"
test_name="auxide-ci-$(basename "${test_directory}")"
test_image="auxide-ci:$(basename "${test_directory}")"
cleanup() {
  docker rm --force "${test_name}" >/dev/null 2>&1 || true
  docker image rm "${test_image}" >/dev/null 2>&1 || true
  rm -rf "${test_directory}"
}
trap cleanup EXIT

# Import under a disposable tag without replacing an existing Auxide tag.
cat > "${test_directory}/policy.json" <<'JSON'
{"default":[{"type":"reject"}],"transports":{"docker-archive":{"":[{"type":"insecureAcceptAnything"}]}}}
JSON
skopeo --policy "${test_directory}/policy.json" copy "docker-archive:$1" "docker-daemon:${test_image}"
configuration="$(docker image inspect "${test_image}")"
jq -e '.[0].Config | .User == "65532:65532" and .Cmd == ["--config", "/run/auxide/config.toml", "run"]' <<< "${configuration}" >/dev/null
entrypoint="$(jq -er '.[0].Config.Entrypoint | select(length == 1) | .[0]' <<< "${configuration}")"
read -r marker interpreter _ < "${entrypoint}"
if [[ "${marker}" != '#!' ]]; then
  interpreter="${marker#\#!}"
fi
[[ "${interpreter}" == /nix/store/*/bin/bash ]]

container=(docker run --rm --name "${test_name}" --network none --read-only --cap-drop ALL --security-opt no-new-privileges)
# shellcheck disable=SC2016 # These variables belong to the container's shell.
timeout 30 "${container[@]}" --entrypoint "${interpreter}" "${test_image}" -c '[[ "$EUID" == 65532 && -r "$SSL_CERT_FILE" ]]'
timeout 30 "${container[@]}" "${test_image}" --help > "${test_directory}/help"
[[ "$(cat "${test_directory}/help")" == *check-config* ]]

config="${test_directory}/config.toml"
printf '[discord]\ntoken_file = "/run/auxide/absent-token"\n' > "${config}"
chmod 644 "${config}"
timeout 30 "${container[@]}" --mount "type=bind,src=${config},dst=/run/auxide/config.toml,readonly" \
  "${test_image}" --config /run/auxide/config.toml check-config

expect_failure() {
  local expected="$1"
  shift
  local status=0
  timeout 30 "${container[@]}" "$@" \
    > "${test_directory}/output" 2>&1 || status=$?
  if [[ "${status}" != 1 || "$(cat "${test_directory}/output")" != *"${expected}"* ]]; then
    cat "${test_directory}/output" >&2
    echo "Expected configuration failure '${expected}', got exit ${status}." >&2
    exit 1
  fi
}

expect_failure 'failed to load /run/auxide/config.toml' "${test_image}"
chmod 000 "${config}"
expect_failure 'Permission denied' --mount "type=bind,src=${config},dst=/run/auxide/config.toml,readonly" \
  "${test_image}" --config /run/auxide/config.toml check-config
chmod 644 "${config}"
printf '[invalid\n' > "${config}"
expect_failure 'failed to load /run/auxide/config.toml' --mount "type=bind,src=${config},dst=/run/auxide/config.toml,readonly" \
  "${test_image}" --config /run/auxide/config.toml check-config

echo "Container startup, user, certificates, and configuration checks passed."
