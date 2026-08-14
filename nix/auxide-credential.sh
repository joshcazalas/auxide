#!/usr/bin/env bash

set -Eeuo pipefail

credential_directory=/var/lib/auxide
credential_file="${credential_directory}/discord-token"

usage() {
  cat >&2 <<'EOF'
usage: auxide-credential set|rotate|status

  set     create the host-encrypted Discord token; refuse if one exists
  rotate  atomically replace an existing token after an explicit prompt
  status  verify that the encrypted credential can be decrypted on this host
EOF
}

if ((EUID != 0)); then
  echo "auxide-credential must run as root." >&2
  exit 1
fi

if (($# != 1)); then
  usage
  exit 2
fi

action="$1"

case "${action}" in
status)
  if [[ ! -f "${credential_file}" ]]; then
    echo "Auxide credential is not provisioned." >&2
    exit 1
  fi
  systemd-creds decrypt \
    --name=discord-token \
    "${credential_file}" \
    /dev/null
  stat --format='%U:%G %a %n' "${credential_file}"
  echo "Auxide credential is present and valid for this host."
  ;;
set | rotate)
  if [[ "${action}" == set && -e "${credential_file}" ]]; then
    echo "Auxide credential already exists; use 'rotate' to replace it explicitly." >&2
    exit 1
  fi
  if [[ "${action}" == rotate && ! -f "${credential_file}" ]]; then
    echo "Auxide credential does not exist; use 'set' first." >&2
    exit 1
  fi

  install -d -m 0700 -o root -g root "${credential_directory}"
  temporary="$(mktemp --tmpdir="${credential_directory}" .discord-token.XXXXXX)"
  cleanup() {
    rm -f -- "${temporary}"
  }
  trap cleanup EXIT

  systemd-ask-password "Auxide Discord bot token" |
    systemd-creds encrypt \
      --with-key=host \
      --name=discord-token \
      - \
      "${temporary}"
  chmod 0600 "${temporary}"
  chown root:root "${temporary}"
  systemd-creds decrypt \
    --name=discord-token \
    "${temporary}" \
    /dev/null
  mv -- "${temporary}" "${credential_file}"
  trap - EXIT
  echo "Auxide credential ${action} completed."
  ;;
*)
  usage
  exit 2
  ;;
esac
