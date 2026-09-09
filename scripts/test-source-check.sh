#!/usr/bin/env bash

set -Eeuo pipefail

repo_root="$(git rev-parse --show-toplevel)"
test_directory="$(mktemp -d)"
trap 'rm -rf "${test_directory}"' EXIT

cat > "${test_directory}/fake auxide" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail

[[ "$1" == --config && "$2" == "${PROBE_TEST_CONFIG}" ]] || exit 90
shift 2
command="$1"
shift
if [[ "${command}" == "${PROBE_TEST_FAIL}" ]]; then
  echo "simulated ${command} failure" >&2
  exit 23
fi

case "${command}" in
youtube-search|youtube-inspect)
  printf 'abcdefghijk\t60\tA playable track\n'
  ;;
youtube-playlist)
  printf 'A playlist: 2 playable\nabcdefghijk\t60\tFirst track\nother_video\t30\tSecond track\n'
  ;;
youtube-fetch)
  [[ "$1" == 'https://www.youtube.com/watch?v=abcdefghijk' ]] || exit 91
  printf '%s\n' "${PROBE_TEST_FETCH}"
  ;;
youtube-probe)
  [[ $# == 3 && "$1" == 'https://www.youtube.com/watch?v=abcdefghijk' && "$2" == --packets && "$3" == 250 ]] || exit 92
  printf '%s\n' "${PROBE_TEST_DECODE}"
  ;;
*) exit 93 ;;
esac
EOF
chmod +x "${test_directory}/fake auxide"

cases=0
check() {
  local expected="$1" description="$2" output status
  shift 2
  if output="$(env \
    AUXIDE_BIN="${test_directory}/fake auxide" \
    AUXIDE_CONFIG="${test_directory}/config with spaces.toml" \
    PROBE_TEST_CONFIG="${test_directory}/config with spaces.toml" \
    PROBE_TEST_FAIL='' \
    PROBE_TEST_FETCH=$'2097152\t4194304' \
    PROBE_TEST_DECODE=$'abcdefghijk\t250\t240000\tfalse' \
    "$@" "${repo_root}/scripts/source-check.sh" 2>&1)"; then
    status=0
  else
    status=$?
  fi

  if [[ "${expected}" == pass ]]; then
    if ((status != 0)) || [[ "${output}" != *'ok   decode'* || "${output}" == *'FAIL '* ]]; then
      printf 'FAIL %s (exit %d)\n%s\n' "${description}" "${status}" "${output}" >&2
      exit 1
    fi
  elif ((status != 1)) || [[ "${output}" != *"FAIL ${expected}:"* ]]; then
    printf 'FAIL %s: expected %s failure (exit %d)\n%s\n' "${description}" "${expected}" "${status}" "${output}" >&2
    exit 1
  fi
  for assignment in "$@"; do
    if [[ "${assignment}" == PROBE_TEST_FAIL=* && "${output}" != *"simulated ${assignment#*=} failure"* ]]; then
      printf 'FAIL %s: command stderr was lost\n%s\n' "${description}" "${output}" >&2
      exit 1
    fi
  done
  cases=$((cases + 1))
}

check pass 'delivered sample and decoded packet limit'
check pass 'unknown content length' PROBE_TEST_FETCH=$'1572864\t'
check pass 'complete short track' PROBE_TEST_FETCH=$'100\t100'
check pass 'exact threshold' PROBE_TEST_FETCH=$'1572864\t1572864'
check pass 'decoded short track reaches EOF' PROBE_TEST_DECODE=$'abcdefghijk\t8\t7680\ttrue'
check pass 'EOF at packet limit' PROBE_TEST_DECODE=$'abcdefghijk\t250\t240000\ttrue'

check fetch 'empty advertised short track' PROBE_TEST_FETCH=$'0\t100'
check fetch 'truncated short track' PROBE_TEST_FETCH=$'99\t100'
check fetch 'empty unknown-length track' PROBE_TEST_FETCH=$'0\t'
check fetch 'unknown-length track below threshold' PROBE_TEST_FETCH=$'1572863\t'
check fetch 'known-length track below threshold' PROBE_TEST_FETCH=$'1572863\t4194304'
check fetch 'zero advertised length' PROBE_TEST_FETCH=$'0\t0'
check fetch 'more bytes than advertised' PROBE_TEST_FETCH=$'101\t100'
check fetch 'malformed content length' PROBE_TEST_FETCH=$'2097152\tunknown'
check fetch 'negative fetched bytes' PROBE_TEST_FETCH=$'-1\t100'
check fetch 'noncanonical count' PROBE_TEST_FETCH=$'0100\t100'
check fetch 'counter exceeds arithmetic range' PROBE_TEST_FETCH=$'0\t18446744073709551615'
check fetch 'extra byte-count column' PROBE_TEST_FETCH=$'100\t100\textra'
check fetch 'extra byte-count row' PROBE_TEST_FETCH=$'100\t100\n100\t100'
check fetch 'missing length separator' PROBE_TEST_FETCH='2097152'

check decode 'bytes arrive but no audio frames' PROBE_TEST_DECODE=$'abcdefghijk\t250\t0\tfalse'
check decode 'zero decoded packets' PROBE_TEST_DECODE=$'abcdefghijk\t0\t1\ttrue'
check decode 'premature decode without EOF' PROBE_TEST_DECODE=$'abcdefghijk\t8\t7680\tfalse'
check decode 'packet limit exceeded' PROBE_TEST_DECODE=$'abcdefghijk\t251\t240960\tfalse'
check decode 'wrong decoded video' PROBE_TEST_DECODE=$'other_video\t250\t240000\tfalse'
check decode 'invalid EOF flag' PROBE_TEST_DECODE=$'abcdefghijk\t250\t240000\tunknown'
check decode 'invalid packet count' PROBE_TEST_DECODE=$'abcdefghijk\ttwo\t240000\tfalse'
check decode 'negative frame count' PROBE_TEST_DECODE=$'abcdefghijk\t250\t-1\tfalse'
check decode 'overflowing frame count' PROBE_TEST_DECODE=$'abcdefghijk\t250\t18446744073709551615\tfalse'
check decode 'missing frame count' PROBE_TEST_DECODE=$'abcdefghijk\t250\t\tfalse'
check decode 'missing EOF flag' PROBE_TEST_DECODE=$'abcdefghijk\t250\t240000'
check decode 'extra decode column' PROBE_TEST_DECODE=$'abcdefghijk\t250\t240000\tfalse\textra'
check decode 'extra decode row' PROBE_TEST_DECODE=$'abcdefghijk\t250\t240000\tfalse\nextra'

check search 'failed search' PROBE_TEST_FAIL=youtube-search
check inspect 'failed metadata resolution' PROBE_TEST_FAIL=youtube-inspect
check playlist 'failed playlist expansion' PROBE_TEST_FAIL=youtube-playlist
check fetch 'failed media delivery' PROBE_TEST_FAIL=youtube-fetch
check decode 'failed audio parsing/decoding' PROBE_TEST_FAIL=youtube-probe

printf 'Source canary: %d offline cases passed\n' "${cases}"
