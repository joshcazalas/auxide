#!/usr/bin/env bash
#
# Ask YouTube for real answers, and check the shape of what comes back.
#
# ADR 0001 says extraction is expected to need regular dependency updates and
# may fail independently of anything in this repository. Nothing in the test
# suite can see that: the fixtures prove the parser handles the JSON yt-dlp
# used to produce. This runs the probes the CLI already exposes against the
# real thing.
#
# It checks shape and never content. A title is somebody else's to edit, and a
# check that asserted on one would fail the day they did.

set -Eeuo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

# Long-lived and unlikely to disappear: the first video uploaded to YouTube, and
# one of Google's own developer playlists. Neither is load-bearing — replace
# them the day either stops being a fair canary.
readonly PROBE_VIDEO="https://www.youtube.com/watch?v=jNQXAC9IVRw"
readonly PROBE_PLAYLIST="https://www.youtube.com/playlist?list=PLOU2XLYxmsIKpaV8h0AGE05so0fAwwfTw"
readonly PROBE_SEARCH="rick astley never gonna give you up"

auxide=(cargo run --quiet -- --config "${AUXIDE_CONFIG:-config.toml}")
failures=0

fail() {
  printf 'FAIL %s\n' "$*" >&2
  failures=$((failures + 1))
}

# Every probe prints tab-separated `id<TAB>seconds<TAB>title` per track, so one
# check covers all three.
check_track_lines() {
  local what="$1" output="$2" minimum="$3"
  local lines=0 id seconds title

  while IFS=$'\t' read -r id seconds title; do
    [[ -z "${id}" ]] && continue
    lines=$((lines + 1))
    if [[ ! "${id}" =~ ^[A-Za-z0-9_-]{5,}$ ]]; then
      fail "${what}: implausible video id '${id}'"
    fi
    # A silently null duration is how a playlist expansion would degrade, and
    # the queue refuses a track without one.
    if [[ ! "${seconds}" =~ ^[0-9]+$ ]] || ((seconds <= 0)); then
      fail "${what}: no positive duration for '${id}' (got '${seconds}')"
    fi
    if [[ -z "${title}" ]]; then
      fail "${what}: no title for '${id}'"
    fi
  done <<<"${output}"

  if ((lines < minimum)); then
    fail "${what}: expected at least ${minimum} track(s), got ${lines}"
  else
    printf 'ok   %s (%d track(s))\n' "${what}" "${lines}"
  fi
}

# Only ever writes to stdout. Counting a failure here would be counting it in
# the subshell a command substitution creates, where the increment is discarded
# and every probe could fail while the script still reported success.
run_probe() {
  "${auxide[@]}" "$@" 2>/dev/null
}

echo "==> Resolving a single video"
if output="$(run_probe youtube-inspect "${PROBE_VIDEO}")"; then
  check_track_lines inspect "${output}" 1
else
  fail "inspect: the probe exited non-zero"
fi

echo "==> Searching"
if output="$(run_probe youtube-search "${PROBE_SEARCH}")"; then
  check_track_lines search "${output}" 1
else
  fail "search: the probe exited non-zero"
fi

echo "==> Expanding a playlist"
if output="$(run_probe youtube-playlist "${PROBE_PLAYLIST}")"; then
  # The first line is the playlist's own summary rather than a track.
  summary="$(head -n 1 <<<"${output}")"
  if [[ ! "${summary}" =~ playable$ ]]; then
    fail "playlist: no summary line, got '${summary}'"
  fi
  check_track_lines playlist "$(tail -n +2 <<<"${output}")" 2
else
  fail "playlist: the probe exited non-zero"
fi

if ((failures > 0)); then
  echo "==> ${failures} check(s) failed; YouTube extraction has probably moved" >&2
  exit 1
fi
echo "==> Every probe answered"
