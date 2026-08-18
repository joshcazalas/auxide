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

# What a fetch has to get hold of before it counts as YouTube serving the
# track rather than offering a taste of it.
#
# The outage this probe exists for served the first mebibyte of every track and
# refused everything past it, which is about a minute of audio — long enough
# that a song sounded like it was playing normally right up until it stopped.
# Every metadata probe passed throughout. Clearing a mebibyte and a half is the
# smallest thing that would have failed.
readonly MUST_REACH=$((1536 * 1024))

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

# `fetched<TAB>total`, where the length may be empty if the origin stated none.
check_reach() {
  local what="$1" output="$2" fetched total
  IFS=$'\t' read -r fetched total <<<"${output}"

  if [[ ! "${fetched}" =~ ^[0-9]+$ ]]; then
    fail "${what}: no byte count, got '${output}'"
    return
  fi
  # A track shorter than the bar is not a refusal to serve it.
  if [[ "${total}" =~ ^[0-9]+$ ]] && ((total <= MUST_REACH)); then
    printf 'ok   %s (%d byte(s), the whole track)\n' "${what}" "${fetched}"
  elif ((fetched < MUST_REACH)); then
    fail "${what}: got ${fetched} of ${MUST_REACH} bytes — YouTube is answering questions about tracks but not handing them over"
  else
    printf 'ok   %s (%d byte(s))\n' "${what}" "${fetched}"
  fi
}

# Only ever writes to stdout. Counting a failure here would be counting it in
# the subshell a command substitution creates, where the increment is discarded
# and every probe could fail while the script still reported success.
#
# Standard error is kept rather than discarded, and shown when the probe fails.
# It carries the reason — what yt-dlp said, or which field would not parse —
# and this whole script exists to report that reason to somebody who was not
# watching. Throwing it away left "the probe exited non-zero" as the entire
# finding.
PROBE_STDERR="$(mktemp)"
readonly PROBE_STDERR
trap 'rm -f "${PROBE_STDERR}"' EXIT

run_probe() {
  "${auxide[@]}" "$@" 2>"${PROBE_STDERR}"
}

# What the probe said on its way out, indented so it reads as detail.
report_stderr() {
  if [[ -s "${PROBE_STDERR}" ]]; then
    sed 's/^/     | /' "${PROBE_STDERR}" >&2
  fi
}

echo "==> Resolving a single video"
if output="$(run_probe youtube-inspect "${PROBE_VIDEO}")"; then
  check_track_lines inspect "${output}" 1
else
  fail "inspect: the probe exited non-zero"
  report_stderr
fi

echo "==> Searching"
if output="$(run_probe youtube-search "${PROBE_SEARCH}")"; then
  check_track_lines search "${output}" 1
else
  fail "search: the probe exited non-zero"
  report_stderr
fi

echo "==> Expanding a playlist"
if output="$(run_probe youtube-playlist "${PROBE_PLAYLIST}")"; then
  # The first line is the playlist's own summary rather than a track.
  summary="$(head -n 1 <<<"${output}")"
  if [[ ! "${summary}" =~ playable$ ]]; then
    fail "playlist: no summary line, got '${summary}'"
  fi
  check_track_lines playlist "$(tail -n +2 <<<"${output}")" 2
  # Whatever the playlist named first, to fetch below. Taken from here rather
  # than hard-coded so there is one less identifier to go stale, and because a
  # conference talk is comfortably longer than the bar a fetch has to clear.
  reachable="$(tail -n +2 <<<"${output}" | head -n 1 | cut -f1)"
else
  fail "playlist: the probe exited non-zero"
  report_stderr
fi

echo "==> Fetching the start of a track"
if [[ -z "${reachable:-}" ]]; then
  fail "fetch: the playlist named nothing to fetch"
elif output="$(run_probe youtube-fetch "https://www.youtube.com/watch?v=${reachable}")"; then
  check_reach fetch "${output}"
else
  fail "fetch: the probe exited non-zero"
  report_stderr
fi

if ((failures > 0)); then
  echo "==> ${failures} check(s) failed; YouTube extraction has probably moved" >&2
  exit 1
fi
echo "==> Every probe answered"
