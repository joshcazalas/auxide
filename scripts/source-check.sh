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

# Long-lived and unlikely to disappear: one of Google's own developer
# playlists. It is not load-bearing — replace it the day it stops being a fair
# canary. The single-video target comes from the search below instead of being
# fixed here: YouTube challenges well-known static probes from hosted-runner IPs
# even while it serves other public videos from the same address.
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
readonly PROBE_PACKETS=250

# CI supplies the cached, wrapped Nix executable. Local development continues
# to use Cargo unless an operator explicitly selects an installed binary.
if [[ -n "${AUXIDE_BIN:-}" ]]; then
  auxide=("${AUXIDE_BIN}" --config "${AUXIDE_CONFIG:-config.toml}")
else
  auxide=(cargo run --locked --quiet -- --config "${AUXIDE_CONFIG:-config.toml}")
fi
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

# Counts come from the CLI. Keep them canonical and within Bash's arithmetic range.
is_count() {
  [[ "$1" =~ ^(0|[1-9][0-9]{0,17})$ ]]
}

# `fetched<TAB>total`, where the length may be empty if the origin stated none.
check_reach() {
  local what="$1" output="$2" fetched total required="${MUST_REACH}"
  if [[ ! "${output}" =~ ^([0-9]+)$'\t'([0-9]*)$ ]]; then
    fail "${what}: invalid byte counts, got '${output}'"
    return
  fi
  IFS=$'\t' read -r fetched total <<<"${output}"

  if ! is_count "${fetched}"; then
    fail "${what}: invalid fetched byte count '${fetched}'"
    return
  fi
  if [[ -n "${total}" ]]; then
    if ! is_count "${total}" || ((total == 0 || fetched > total)); then
      fail "${what}: inconsistent byte counts, got '${output}'"
      return
    fi
    if ((total < required)); then
      required="${total}"
    fi
  fi
  if ((fetched < required)); then
    fail "${what}: got ${fetched} of ${required} required bytes — the track was not fully served up to the probe limit"
    return
  fi
  printf 'ok   %s (%d byte(s))\n' "${what}" "${fetched}"
}

# `id<TAB>packets<TAB>frames<TAB>reached_end`, from the actual audio decoder.
check_decode() {
  local output="$1" expected_id="$2" id packets frames reached_end
  if [[ ! "${output}" =~ ^([A-Za-z0-9_-]+)$'\t'([0-9]+)$'\t'([0-9]+)$'\t'(true|false)$ ]]; then
    fail "decode: invalid audio probe result, got '${output}'"
    return
  fi
  IFS=$'\t' read -r id packets frames reached_end <<<"${output}"
  if [[ "${id}" != "${expected_id}" ]] || ! is_count "${packets}" || ! is_count "${frames}"; then
    fail "decode: invalid track or counters, got '${output}'"
    return
  fi
  if ((packets == 0 || packets > PROBE_PACKETS || frames == 0)); then
    fail "decode: expected decoded audio within ${PROBE_PACKETS} packets, got '${output}'"
    return
  fi
  if [[ "${reached_end}" == false ]] && ((packets < PROBE_PACKETS)); then
    fail "decode: stopped before the packet limit without reaching the end of the track"
    return
  fi
  printf 'ok   decode (%d packet(s), %d frame(s))\n' "${packets}" "${frames}"
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

echo "==> Searching"
if output="$(run_probe youtube-search "${PROBE_SEARCH}")"; then
  check_track_lines search "${output}" 1
  # Search prepares each result fully rather than returning a flat list, so a
  # result here is a video YouTube just allowed this runner to resolve. Reuse
  # one for the direct probes below. This keeps them sensitive to extraction
  # drift without confusing a challenge on one famous fixed ID for global
  # breakage.
  IFS=$'\t' read -r reachable _ <<<"${output}"
else
  fail "search: the probe exited non-zero"
  report_stderr
fi

echo "==> Resolving a single video"
if [[ -z "${reachable:-}" ]]; then
  fail "inspect: the search named nothing to inspect"
elif output="$(run_probe youtube-inspect "https://www.youtube.com/watch?v=${reachable}")"; then
  check_track_lines inspect "${output}" 1
else
  fail "inspect: the probe exited non-zero"
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
else
  fail "playlist: the probe exited non-zero"
  report_stderr
fi

echo "==> Fetching the start of a track"
if [[ -z "${reachable:-}" ]]; then
  fail "fetch: the search named nothing to fetch"
elif output="$(run_probe youtube-fetch "https://www.youtube.com/watch?v=${reachable}")"; then
  check_reach fetch "${output}"
else
  fail "fetch: the probe exited non-zero"
  report_stderr
fi

echo "==> Decoding the start of a track"
if [[ -z "${reachable:-}" ]]; then
  fail "decode: the search named nothing to decode"
elif output="$(run_probe youtube-probe "https://www.youtube.com/watch?v=${reachable}" --packets "${PROBE_PACKETS}")"; then
  check_decode "${output}" "${reachable}"
else
  fail "decode: the probe exited non-zero"
  report_stderr
fi

if ((failures > 0)); then
  echo "==> ${failures} check(s) failed; inspect the metadata, delivery, and decoding results above" >&2
  exit 1
fi
echo "==> Every probe answered"
