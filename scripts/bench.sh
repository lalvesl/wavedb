#!/usr/bin/env bash
# Run the comparative benchmark on a machine quiet enough to be believed.
#
# The suite refuses to *record* a row when the 1-minute load average at start
# exceeds its budget (`NOISE_PER_CPU`, floor 2.00) — a slow number a future
# bisect blames on a commit is worse than no number. That guard is right, and
# it is also easy to trip by accident: `nix run` builds first, and a 40-second
# cargo build pushes the 1-minute average past 2.00 just in time to be sampled.
# Two runs were burnt that way, ~45 minutes each, before this script existed.
#
# So: build first, wait for the machine to go quiet, and only then measure.
#
# Usage: scripts/bench.sh [args passed to the benchmark]
#   scripts/bench.sh --only wavedb --workload micro
#   scripts/bench.sh --dry-run          # print the plan; measures nothing
#
# Env:
#   BENCH_LOAD_MAX   launch below this 1-minute load average (default 1.0)
#   BENCH_WAIT_SECS  give up waiting after this long (default 900)

set -euo pipefail

cd "$(dirname "$0")/.."

load_max=${BENCH_LOAD_MAX:-1.0}
wait_secs=${BENCH_WAIT_SECS:-900}

load1() { cut -d' ' -f1 /proc/loadavg; }

# 1. Build inside the same environment the run will use, so the measured
#    invocation compiles nothing. `--dry-run` resolves the plan and stops, so
#    it exercises the real build and measures nothing; its output is
#    discarded.
echo "── building (so the measured run does not) ─────────────────────────────"
nix run .#bench -- --dry-run >/dev/null 2>&1 ||
  echo "warm-up run failed — continuing; the real run will report why"

# 2. Wait out the build's own load before the guard samples it.
echo "── waiting for load < ${load_max} (up to ${wait_secs}s) ────────────────"
waited=0
while [ "$(awk -v m="$load_max" '{print ($1 < m)}' /proc/loadavg)" != "1" ]; do
  if [ "$waited" -ge "$wait_secs" ]; then
    echo "still at $(load1) after ${wait_secs}s — running anyway; the suite" \
      "will refuse to record if that is too noisy" >&2
    break
  fi
  sleep 15
  waited=$((waited + 15))
done

# 3. Measure. A scratch directory left by an interrupted run makes the suite
#    refuse (it would replay someone else's journal), so clear ours first —
#    only the ones this suite creates, never a bare `rm -rf /tmp/*`.
find "${TMPDIR:-/tmp}" -maxdepth 1 -name 'wavedb-bench-*' -type d \
  -exec rm -rf {} + 2>/dev/null || true

echo "── measuring at $(date +%H:%M), load $(load1) ──────────────────────────"
exec nix run .#bench -- "$@"
