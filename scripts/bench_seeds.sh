#!/usr/bin/env bash
# Pin a dataset tier's seeds as GC roots (RFC 0065 §5, RFC 0060 §6).
#
# A seed is a filled data directory in the Nix store, and filling one is the
# most expensive thing this suite does: the `large` tier's WaveDB seed is
# hours. Nothing protects it — a `nix-collect-garbage` between runs deletes
# every seed that no root points at, and the next run silently refills.
#
# `nix build --out-link` is the fix: the symlink it leaves is an indirect GC
# root, so the store path survives until the link is removed. This script is
# that, over a whole tier, into a gitignored `.bench-seeds/<tier>/`.
#
# It **pins what is already built** and only reports what is missing. Building
# is opt-in (`--build`) because at `large` and `huge` that is hours of work,
# and a script that starts it by accident is a script nobody runs twice.
#
# Usage:
#   scripts/bench_seeds.sh                 pin the default tier
#   scripts/bench_seeds.sh large           pin that tier
#   scripts/bench_seeds.sh large --build   build the missing ones first
#   scripts/bench_seeds.sh --list          what is pinned, and how big
#   scripts/bench_seeds.sh large --unpin   drop that tier's roots

set -euo pipefail

cd "$(dirname "$0")/.."

roots=.bench-seeds

# The six outputs a tier has: the portable TSV, and one seed per system.
outputs_for() {
  local tier=$1
  echo "bench-dataset-${tier}"
  local s
  for s in wavedb sqlite postgres mysql mongodb; do
    echo "bench-seed-${s}-${tier}"
  done
}

# The tier `params.nix` calls default, so the script and the apps agree
# without the name being written twice.
default_tier() {
  nix eval --raw --impure --expr '(import ./benches/nix/params.nix).defaultTier'
}

list_roots() {
  if [ ! -d "$roots" ]; then
    echo "nothing pinned ($roots does not exist)"
    return
  fi
  local link target
  while IFS= read -r link; do
    target=$(readlink -f "$link")
    printf '%-34s %8s  %s\n' \
      "${link#"$roots"/}" \
      "$(du -sh "$target" 2>/dev/null | cut -f1)" \
      "$target"
  done < <(find "$roots" -maxdepth 2 -type l | sort)
}

tier=""
build=0
action=pin
for arg in "$@"; do
  case $arg in
    --list) action=list ;;
    --unpin) action=unpin ;;
    --build) build=1 ;;
    -*) echo "unknown flag $arg" >&2; exit 2 ;;
    *) tier=$arg ;;
  esac
done

if [ "$action" = list ]; then
  list_roots
  exit 0
fi

tier=${tier:-$(default_tier)}
dir="$roots/$tier"

if [ "$action" = unpin ]; then
  rm -rf "${dir:?}"
  echo "unpinned $tier — its store paths are now collectable"
  exit 0
fi

mkdir -p "$dir"
missing=()
for out in $(outputs_for "$tier"); do
  # Ask whether it would have to be built, rather than building to find out.
  if nix build --dry-run ".#$out" 2>&1 | grep -q 'will be built'; then
    if [ "$build" = 1 ]; then
      echo "building $out (this is the expensive part)"
      nix build --out-link "$dir/$out" ".#$out"
      continue
    fi
    missing+=("$out")
    continue
  fi
  nix build --out-link "$dir/$out" ".#$out"
  echo "pinned $out"
done

if [ ${#missing[@]} -gt 0 ]; then
  echo
  echo "not built, so not pinned: ${missing[*]}"
  echo "build them with: scripts/bench_seeds.sh $tier --build"
fi
