#!/usr/bin/env bash
# Fold a settle-round probe log (wavedb-storage `probe` feature,
# `crates/wavedb-storage/src/probe.rs`) into KiB written per round by
# component, in bands of rounds — what a durable write costs, and what of it
# is which structure.
#
#   scripts/probe_summary.sh LOG [ROUNDS_PER_BAND] [HEAD=NAME ...]
#
# A page counts under the head its records carry (their STRUCT_HASH, or
# the B+tree node's, a chain segment's, a Pivot's); a page holding several
# heads counts as `mixed`. HEAD=NAME pairs label the heads, e.g.
# `4250545245450001=bptree`; an unlabelled head prints as its first 8 hex
# digits. The last columns are the records per page of each component.
set -euo pipefail

log=${1:?usage: probe_summary.sh LOG [ROUNDS_PER_BAND] [HEAD=NAME ...]}
band=${2:-1000}
shift $(( $# >= 2 ? 2 : 1 ))

awk -F' [|] ' -v band="$band" -v labels="$*" '
BEGIN {
    n = split(labels, pairs, " ")
    for (i = 1; i <= n; i++) {
        split(pairs[i], kv, "=")
        name[kv[1]] = kv[2]
    }
}
function field(s, key,    m) {
    if (match(s, " " key "=[^ ]*")) return substr(s, RSTART + length(key) + 2, RLENGTH - length(key) - 2)
    if (match(s, "^" key "=[^ ]*")) return substr(s, RSTART + length(key) + 1, RLENGTH - length(key) - 1)
    return ""
}
/^round / {
    b = int(rounds / band); rounds++; per[b]++
    if (b > last) last = b
    for (i = 2; i <= NF; i++) {
        part = $i; kind = part; sub(/ .*/, "", kind)
        blocks = field(part, "blocks")
        if (kind == "page") {
            heads = field(part, "heads")
            if (heads == "") comp = "empty"
            else if (index(heads, ",")) comp = "mixed"
            else {
                head = heads; sub(/:.*/, "", head)
                comp = (head in name) ? name[head] : substr(head, 1, 8)
            }
            recs[b, comp] += field(part, "records"); pages[b, comp]++
        } else comp = kind
        kib[b, comp] += blocks * 4
        if (!(comp in seen)) { seen[comp] = 1; order[++ncomp] = comp }
    }
}
END {
    printf "%-13s", "rounds"
    for (c = 1; c <= ncomp; c++) printf "%10s", order[c]
    printf "%10s   records/page\n", "KiB/round"
    for (b = 0; b <= last; b++) {
        if (!per[b]) continue
        printf "%-13s", (b * band) "-" ((b + 1) * band - 1)
        total = 0
        for (c = 1; c <= ncomp; c++) {
            v = kib[b, order[c]] / per[b]; total += v
            printf "%10.1f", v
        }
        printf "%10.1f  ", total
        for (c = 1; c <= ncomp; c++)
            if (pages[b, order[c]]) printf " %s=%.1f", order[c], recs[b, order[c]] / pages[b, order[c]]
        printf "\n"
    }
}' "$log"
