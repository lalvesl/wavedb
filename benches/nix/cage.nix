# The cage every measured run executes inside, so all five systems get the
# *same* machine rather than whatever this one happens to be (RFC 0060 §5).
# Three tools, because no one of them does all three jobs:
#
#   systemd-run  the cgroup, and the only one of the three that can cap
#                memory. `MemoryMax` bounds the **page cache** too, which is
#                what stops a 2 GiB dataset from simply living in RAM and
#                makes a cold read genuinely cold.
#   taskset      the CPU budget. `AllowedCPUs` would be tidier, but `cpuset`
#                is not among the controllers delegated to a user scope here
#                (`cpu io memory pids`), so affinity it is — and affinity is
#                what `nproc` reports, so the servers size their thread pools
#                from it.
#   bwrap        the namespace. It caps *nothing*: it is here for a private
#                PID namespace, so a killed run cannot leave a mongod behind,
#                and for one uniform filesystem shape.
#
# `--dev-bind / /` on purpose: the databases must write to the real disk. A
# tmpfs would put them in RAM and measure the wrong thing.
rec {
  cpus = "0-3";
  cpuBudget = "4"; # how many `cpus` names, for the guard

  # 500 MB for the run AND its server, from the first instruction to the last.
  # Two reasons, and the second is the stronger one:
  #
  #   the measurement  at this size the page cache stops hiding the disk, so a
  #                    cold read is cold and a dataset larger than memory is
  #                    one (RFC 0060 open question 4);
  #   the comparison   Postgres, MySQL and MongoDB each size their caches from
  #                    the *machine's* RAM by default, so an uncaged run does
  #                    not compare five systems on one machine — it compares
  #                    five opinions about how much of the machine to take.
  #                    Each is pinned to 256 MB
  #                    (`benches/src/systems/server.rs`), MongoDB's floor
  #                    setting the number for all three.
  #
  # The scope is created AT the budget rather than loose-then-tightened: a fill
  # runs inside it too (it measured *faster* there — see `benches/src/cage.rs`),
  # so there was nothing left for the loose window to buy, and one
  # configuration with no exceptions is the whole point. `benches/src/cage.rs`
  # refuses to record outside it.
  memMax = "524288000"; # 500 MB, in bytes for `memory.max`

  # Prefix a shell line with this to run it caged. It ends in `--`, so the
  # command to run follows on the next line.
  wrap = ''
    export BENCH_MEM_MAX=${memMax}
    export BENCH_CPU_BUDGET=${cpuBudget}
    exec systemd-run --user --scope -q \
      -p MemoryMax=${memMax} -p MemorySwapMax=0 \
      -p Delegate=yes -- \
      taskset -c ${cpus} \
      bwrap --dev-bind / / --unshare-pid --proc /proc -- \
  '';
}
