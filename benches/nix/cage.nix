# The cage every measured row executes inside (RFC 0065 §2).
#
# Three tools, because no one of them does all three jobs:
#
#   systemd-run  the cgroup, and the only one of the three that can cap
#                memory. `MemoryMax` bounds the **page cache** too, which is
#                what stops a dataset from simply living in RAM and makes a
#                cold read genuinely cold.
#   taskset      the CPU budget. `AllowedCPUs` would be tidier, but `cpuset`
#                is not among the controllers delegated to a user scope here
#                (`cpu io memory pids`), so affinity it is — and affinity is
#                what `nproc` reports, so the servers size their thread pools
#                from it.
#   bwrap        the namespace. It caps *nothing*: it is here for a private
#                PID namespace, so a killed row cannot leave a mongod behind,
#                and for one uniform filesystem shape.
#
# `--dev-bind / /` on purpose: the databases must write to the real disk. A
# tmpfs would put them in RAM and measure the wrong thing.
#
# ## What changed in RFC 0065: the cage is per ROW, not per run
#
# This file no longer wraps anything. It declares the budgets, and
# `benches/src/cage/exec.rs` builds one cage around each `bench-row` child.
# The supervisor itself stays outside — it measures nothing, and charging a
# row's 500 MB for the orchestration around it would put work inside the
# measurement that is not the measurement.
#
# A fresh cgroup per row also means the memory budget starts empty. Under the
# old shape row *k* inherited whatever page cache row *k−1* left behind, and
# since `MemoryMax` bounds page cache, that inheritance was not neutral.
rec {
  cpus = "0-3";
  cpuBudget = "4"; # how many `cpus` names, for the guard

  # 500 MB for the row AND its server, from the first instruction to the last.
  # Two reasons, and the second is the stronger one:
  #
  #   the measurement  at this size the page cache stops hiding the disk, so a
  #                    cold read is cold and a dataset larger than memory is
  #                    one;
  #   the comparison   Postgres, MySQL and MongoDB each size their caches from
  #                    the *machine's* RAM by default, so an uncaged run does
  #                    not compare five systems on one machine — it compares
  #                    five opinions about how much of the machine to take.
  #                    Each is pinned to 256 MB
  #                    (`benches/src/systems/server.rs`), MongoDB's floor
  #                    setting the number for all three.
  memMax = "524288000"; # 500 MB, in bytes for `memory.max`

  # Part of every row's identity (RFC 0065 §1). Bump it when anything above
  # changes: a row measured under a different recipe was measured on a
  # different machine, and must not silently compare with the corpus.
  revision = "1";

  # What the supervisor needs in order to build a row's cage. It reads these
  # rather than taking defaults, so the recorded budgets and the declared ones
  # cannot drift apart — and `benches/src/cage/verify.rs` makes each row prove
  # it actually got them.
  exports = ''
    export BENCH_MEM_MAX=${memMax}
    export BENCH_CPU_BUDGET=${cpuBudget}
    export BENCH_CPUS=${cpus}
  '';
}
