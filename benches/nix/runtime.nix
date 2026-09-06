# What a measured run needs on `PATH`.
#
# The server binaries are `runtimeInputs` rather than an assumption about the
# machine: an adapter starts its own server in the run's scratch directory, so
# a benchmark row can never be measuring whatever the developer happens to
# have installed and running. Every competitor version comes from
# `flake.lock` exactly like the toolchain does — that pinning is the reason
# this runs through Nix at all, and it is what makes a recorded result
# attributable to a whole stack.
{
  pkgs,
  rustToolchain,
}:
with pkgs;
[
  rustToolchain
  pkg-config
  git
  coreutils

  # The measured peers (RFC 0060).
  sqlite
  postgresql_18 # postgres, initdb, pg_ctl
  mysql84 # mysqld, mysqladmin
  mongodb-ce # mongod

  # The cage (see ./cage.nix).
  bubblewrap
  util-linux # taskset
  systemd # systemd-run, for the cgroup
]
