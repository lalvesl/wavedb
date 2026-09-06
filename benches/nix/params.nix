# The benchmark's two size knobs, in one place because five derivations and
# both apps have to agree on them: a seed's store path is named after `rows`,
# and the runner is told the same number so it never measures a dataset of a
# size it wasn't told about.
#
# Deliberately modest: the exceeds-RAM sizes of RFC 0060 §3 take hours to fill
# through a per-op-fsync engine, which is open question 4 in that RFC and is
# not answered here.
rec {
  benchRows = 200000;
  benchSeed = 42;

  # The string forms every builder script interpolates.
  rows = toString benchRows;
  sd = toString benchSeed;
}
