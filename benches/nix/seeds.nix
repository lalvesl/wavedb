# ── bench seeds: filled data directories as derivations (RFC 0060 §6) ────────
#
# A filled data directory is a pure function of (system, version, rows, seed),
# so it belongs in the Nix store rather than being refilled before every run.
# Two properties are why this is Nix and not a `~/.cache` directory:
#
#  * **Version binding is free and correct.** A data directory is locked to its
#    server's major version; because the server package is an input, bumping it
#    in `flake.lock` invalidates the seed. An ad-hoc cache would hand a
#    PostgreSQL 18 datadir to PostgreSQL 19.
#  * **Cached by inputs, not content.** These outputs hold timestamps, WAL and
#    random system identifiers — they are *not* bit-reproducible and must never
#    be marked fixed-output.
{
  pkgs,
  benchGen,
  benchDataset,
  rows,
  sd,
}:
let
  # Every seed is a data directory a server can be pointed at directly. Two
  # rules hold in all of them: the server is shut down **cleanly** inside the
  # builder (otherwise the first measured operation pays for crash recovery),
  # and nothing is timed.
  mkSeed =
    name: deps: script:
    pkgs.runCommand "bench-seed-${name}-${rows}"
      {
        nativeBuildInputs = deps;
        dataset = benchDataset;
      }
      ''
        mkdir -p "$out"
        # `cat`, not `cp`: a copy out of the store keeps its read-only mode and
        # the append below would fail.
        cat "$dataset/manifest.txt" > "$out/manifest.txt"
        echo "system=${name}" >> "$out/manifest.txt"
        export HOME="$TMPDIR"

        # Every seed must prove it loaded. Bulk loaders are cheerful: a `psql`
        # heredoc without ON_ERROR_STOP returns 0 after a failed \copy, which
        # once produced a green build and an empty database. A seed that builds
        # but holds nothing is worse than no seed — the benchmark would measure
        # an empty table and report it.
        expectRows() {
          if [ "$1" != "${rows}" ]; then
            echo "seed ${name}: loaded $1 rows, expected ${rows}" >&2
            exit 1
          fi
          echo "seed ${name}: verified $1 rows"
        }

        ${script}
      '';
in
{
  # WaveDB has no bulk path — one insert is one batch is one fsync — so it
  # fills through the engine rather than from the TSV. The sidecar
  # (`ids.bin`, `pivot.bin`) is not optional: a NonUnique anchor id is minted
  # from the clock and cannot be recomputed from the seed.
  wavedb = mkSeed "wavedb" [ benchGen ] ''
    bench-gen fill-wavedb --rows ${rows} --seed ${sd} --out "$out"
    # 16 bytes per minted anchor id — the sidecar's length is the record
    # count, and without it the seed is unreadable.
    expectRows "$(( $(stat -c %s "$out/ids.bin") / 16 ))"
  '';

  sqlite = mkSeed "sqlite" [ pkgs.sqlite ] ''
    sqlite3 "$out/bench.db" <<SQL
    .bail on
    CREATE TABLE thing (
      id    INTEGER PRIMARY KEY,
      kind  INTEGER NOT NULL,
      score INTEGER NOT NULL,
      name  TEXT    NOT NULL,
      tag   TEXT    NOT NULL,
      body  TEXT    NOT NULL
    );
    .mode tabs
    .import $dataset/dataset.tsv thing
    CREATE INDEX idx_thing_tag ON thing(tag);
    PRAGMA journal_mode = WAL;
    PRAGMA wal_checkpoint(TRUNCATE);
    SQL
    expectRows "$(sqlite3 "$out/bench.db" 'SELECT count(*) FROM thing')"
  '';

  postgres = mkSeed "postgres" [ pkgs.postgresql_18 ] ''
    initdb -D "$out/data" --no-locale --encoding=UTF8 -U bench
    pg_ctl -D "$out/data" -w -o "-k $TMPDIR -c listen_addresses=" start
    psql -h "$TMPDIR" -U bench -d postgres -v ON_ERROR_STOP=1 <<SQL
    CREATE TABLE thing (
      id    BIGINT PRIMARY KEY,
      kind  INTEGER NOT NULL,
      score BIGINT  NOT NULL,
      name  TEXT    NOT NULL,
      tag   TEXT    NOT NULL,
      body  TEXT    NOT NULL
    );
    \copy thing FROM '$dataset/dataset.tsv' WITH (FORMAT text)
    CREATE INDEX idx_thing_tag ON thing(tag);
    CHECKPOINT;
    SQL
    expectRows "$(psql -h "$TMPDIR" -U bench -d postgres -tAc \
      'SELECT count(*) FROM thing')"
    pg_ctl -D "$out/data" -w stop   # clean shutdown: no recovery later
  '';

  mysql = mkSeed "mysql" [ pkgs.mysql84 ] ''
    mysqld --initialize-insecure --datadir="$out/data" \
      --user="$(id -un)" --log-error="$TMPDIR/init.log"
    mysqld --datadir="$out/data" --socket="$TMPDIR/mysql.sock" \
      --skip-networking --log-error="$TMPDIR/run.log" \
      --local-infile=1 &
    for _ in $(seq 1 60); do
      mysqladmin --socket="$TMPDIR/mysql.sock" -u root ping \
        >/dev/null 2>&1 && break
      sleep 1
    done
    mysql --socket="$TMPDIR/mysql.sock" -u root --local-infile=1 <<SQL
    CREATE DATABASE bench;
    USE bench;
    CREATE TABLE thing (
      id    BIGINT PRIMARY KEY,
      kind  INT    NOT NULL,
      score BIGINT NOT NULL,
      name  TEXT   NOT NULL,
      tag   VARCHAR(64) NOT NULL,
      body  TEXT   NOT NULL
    ) ENGINE=InnoDB;
    LOAD DATA LOCAL INFILE '$dataset/dataset.tsv' INTO TABLE thing;
    CREATE INDEX idx_thing_tag ON thing(tag);
    SQL
    expectRows "$(mysql --socket="$TMPDIR/mysql.sock" -u root -N -B \
      -e 'SELECT count(*) FROM bench.thing')"
    mysqladmin --socket="$TMPDIR/mysql.sock" -u root shutdown
    wait
  '';

  # The one seed that cannot run directly in the Nix sandbox. MongoDB's
  # bundled tcmalloc reads the possible-CPU mask at startup and `CHECK`-fails
  # (SIGABRT, before a single argument is parsed) when it cannot — and the
  # sandbox neither mounts /sys nor lets the builder create it, since its root
  # is read-only. So the whole seed runs inside a bubblewrap namespace
  # carrying a synthetic /sys. The value is **fixed**: the seed must stay a
  # pure function of its inputs, not of the builder's core count.
  mongodb =
    mkSeed "mongodb"
      [
        pkgs.mongodb-ce # mongod
        pkgs.mongosh # the shell (free, separate package)
        pkgs.mongodb-tools # mongoimport (free, separate package)
        pkgs.bubblewrap # the /sys shim below
        pkgs.bash # bwrap's root is a fresh tmpfs: no /bin/sh in it
      ]
      ''
        echo 0-3 > "$TMPDIR/cpu-possible"
        mkdir -p "$out/data"

        # Quoted heredoc: this runs *inside* the namespace and reads its paths
        # from the environment bwrap forwards below.
        cat > "$TMPDIR/seed.sh" <<'SEED'
        set -euo pipefail
        mongod --dbpath "$OUT/data" --bind_ip 127.0.0.1 --port 27017 \
          --fork --logpath "$TMPDIR/mongod.log"
        # `--columnsHaveTypes` needs the types spelled out, or every column
        # arrives as a string and the collection silently holds the wrong shape.
        # `_id` is set from the dataset id so the point-lookup key is the same
        # value on every system.
        mongoimport --host 127.0.0.1 --port 27017 \
          --db bench --collection thing --type tsv \
          --columnsHaveTypes \
          --fields '_id.int64(),kind.int32(),score.int64(),name.string(),tag.string(),body.string()' \
          --file "$DATASET/dataset.tsv" \
          --numInsertionWorkers 4
        mongosh --host 127.0.0.1 --port 27017 bench --quiet --eval \
          'db.thing.createIndex({tag: 1})'
        mongosh --host 127.0.0.1 --port 27017 bench --quiet \
          --eval 'db.thing.countDocuments({})' > "$TMPDIR/count.txt"
        mongod --dbpath "$OUT/data" --shutdown
        SEED

        bwrap \
          --tmpfs / \
          --ro-bind /nix/store /nix/store \
          --bind "$TMPDIR" "$TMPDIR" \
          --bind "$out" "$out" \
          --proc /proc --dev /dev --tmpfs /tmp \
          --tmpfs /sys \
          --ro-bind "$TMPDIR/cpu-possible" /sys/devices/system/cpu/possible \
          --setenv HOME "$TMPDIR" \
          --setenv OUT "$out" \
          --setenv DATASET "$dataset" \
          --chdir "$TMPDIR" \
          -- bash "$TMPDIR/seed.sh"

        # Counted inside the namespace, verified out here: `expectRows` is the
        # outer builder's function.
        expectRows "$(cat "$TMPDIR/count.txt")"
      '';
}
