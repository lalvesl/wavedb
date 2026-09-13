//! The server bracket, against **live servers**.
//!
//! These are integration tests rather than unit tests for one reason: they
//! start a real `postgres`, drive it through the RFC 0065 harness, and stop
//! it. Nothing about that is mockable, and a driver that only compiles is a
//! driver nobody has run.
//!
//! They **skip** when the binaries are not on `PATH` rather than failing. The
//! peers come from `flake.lock` (`nix develop`, or `nix run .#bench`), so a
//! bare `cargo test` outside the shell has no `initdb` — and turning that into
//! a red suite would teach everyone to ignore it. The skip prints, so a run
//! that proved nothing says so.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use wavedb_bench::harness::micro::MicroWorkload;
use wavedb_bench::harness::{DriverFactory, run};

/// Is `binary` on `PATH`?
fn available(binary: &str) -> bool {
    std::process::Command::new(binary)
        .arg("--version")
        .output()
        .is_ok()
}

struct Scratch(PathBuf);

impl Scratch {
    fn new(what: &str) -> Self {
        let dir = std::env::temp_dir()
            .join(format!("wavedb-bench-live-{what}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch");
        // PostgreSQL refuses to start on a data directory readable by group
        // or other.
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn workload() -> MicroWorkload {
    MicroWorkload {
        rows: 300,
        reads: 150,
        updates: 150,
        seed: 42,
        seeded: false,
    }
}

/// A full micro row against a real PostgreSQL: initdb, start, four phases
/// (including the **server restart** that makes `read_cold` cold), stop.
#[test]
fn a_postgres_micro_row_runs_against_a_live_server() {
    if !available("initdb") {
        eprintln!("SKIP: initdb is not on PATH (run under `nix develop`)");
        return;
    }
    use wavedb_bench::systems::drivers::postgres::{
        Factory, init, start, stop,
    };

    let scratch = Scratch::new("pg");
    init(&scratch.0).expect("initdb");
    let server = start(&scratch.0, "off").expect("start");
    let shared = Arc::new(Mutex::new(Some(server)));

    let factory = Factory {
        socket_dir: scratch.0.clone(),
        create: true,
        sync: "off",
        server: Arc::clone(&shared),
    };
    assert_eq!(factory.consumers(), 1);

    let phases = run(&factory, workload()).expect("run");

    assert_eq!(phases.len(), 4);
    assert_eq!(phases[0].name, "insert");
    assert_eq!(phases[0].samples.len(), 300, "insert");
    assert_eq!(phases[1].samples.len(), 150, "read_hot");
    assert_eq!(phases[2].samples.len(), 150, "read_cold");
    assert_eq!(phases[3].samples.len(), 150, "update");
    assert!(phases.iter().all(|p| p.wall_ns > 0));

    // The restart happened and left a usable server: the row would have died
    // in `read_cold` otherwise, and this is the process that is now running.
    let left = shared.lock().expect("lock").take().expect("a server");
    stop(left, &scratch.0).expect("stop");
}

/// A full micro row against a real MySQL, restart and all.
#[test]
fn a_mysql_micro_row_runs_against_a_live_server() {
    if !available("mysqld") {
        eprintln!("SKIP: mysqld is not on PATH (peers come from flake.lock)");
        return;
    }
    use wavedb_bench::systems::drivers::mysql::{Factory, init, start, stop};

    let scratch = Scratch::new("my");
    init(&scratch.0).expect("initialize");
    let server = start(&scratch.0, "2").expect("start");
    let shared = Arc::new(Mutex::new(Some(server)));

    let factory = Factory {
        dir: scratch.0.clone(),
        database: "bench".into(),
        create: true,
        flush: "2",
        server: Arc::clone(&shared),
    };
    let phases = run(&factory, workload()).expect("run");

    assert_eq!(phases.len(), 4);
    assert_eq!(phases[0].samples.len(), 300, "insert");
    assert_eq!(phases[2].samples.len(), 150, "read_cold");
    assert!(phases.iter().all(|p| p.wall_ns > 0));

    let left = shared.lock().expect("lock").take().expect("a server");
    stop(left, &scratch.0).expect("stop");
}

/// A full micro row against a real MongoDB — the reference peer.
#[test]
fn a_mongodb_micro_row_runs_against_a_live_server() {
    if !available("mongod") {
        eprintln!("SKIP: mongod is not on PATH (peers come from flake.lock)");
        return;
    }
    use wavedb_bench::systems::drivers::mongodb::{Factory, port, start, stop};

    let scratch = Scratch::new("mg");
    std::fs::create_dir_all(scratch.0.join("data")).expect("dbpath");
    let p = port();
    let server = start(&scratch.0, p).expect("start");
    let shared = Arc::new(Mutex::new(Some(server)));

    let factory = Factory {
        dir: scratch.0.clone(),
        port: p,
        journal: false,
        create: true,
        server: Arc::clone(&shared),
    };
    let phases = run(&factory, workload()).expect("run");

    assert_eq!(phases.len(), 4);
    assert_eq!(phases[0].samples.len(), 300, "insert");
    assert_eq!(phases[2].samples.len(), 150, "read_cold");

    let left = shared.lock().expect("lock").take().expect("a server");
    stop(left, &scratch.0).expect("stop");
}
