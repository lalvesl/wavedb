//! The showcase node: serves the schema's `REGISTRY` on a fixed loopback
//! port with a **persistent** data directory, so you can kill it, restart
//! it, and watch the journal replay bring everything back.
//!
//! ```sh
//! cargo run -p showcase --example node
//! ```
//!
//! `SHOWCASE_ADDR` overrides the bind address; `SHOWCASE_DATA` the data
//! directory (default: `<tmp>/wavedb-showcase-node`). The signing secret
//! is the fixed [`showcase::DEMO_SECRET`] so the client example can mint
//! its own access token — demo plumbing, exactly like the e2e tests'.

use std::io::Write as _;

use showcase::{DEMO_SECRET, REGISTRY};
use wavedb_quick_node::Server;

fn main() {
    wavedb_platform::carrier::block_on(async {
        let addr = std::env::var("SHOWCASE_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:4780".into());
        let data = std::env::var("SHOWCASE_DATA").map_or_else(
            |_| std::env::temp_dir().join("wavedb-showcase-node"),
            Into::into,
        );
        println!("data dir: {}", data.display());

        // `.registry(REGISTRY)` alone opens the engine: `expose_server!` also
        // emits the StorageRegistry, so the listed types' storage slots are
        // exactly the declared surface.
        let bound = Server::new(REGISTRY)
            .secret(DEMO_SECRET)
            .data_dir(&data)
            .bind(addr.as_str())
            .await
            .expect("open the engine and bind (is another node running?)");
        let addr = bound.local_addr().expect("read the bound address");
        println!("LISTENING {addr}");
        println!(
            "kill me mid-demo and restart: the journal replays everything"
        );
        std::io::stdout().flush().expect("flush the address line");
        bound.run().await.expect("serve");
    });
}
