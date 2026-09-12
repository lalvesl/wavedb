//! Reading a stored row back.
//!
//! Missing fields default rather than refuse. The corpus is append-only and a
//! row outlives the build that wrote it, so a reader that insisted on a field
//! added last week would make every older row unreadable — and the evolution
//! table, whose whole job is to reach back across commits, is exactly the
//! consumer that would break.
//!
//! What is **not** tolerant is the schema tag and the digest: a file whose
//! digest disagrees with the key it contains has been edited or was written by
//! something else, and serving it as a measurement would launder that.

use super::record::{FootprintRecord, PhaseRecord, RowKey, RowRecord, SCHEMA};
use crate::json::{Value, parse};

/// Decode a stored row.
///
/// # Errors
/// Malformed JSON, a schema tag this build does not write, a key that cannot
/// be read, or a digest that does not match the key beside it.
pub fn from_json(text: &str) -> Result<RowRecord, String> {
    let v = parse(text).map_err(|e| e.to_string())?;
    let schema = str_at(&v, "schema");
    if schema != SCHEMA {
        return Err(format!("schema {schema:?}, expected {SCHEMA:?}"));
    }
    let key = key_from(v.get("key").ok_or("no key object")?)?;

    // The digest is derived, so the stored copy is a checksum rather than
    // data: recomputing it is how a hand-edited row is caught.
    let stored = str_at(&v, "digest");
    let computed = key.digest();
    if stored != computed {
        return Err(format!(
            "digest {stored} does not match the key it names ({computed})"
        ));
    }

    let prov = v.get("provenance");
    let seed_path = match str_at(&v, "seed_path") {
        s if s.is_empty() => None,
        s => Some(s),
    };
    Ok(RowRecord {
        key,
        timestamp: str_at(&v, "timestamp"),
        dirty: bool_at(prov, "dirty"),
        caged: bool_at(prov, "caged"),
        forced: bool_at(prov, "forced"),
        bracket: str_at(&v, "bracket"),
        compression: str_at(&v, "compression"),
        retains_history: bool_at(Some(&v), "retains_history"),
        settings: settings_from(v.get("settings")),
        phases: v
            .get("phases")
            .and_then(Value::as_arr)
            .unwrap_or_default()
            .iter()
            .map(phase_from)
            .collect(),
        footprints: v
            .get("footprints")
            .and_then(Value::as_arr)
            .unwrap_or_default()
            .iter()
            .map(footprint_from)
            .collect(),
        live_records: num_at(&v, "live_records"),
        logical_bytes: num_at(&v, "logical_bytes"),
        notes: v
            .get("notes")
            .and_then(Value::as_arr)
            .unwrap_or_default()
            .iter()
            .filter_map(|n| n.as_str().map(str::to_string))
            .collect(),
        seed_path,
        materialise_ms: num_at(&v, "materialise_ms"),
    })
}

fn key_from(v: &Value) -> Result<RowKey, String> {
    let system = str_at(v, "system");
    if system.is_empty() {
        return Err("key has no system".into());
    }
    let sha = str_at(v, "wavedb_git_sha");
    Ok(RowKey {
        system,
        system_version: str_at(v, "system_version"),
        variant: str_at(v, "variant"),
        durability: str_at(v, "durability"),
        workload: str_at(v, "workload"),
        tier: str_at(v, "tier"),
        dataset_revision: num_at(v, "dataset_revision"),
        generator_seed: num_at(v, "generator_seed"),
        consumers: num_at(v, "consumers") as u32,
        host_key: str_at(v, "host_key"),
        cage_revision: num_at(v, "cage_revision") as u32,
        // Empty is how the encoder spells `None`, and the digest folds it the
        // same way — so this round-trips rather than turning a peer row into a
        // WaveDB row with a blank SHA.
        wavedb_git_sha: if sha.is_empty() { None } else { Some(sha) },
    })
}

fn phase_from(v: &Value) -> PhaseRecord {
    PhaseRecord {
        name: str_at(v, "name"),
        count: num_at(v, "count"),
        wall_ns: num_at(v, "wall_ns"),
        total_ns: num_at(v, "total_ns"),
        p50_ns: num_at(v, "p50_ns"),
        p95_ns: num_at(v, "p95_ns"),
        p99_ns: num_at(v, "p99_ns"),
        max_ns: num_at(v, "max_ns"),
        bytes_written: num_at(v, "bytes_written"),
        read_bytes: num_at(v, "read_bytes"),
        rchar: num_at(v, "rchar"),
    }
}

fn footprint_from(v: &Value) -> (String, FootprintRecord) {
    (
        str_at(v, "point"),
        FootprintRecord {
            apparent_bytes: num_at(v, "apparent_bytes"),
            allocated_bytes: num_at(v, "allocated_bytes"),
            log_bytes: num_at(v, "log_bytes"),
            files: num_at(v, "files"),
        },
    )
}

fn settings_from(v: Option<&Value>) -> Vec<(String, String)> {
    match v {
        Some(Value::Obj(pairs)) => pairs
            .iter()
            .filter_map(|(k, val)| {
                val.as_str().map(|s| (k.clone(), s.to_string()))
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn str_at(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn num_at(v: &Value, key: &str) -> u64 {
    v.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn bool_at(v: Option<&Value>, key: &str) -> bool {
    v.and_then(|v| v.get(key))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::from_json;
    use crate::corpus::{FootprintRecord, PhaseRecord, RowKey, RowRecord};

    fn wavedb_key() -> RowKey {
        RowKey {
            system: "wavedb".into(),
            system_version: "0.1.0".into(),
            variant: "multi".into(),
            durability: "durable".into(),
            workload: "shop".into(),
            tier: "large".into(),
            dataset_revision: 1,
            generator_seed: 42,
            consumers: 3,
            host_key: "i5-8300h-4c-500m-btrfs-e636".into(),
            cage_revision: 1,
            wavedb_git_sha: Some("04085ec".into()),
        }
    }

    fn peer_key() -> RowKey {
        RowKey {
            system: "postgres".into(),
            system_version: "18.1".into(),
            variant: "-".into(),
            wavedb_git_sha: None,
            ..wavedb_key()
        }
    }

    fn record(key: RowKey) -> RowRecord {
        RowRecord {
            key,
            timestamp: "2026-09-04T03-54Z".into(),
            dirty: true,
            caged: true,
            forced: false,
            bracket: "embedded".into(),
            compression: "zstd dictionaries".into(),
            retains_history: true,
            settings: vec![("relax_window".into(), "0ms".into())],
            phases: vec![PhaseRecord {
                name: "checkout".into(),
                count: 200,
                wall_ns: 2_000_000_000,
                total_ns: 5_900_000_000,
                p50_ns: 28_000_000,
                p95_ns: 41_000_000,
                p99_ns: 90_000_000,
                max_ns: 1_787_625_091_956_123_457,
                bytes_written: 4096,
                read_bytes: 8192,
                rchar: 1_048_576,
            }],
            footprints: vec![(
                "settled".into(),
                FootprintRecord {
                    apparent_bytes: 23_000_000,
                    allocated_bytes: 23_400_000,
                    log_bytes: 58,
                    files: 3,
                },
            )],
            live_records: 860_384,
            logical_bytes: 27_500_000,
            notes: vec!["retains every superseded version".into()],
            seed_path: Some("/nix/store/abc-bench-seed".into()),
            materialise_ms: 91,
        }
    }

    #[test]
    fn a_record_survives_the_round_trip() {
        let original = record(wavedb_key());
        let back = from_json(&original.to_json()).expect("decode");
        assert_eq!(back, original);
    }

    /// The max latency here is a `key_nanos`-scale instant, which is the value
    /// an `f64` number path would have silently rounded.
    #[test]
    fn a_nanosecond_field_beyond_f64_precision_round_trips() {
        let original = record(wavedb_key());
        let back = from_json(&original.to_json()).expect("decode");
        assert_eq!(back.phases[0].max_ns, 1_787_625_091_956_123_457);
    }

    /// The reuse mechanism, asserted rather than described: two rows differing
    /// **only** in the WaveDB SHA are the same peer row and two different
    /// WaveDB rows.
    #[test]
    fn the_sha_moves_a_wavedb_digest_and_not_a_peer_one() {
        let mut a = wavedb_key();
        let before = a.digest();
        a.wavedb_git_sha = Some("deadbee".into());
        assert_ne!(before, a.digest(), "a WaveDB row must be remeasured");

        let mut p = peer_key();
        let peer_before = p.digest();
        p.wavedb_git_sha = None;
        assert_eq!(peer_before, p.digest(), "a peer row must be reusable");
    }

    /// `None` is spelled as an empty string on the wire, so it has to come
    /// back as `None` — a peer row that decoded as `Some("")` would start
    /// being remeasured for no reason.
    #[test]
    fn a_peer_row_decodes_without_a_sha() {
        let back = from_json(&record(peer_key()).to_json()).expect("decode");
        assert_eq!(back.key.wavedb_git_sha, None);
        assert!(!back.key.is_wavedb());
    }

    /// Consumer count is in the identity: three consumers is a different
    /// measurement from one, not a better one.
    #[test]
    fn the_consumer_count_moves_the_digest() {
        let mut k = wavedb_key();
        let three = k.digest();
        k.consumers = 1;
        assert_ne!(three, k.digest());
    }

    /// Concatenating the fields without a separator would let neighbouring
    /// values trade characters and collide.
    #[test]
    fn adjacent_fields_cannot_trade_characters() {
        let mut a = wavedb_key();
        a.system = "wave".into();
        a.system_version = "db0.1.0".into();
        let mut b = wavedb_key();
        b.system = "wavedb".into();
        b.system_version = "0.1.0".into();
        assert_ne!(a.digest(), b.digest());
    }

    #[test]
    fn a_hand_edited_row_is_refused() {
        let text = record(wavedb_key())
            .to_json()
            .replace("\"tier\": \"large\"", "\"tier\": \"huge\"");
        let err = from_json(&text).expect_err("must refuse");
        assert!(err.contains("does not match"), "{err}");
    }

    #[test]
    fn a_foreign_schema_is_refused() {
        let text = record(wavedb_key())
            .to_json()
            .replace("wavedb-bench/2", "wavedb-bench/1");
        let err = from_json(&text).expect_err("must refuse");
        assert!(err.contains("schema"), "{err}");
    }

    /// A field this build has never heard of must not make the row
    /// unreadable — the corpus outlives the code that wrote it.
    #[test]
    fn an_unknown_field_does_not_break_a_row() {
        let text = record(wavedb_key())
            .to_json()
            .replace("\"bracket\":", "\"invented_later\": 7,\n  \"bracket\":");
        assert!(from_json(&text).is_ok());
    }

    #[test]
    fn a_row_is_filed_under_its_digest() {
        let r = record(wavedb_key());
        assert_eq!(r.file_name(), format!("{}.json", r.key.digest()));
    }
}
