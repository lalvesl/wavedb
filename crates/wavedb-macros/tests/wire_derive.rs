//! `#[derive(WaveWire)]` round-trips through the core `WaveWire` format.

// The trait and the derive macro share the name `WaveWire` (like `Clone`), and
// here two crates export a `WaveWire` derive (the `wavedb-wire` one re-exported
// through core, and the `wavedb-macros` one under test). Import only the derive
// being tested by name; reference the trait fully-qualified to avoid the clash.
use wavedb_core::wire::{from_wire, to_wire};
use wavedb_macros::WaveWire;

#[derive(WaveWire, Debug, PartialEq, Clone)]
struct Named {
    a: u32,
    b: String,
    c: Vec<u16>,
    d: Option<u64>,
}

#[derive(WaveWire, Debug, PartialEq, Clone)]
struct Tuple(u8, String, bool);

#[derive(WaveWire, Debug, PartialEq, Clone)]
struct Unit;

fn roundtrip<T: wavedb_core::wire::WaveWire + PartialEq + std::fmt::Debug>(
    value: &T,
) {
    let bytes = to_wire(value);
    assert_eq!(
        bytes.len(),
        <T as wavedb_core::wire::WaveWire>::STACK_SIZE
            + wavedb_core::wire::WaveWire::heap_size(value),
    );
    let back: T = from_wire(&bytes).expect("decode");
    assert_eq!(&back, value);
}

#[test]
fn named_struct_roundtrips() {
    roundtrip(&Named {
        a: 0xDEAD_BEEF,
        b: "wavedb".into(),
        c: vec![1, 2, 3],
        d: Some(42),
    });
    roundtrip(&Named {
        a: 0,
        b: String::new(),
        c: vec![],
        d: None,
    });
}

#[test]
fn tuple_struct_roundtrips() {
    roundtrip(&Tuple(7, "x".into(), true));
}

#[test]
fn unit_struct_roundtrips() {
    assert_eq!(<Unit as wavedb_core::wire::WaveWire>::STACK_SIZE, 0);
    roundtrip(&Unit);
}

#[test]
fn named_stack_size_is_field_sum() {
    // u32(4) + String(4) + Vec(4) + Option(1) = 13
    assert_eq!(<Named as wavedb_core::wire::WaveWire>::STACK_SIZE, 13);
}

/// The twin comes through the `wavedb_core::wire` paths too: a schema crate
/// needs no direct `wavedb-wire` dependency for it.
#[test]
fn the_twin_views_and_comes_back() {
    let value = Named {
        a: 3,
        b: "wavedb".into(),
        c: vec![4, 5],
        d: Some(6),
    };
    let bytes = to_wire(&value);
    let view: NamedRef<'_> =
        wavedb_core::wire::view_wire::<Named>(&bytes).expect("view");
    assert_eq!((view.a, view.b, view.d), (3, "wavedb", Some(6)));
    assert_eq!(view.c.to_vec(), [4, 5]);
    assert_eq!(Named::from(view), value);
    let wire = wavedb_core::wire::Wire::<Named>::encode(&value);
    assert_eq!(Named::try_from(wire), Ok(value));
    let tuple = to_wire(&Tuple(1, "t".into(), false));
    let view = wavedb_core::wire::view_wire::<Tuple>(&tuple).expect("view");
    assert_eq!(Tuple::from(view), Tuple(1, "t".into(), false));
    assert_eq!(
        wavedb_core::wire::view_wire::<Unit>(&[]).expect("view"),
        UnitRef
    );
}
