//! Borrowed views (RFC 0076): a view is the decode's walk without its
//! copies, so it reads back exactly what the decode would, raises the same
//! errors on the same bytes, and borrows the buffer rather than copying it.

use core::fmt::Debug;

use wavedb_wire::{
    Error, ListRef, WaveWire, Wire, from_wire, to_wire, view_wire,
};

/// `from_ref(view(encode(x))) == x`, and the view agrees with the decode.
fn through_view<T: WaveWire + PartialEq + Debug>(value: &T) {
    let bytes = to_wire(value);
    let view = view_wire::<T>(&bytes).expect("view");
    assert_eq!(T::from_ref(view), *value);
    assert_eq!(from_wire::<T>(&bytes).expect("decode"), *value);
}

/// The view and the decode of `bytes` fail alike.
fn both_fail<T: WaveWire + Debug>(bytes: &[u8], expected: &Error) {
    assert_eq!(from_wire::<T>(bytes).unwrap_err(), *expected, "decode");
    assert_eq!(view_wire::<T>(bytes).unwrap_err(), *expected, "view");
}

/// Whether `inner` lies within `outer`'s bytes: borrowed, not copied.
fn within(outer: &[u8], inner: &[u8]) -> bool {
    let o = outer.as_ptr_range();
    let i = inner.as_ptr_range();
    o.start <= i.start && i.end <= o.end
}

#[derive(WaveWire, PartialEq, Debug)]
struct Person {
    id: u64,
    name: String,
    avatar: Vec<u8>,
    scores: Vec<u32>,
    tags: Vec<String>,
    friend: Option<Box2>,
}

#[derive(WaveWire, PartialEq, Debug)]
struct Box2(u16, String);

#[derive(WaveWire, PartialEq, Debug)]
struct Empty;

#[derive(WaveWire, PartialEq, Debug)]
enum Colour {
    Red,
    Green,
}

#[derive(WaveWire, PartialEq, Debug)]
enum Shape {
    Dot,
    Circle(u32),
    Label { text: String, at: (i32, i32) },
}

// A parameter the twin must carry through, and that is not `Clone`.
#[derive(WaveWire, PartialEq, Debug)]
struct Holder<T: WaveWire> {
    inner: T,
    note: String,
}

fn person() -> Person {
    Person {
        id: 7,
        name: "Ada".into(),
        avatar: vec![1, 2, 3, 250],
        scores: vec![10, 20, 30],
        tags: vec!["a".into(), String::new(), "ccc".into()],
        friend: Some(Box2(9, "Bob".into())),
    }
}

#[test]
fn every_builtin_comes_back_through_its_view() {
    through_view(&0u8);
    through_view(&u128::MAX);
    through_view(&-7i32);
    through_view(&2.5f64);
    through_view(&true);
    through_view(&'🌊');
    through_view(&());
    through_view(&String::new());
    through_view(&"wave".to_string());
    through_view(&vec![0xA5u8; 1000]);
    through_view(&vec![u64::MAX, 0, 42]);
    through_view(&Vec::<i16>::new());
    through_view(&vec![true, false]);
    through_view(&vec!["a".to_string(), String::new()]);
    through_view(&vec![vec![1u8], vec![], vec![2, 3]]);
    through_view(&Option::<u64>::None);
    through_view(&Some("x".to_string()));
    through_view(&vec![Some(1u8), None]);
    through_view(&[1u32, 2, 3]);
    through_view(&["a".to_string(), "bb".to_string()]);
    through_view(&[0u8; 0]);
    through_view(&(1u8, "x".to_string(), 9u64));
    through_view(&(vec![1u16, 2], 'z', Option::<u32>::None, true));
}

#[test]
fn every_derived_shape_comes_back_through_its_twin() {
    through_view(&person());
    through_view(&Person {
        friend: None,
        ..person()
    });
    through_view(&Box2(0, String::new()));
    through_view(&Empty);
    through_view(&Colour::Green);
    through_view(&Shape::Dot);
    through_view(&Shape::Circle(5));
    through_view(&Shape::Label {
        text: "here".into(),
        at: (-1, 2),
    });
    through_view(&Holder {
        inner: person(),
        note: "n".into(),
    });
    through_view(&vec![Shape::Dot, Shape::Circle(1)]);
}

#[test]
fn a_twin_borrows_its_strings_and_bytes_from_the_buffer() {
    let bytes = to_wire(&person());
    let p = view_wire::<Person>(&bytes).expect("view");
    assert_eq!(p.id, 7);
    assert_eq!(p.name, "Ada");
    assert!(within(&bytes, p.name.as_bytes()));
    assert_eq!(p.avatar, [1, 2, 3, 250]);
    assert!(within(&bytes, p.avatar));
    let friend = p.friend.expect("a friend");
    assert_eq!((friend.0, friend.1), (9, "Bob"));
    assert!(within(&bytes, friend.1.as_bytes()));
    let tags: Vec<&str> = p.tags.iter().collect();
    assert_eq!(tags, ["a", "", "ccc"]);
    assert!(tags.iter().all(|t| within(&bytes, t.as_bytes())));
}

#[test]
fn a_twin_goes_back_to_owned_by_from_and_a_wire_by_try_from() {
    let bytes = to_wire(&person());
    let p = view_wire::<Person>(&bytes).expect("view");
    assert_eq!(Person::from(p), person());
    let owned: Person = p.into();
    assert_eq!(owned, person());
    let wire = Wire::<Person>::encode(&person());
    let owned: Person = wire.try_into().expect("decode");
    assert_eq!(owned, person());
    // Bad bytes are an error to convert, not a panic.
    let bad = Wire::<Person>::new(Box::new([1, 2]), 0);
    assert_eq!(Person::try_from(bad), Err(Error::UnexpectedEof));
    // A field alone comes back with what std already has.
    assert_eq!(p.name.to_owned(), "Ada");
    assert_eq!(p.avatar.to_vec(), [1, 2, 3, 250]);
    assert_eq!(p.scores.to_vec(), [10, 20, 30]);
    let shape = Shape::Label {
        text: "t".into(),
        at: (3, 4),
    };
    let bytes = to_wire(&shape);
    assert_eq!(
        Shape::from(view_wire::<Shape>(&bytes).expect("view")),
        shape
    );
    assert_eq!(view_wire::<Colour>(&[1]).expect("view"), ColourRef::Green);
}

#[test]
fn a_twin_prints_its_fields() {
    let bytes = to_wire(&Shape::Label {
        text: "t".into(),
        at: (3, 4),
    });
    let shape = view_wire::<Shape>(&bytes).expect("view");
    assert_eq!(format!("{shape:?}"), r#"Label { text: "t", at: (3, 4) }"#);
    let bytes = to_wire(&Box2(1, "b".into()));
    let b = view_wire::<Box2>(&bytes).expect("view");
    assert_eq!(format!("{b:?}"), r#"Box2Ref(1, "b")"#);
    let bytes = to_wire(&vec![1u32, 2]);
    let list = view_wire::<Vec<u32>>(&bytes).expect("view");
    assert_eq!(format!("{list:?}"), "[1, 2]");
}

#[test]
fn a_list_reads_by_index_and_in_order() {
    let numbers = (0..100u64).collect::<Vec<_>>();
    let bytes = to_wire(&numbers);
    let list: ListRef<'_, u64> = view_wire::<Vec<u64>>(&bytes).expect("view");
    assert_eq!(list.len(), 100);
    assert_eq!(list.get(0), Some(0));
    assert_eq!(list.get(99), Some(99));
    assert_eq!(list.get(100), None);
    assert_eq!(list.iter().len(), 100);
    assert!(list.iter().eq(0..100));
    // Elements with heap bytes are found by walking.
    let words = vec!["x".to_string(), "yy".to_string(), "zzz".to_string()];
    let bytes = to_wire(&words);
    let list = view_wire::<Vec<String>>(&bytes).expect("view");
    assert_eq!(list.get(2), Some("zzz"));
    assert_eq!(list.get(3), None);
    assert!(!list.is_empty());
    assert_eq!((&list).into_iter().last(), Some("zzz"));
}

#[test]
fn a_short_buffer_is_eof_for_both() {
    both_fail::<u64>(&[0; 4], &Error::UnexpectedEof);
    both_fail::<Person>(&[], &Error::UnexpectedEof);
    let bytes = to_wire(&person());
    both_fail::<Person>(&bytes[..bytes.len() - 1], &Error::UnexpectedEof);
    // A region of 3 bytes declared for a Vec<u16>.
    both_fail::<Vec<u16>>(&[3, 0, 0, 0, 1, 2, 3], &Error::UnexpectedEof);
}

#[test]
fn bad_bytes_fail_the_view_as_they_fail_the_decode() {
    both_fail::<String>(&[2, 0, 0, 0, 0xFF, 0xFE], &Error::Utf8);
    both_fail::<bool>(&[2], &Error::InvalidBool(2));
    both_fail::<char>(&0xD800u32.to_le_bytes(), &Error::InvalidChar(0xD800));
    both_fail::<Colour>(&[2], &Error::InvalidTag(2));
    // A field-carrying enum: tag 9, zero payload length.
    both_fail::<Shape>(&[9, 0, 0, 0, 0], &Error::InvalidTag(9));
    // A bad element deep in a list is found by the view, not by a read.
    let mut bytes = to_wire(&vec!["ok".to_string(), "no".to_string()]);
    let last = bytes.len() - 1;
    bytes[last] = 0xFF;
    both_fail::<Vec<String>>(&bytes, &Error::Utf8);
    both_fail::<[bool; 2]>(&[1, 7], &Error::InvalidBool(7));
}

/// Units of no bytes can never use up a region that has some: both walks
/// refuse it rather than spin.
#[test]
fn a_list_of_empty_units_over_a_full_region_is_eof() {
    both_fail::<Vec<()>>(&[1, 0, 0, 0, 0], &Error::UnexpectedEof);
}

#[test]
fn a_wire_views_past_its_envelope() {
    let mut bytes = b"HEAD".to_vec();
    bytes.extend(to_wire(&person()));
    let wire = Wire::<Person>::new(bytes.into_boxed_slice(), 4);
    let p = wire.view().expect("view");
    assert_eq!(p.name, "Ada");
    assert!(within(wire.bytes(), p.name.as_bytes()));
    assert_eq!(wire.decode().expect("decode"), person());
    assert_eq!(&wire.bytes()[..4], b"HEAD");
    let (raw, start) = wire.into_parts();
    assert_eq!((raw.len() > 4, start), (true, 4));
}

#[test]
fn a_wire_starting_past_its_end_is_eof() {
    let wire = Wire::<u8>::new(Box::new([1, 2]), 3);
    assert_eq!(wire.view().unwrap_err(), Error::UnexpectedEof);
    assert_eq!(wire.decode().unwrap_err(), Error::UnexpectedEof);
}

#[test]
fn a_wire_encodes_and_moves_between_threads_whatever_it_holds() {
    const fn send_sync<T: Send + Sync>() {}
    // It claims no `T`, so even one that is neither is fine to move.
    send_sync::<Wire<std::rc::Rc<u8>>>();
    let wire = Wire::encode(&person());
    let back = std::thread::spawn(move || wire.view().map(Person::from))
        .join()
        .expect("thread");
    assert_eq!(back, Ok(person()));
}

/// A `Wire<T>` encodes as `T` exactly, so a reply of read records is a reply
/// of values: what one side writes as `Vec<Wire<T>>` the other reads as
/// `Vec<T>`, and the other way round.
#[test]
fn a_wire_is_its_records_bytes_on_the_wire() {
    let people = vec![
        person(),
        Person {
            friend: None,
            ..person()
        },
    ];
    let wires: Vec<Wire<Person>> = people.iter().map(Wire::encode).collect();
    assert_eq!(to_wire(&wires), to_wire(&people));
    let back: Vec<Wire<Person>> = from_wire(&to_wire(&people)).expect("decode");
    assert_eq!(back.len(), 2);
    assert_eq!(back[1].decode().expect("decode"), people[1]);
    // Kept as the bytes it took, no more: the body is exactly the record.
    assert_eq!(back[0].body(), to_wire(&people[0]).as_slice());
    // A view through the list reads each record in place.
    let bytes = to_wire(&people);
    let list = view_wire::<Vec<Wire<Person>>>(&bytes).expect("view");
    assert_eq!(list.get(0).expect("first").name, "Ada");
}

/// Decoding a wire checks what it keeps, as decoding the value would.
#[test]
fn a_wire_decodes_only_bytes_its_type_would() {
    both_fail::<Wire<String>>(&[2, 0, 0, 0, 0xFF, 0xFE], &Error::Utf8);
    both_fail::<Wire<bool>>(&[2], &Error::InvalidBool(2));
    both_fail::<Wire<Person>>(&[0; 3], &Error::UnexpectedEof);
}
