//! Reading JSON back.
//!
//! ## Why a value tree and not a shape-directed reader
//!
//! Parsing straight into `RowRecord` would be fewer lines today and a trap
//! tomorrow: the corpus is append-only and rows written by an older build must
//! stay readable, so a reader has to be able to see a field it does not know
//! and carry on. A value tree does that for free; a shape-directed one turns
//! every added field into a parse error somewhere.
//!
//! ## Numbers keep their lexeme
//!
//! [`Value::Num`] holds the **text** rather than an `f64`, and that is not
//! fastidiousness. WaveDB instants come from `platform::time::key_nanos()` —
//! milliseconds × 1e6 plus a counter, so ~1.8e18 — and `f64` carries 53 bits
//! of mantissa, about 9.0e15. Parsing an instant through a float would round
//! it silently and the id would come back **wrong**, not approximate. So the
//! lexeme is kept and [`Value::as_u64`] parses it as an integer.

use std::fmt;

/// A parsed JSON value.
///
/// Objects keep their pairs in a `Vec` rather than a map: a record has a
/// handful of keys, insertion order is what makes the file diff cleanly, and
/// a linear scan over ten entries beats hashing them.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    /// The numeric lexeme exactly as it was written.
    Num(String),
    Str(String),
    Arr(Vec<Value>),
    Obj(Vec<(String, Value)>),
}

impl Value {
    /// The value at `key`, if this is an object that has one.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Self> {
        match self {
            Self::Obj(pairs) => {
                pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v)
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(s) => Some(s),
            _ => None,
        }
    }

    /// The value as an integer, parsed from the lexeme rather than through a
    /// float — see the module note on instants.
    #[must_use]
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Num(lex) => lex.parse().ok(),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Num(lex) => lex.parse().ok(),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_arr(&self) -> Option<&[Self]> {
        match self {
            Self::Arr(items) => Some(items),
            _ => None,
        }
    }
}

/// Where a parse gave up, and on what.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub at: usize,
    pub what: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "json: {} at byte {}", self.what, self.at)
    }
}

/// Parse one JSON document. Trailing whitespace is fine; trailing content is
/// not — a truncated write that left two documents in one file is a fault
/// worth reporting rather than half-reading.
///
/// # Errors
/// Malformed input, with the byte offset it failed at.
pub fn parse(text: &str) -> Result<Value, Error> {
    let mut p = Parser {
        src: text.as_bytes(),
        at: 0,
    };
    p.ws();
    let v = p.value()?;
    p.ws();
    if p.at < p.src.len() {
        return Err(p.err("trailing content"));
    }
    Ok(v)
}

struct Parser<'a> {
    src: &'a [u8],
    at: usize,
}

impl Parser<'_> {
    fn value(&mut self) -> Result<Value, Error> {
        match self.peek() {
            Some(b'{') => self.object(),
            Some(b'[') => self.array(),
            Some(b'"') => Ok(Value::Str(self.string()?)),
            Some(b't') => self.lit("true", Value::Bool(true)),
            Some(b'f') => self.lit("false", Value::Bool(false)),
            Some(b'n') => self.lit("null", Value::Null),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.number(),
            Some(_) => Err(self.err("unexpected character")),
            None => Err(self.err("unexpected end of input")),
        }
    }

    fn object(&mut self) -> Result<Value, Error> {
        self.at += 1; // '{'
        let mut pairs = Vec::new();
        self.ws();
        if self.peek() == Some(b'}') {
            self.at += 1;
            return Ok(Value::Obj(pairs));
        }
        loop {
            self.ws();
            let key = self.string()?;
            self.ws();
            if self.peek() != Some(b':') {
                return Err(self.err("expected ':'"));
            }
            self.at += 1;
            self.ws();
            pairs.push((key, self.value()?));
            self.ws();
            match self.peek() {
                Some(b',') => self.at += 1,
                Some(b'}') => {
                    self.at += 1;
                    return Ok(Value::Obj(pairs));
                }
                _ => return Err(self.err("expected ',' or '}'")),
            }
        }
    }

    fn array(&mut self) -> Result<Value, Error> {
        self.at += 1; // '['
        let mut items = Vec::new();
        self.ws();
        if self.peek() == Some(b']') {
            self.at += 1;
            return Ok(Value::Arr(items));
        }
        loop {
            self.ws();
            items.push(self.value()?);
            self.ws();
            match self.peek() {
                Some(b',') => self.at += 1,
                Some(b']') => {
                    self.at += 1;
                    return Ok(Value::Arr(items));
                }
                _ => return Err(self.err("expected ',' or ']'")),
            }
        }
    }

    fn string(&mut self) -> Result<String, Error> {
        // End-of-input is called out separately: a row file that was
        // truncated by a killed run is the failure this reader will actually
        // meet, and "expected a string" would send the reader looking for a
        // malformed key instead of a short file.
        match self.peek() {
            Some(b'"') => {}
            Some(_) => return Err(self.err("expected a string")),
            None => return Err(self.err("unexpected end of input")),
        }
        self.at += 1;
        let mut out = String::new();
        loop {
            match self.next() {
                None => return Err(self.err("unterminated string")),
                Some(b'"') => return Ok(out),
                Some(b'\\') => self.escape(&mut out)?,
                Some(c) => {
                    // Multi-byte UTF-8 arrives one byte at a time; collect the
                    // continuation bytes and decode the sequence whole.
                    let start = self.at - 1;
                    let len = utf8_len(c);
                    self.at = start + len;
                    let Some(chunk) = self.src.get(start..self.at) else {
                        return Err(self.err("truncated utf-8"));
                    };
                    let Ok(s) = std::str::from_utf8(chunk) else {
                        return Err(self.err("invalid utf-8"));
                    };
                    out.push_str(s);
                }
            }
        }
    }

    fn escape(&mut self, out: &mut String) -> Result<(), Error> {
        match self.next() {
            Some(b'"') => out.push('"'),
            Some(b'\\') => out.push('\\'),
            Some(b'/') => out.push('/'),
            Some(b'n') => out.push('\n'),
            Some(b't') => out.push('\t'),
            Some(b'r') => out.push('\r'),
            Some(b'b') => out.push('\u{8}'),
            Some(b'f') => out.push('\u{c}'),
            Some(b'u') => {
                let Some(hex) = self.src.get(self.at..self.at + 4) else {
                    return Err(self.err("truncated \\u escape"));
                };
                self.at += 4;
                let Ok(text) = std::str::from_utf8(hex) else {
                    return Err(self.err("bad \\u escape"));
                };
                let Ok(code) = u32::from_str_radix(text, 16) else {
                    return Err(self.err("bad \\u escape"));
                };
                // Lone surrogates are the one code point `char` refuses. The
                // writer never emits one (it escapes only control characters),
                // so this is a corrupt file rather than a shape to support.
                let Some(c) = char::from_u32(code) else {
                    return Err(self.err("\\u escape is not a character"));
                };
                out.push(c);
            }
            _ => return Err(self.err("unknown escape")),
        }
        Ok(())
    }

    fn number(&mut self) -> Result<Value, Error> {
        let start = self.at;
        if self.peek() == Some(b'-') {
            self.at += 1;
        }
        while matches!(self.peek(),
            Some(c) if c.is_ascii_digit()
                || c == b'.' || c == b'e' || c == b'E'
                || c == b'+' || c == b'-')
        {
            self.at += 1;
        }
        let Some(lex) = self.src.get(start..self.at) else {
            return Err(self.err("bad number"));
        };
        let Ok(text) = std::str::from_utf8(lex) else {
            return Err(self.err("bad number"));
        };
        if text.is_empty() || text == "-" {
            return Err(self.err("bad number"));
        }
        Ok(Value::Num(text.to_string()))
    }

    fn lit(&mut self, word: &str, value: Value) -> Result<Value, Error> {
        if self.src[self.at..].starts_with(word.as_bytes()) {
            self.at += word.len();
            return Ok(value);
        }
        Err(self.err("unknown literal"))
    }

    fn ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.at += 1;
        }
    }

    const fn peek(&self) -> Option<u8> {
        if self.at < self.src.len() {
            Some(self.src[self.at])
        } else {
            None
        }
    }

    const fn next(&mut self) -> Option<u8> {
        let c = self.peek();
        if c.is_some() {
            self.at += 1;
        }
        c
    }

    fn err(&self, what: &str) -> Error {
        Error {
            at: self.at,
            what: what.to_string(),
        }
    }
}

/// How many bytes the UTF-8 sequence starting with `lead` occupies.
const fn utf8_len(lead: u8) -> usize {
    match lead {
        0xF0..=0xF7 => 4,
        0xE0..=0xEF => 3,
        0xC0..=0xDF => 2,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::{Value, parse};
    use crate::json::Json;

    #[test]
    fn an_object_keeps_its_keys_in_order() {
        let v = parse(r#"{"b": 1, "a": 2}"#).expect("parse");
        let Value::Obj(pairs) = &v else {
            panic!("not an object")
        };
        assert_eq!(pairs[0].0, "b");
        assert_eq!(pairs[1].0, "a");
        assert_eq!(v.get("a").and_then(Value::as_u64), Some(2));
    }

    /// The reason [`Value::Num`] keeps its lexeme: a `key_nanos` instant is
    /// ~1.8e18 and `f64` carries about 9.0e15, so a float round-trip would
    /// come back a *different* number rather than an imprecise one.
    #[test]
    fn a_large_instant_survives_the_round_trip_exactly() {
        let instant: u64 = 1_787_625_091_956_123_457;
        let text = format!(r#"{{"instant": {instant}}}"#);
        let v = parse(&text).expect("parse");
        assert_eq!(v.get("instant").and_then(Value::as_u64), Some(instant));
        // And the float path would not have.
        let as_float = v.get("instant").and_then(Value::as_f64).expect("f64");
        assert_ne!(as_float as u64, instant, "f64 was lossless after all?");
    }

    /// An older reader must survive a field it has never heard of, because the
    /// corpus is append-only and rows outlive the build that wrote them.
    #[test]
    fn an_unknown_field_is_carried_not_refused() {
        let v = parse(r#"{"known": 1, "invented_later": {"deep": [1,2]}}"#)
            .expect("parse");
        assert_eq!(v.get("known").and_then(Value::as_u64), Some(1));
        assert!(v.get("invented_later").is_some());
    }

    #[test]
    fn what_the_writer_emits_is_what_the_reader_accepts() {
        let mut j = Json::new();
        j.obj(None, |j| {
            j.str("system", "wavedb");
            j.num("rows", 200_000);
            j.ratio("amp", 0.93);
            j.boolean("caged", true);
            j.arr(Some("phases"), |j| {
                j.elem("insert");
                j.elem("read_hot");
            });
            j.obj(Some("nested"), |j| j.num("depth", 2));
        });
        let text = j.finish();
        let v = parse(&text).expect("the writer emitted unparseable JSON");
        assert_eq!(v.get("system").and_then(Value::as_str), Some("wavedb"));
        assert_eq!(v.get("rows").and_then(Value::as_u64), Some(200_000));
        assert_eq!(v.get("amp").and_then(Value::as_f64), Some(0.93));
        assert_eq!(v.get("caged").and_then(Value::as_bool), Some(true));
        assert_eq!(
            v.get("phases").and_then(Value::as_arr).map(<[_]>::len),
            Some(2)
        );
        assert_eq!(
            v.get("nested")
                .and_then(|n| n.get("depth"))
                .and_then(Value::as_u64),
            Some(2)
        );
    }

    /// Every escape the writer can emit has to come back as it went in.
    #[test]
    fn the_writers_escapes_round_trip() {
        let nasty = "quote\" back\\slash\nnewline\ttab \u{1}control ácido 日本";
        let mut j = Json::new();
        j.obj(None, |j| j.str("s", nasty));
        let v = parse(&j.finish()).expect("parse");
        assert_eq!(v.get("s").and_then(Value::as_str), Some(nasty));
    }

    #[test]
    fn empty_containers_parse() {
        assert_eq!(parse("{}").expect("obj"), Value::Obj(Vec::new()));
        assert_eq!(parse("[]").expect("arr"), Value::Arr(Vec::new()));
    }

    #[test]
    fn a_truncated_record_is_a_typed_failure_not_a_partial_read() {
        let err =
            parse(r#"{"system": "wavedb", "rows":"#).expect_err("must fail");
        assert!(err.what.contains("end of input"), "{err}");
    }

    #[test]
    fn two_documents_in_one_file_are_refused() {
        let err = parse("{} {}").expect_err("must fail");
        assert_eq!(err.what, "trailing content");
    }

    #[test]
    fn negative_and_fractional_numbers_parse() {
        let v = parse(r#"{"a": -12, "b": 1.5e3}"#).expect("parse");
        assert_eq!(v.get("a").and_then(Value::as_f64), Some(-12.0));
        assert_eq!(v.get("b").and_then(Value::as_f64), Some(1500.0));
    }
}
