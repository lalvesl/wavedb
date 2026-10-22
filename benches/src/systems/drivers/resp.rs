//! RESP2 — the protocol Valkey speaks — hand-rolled.
//!
//! The bench needs five reply types and one request shape (an array of bulk
//! strings), so the client is this file rather than a crate: what sits
//! between the timed window and the socket is a `BufWriter` and a
//! `BufReader`, and a reader can see all of it. It is the same stance the
//! workspace takes on its own HTTP and WebSocket halves.
//!
//! ## Pipelining is the API
//!
//! [`Resp::send`] only encodes into the write buffer; [`Resp::flush`] puts
//! everything queued on the wire in one write; [`Resp::reply`] reads one
//! answer. So a `MULTI … EXEC` transaction, or a page of `HGETALL`s, is one
//! round trip — the way an application issues it — and [`Resp::call`] is the
//! one-command case.

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

/// One reply, as the server sent it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// `+OK`, `+QUEUED`, `+PONG`.
    Status(String),
    /// `-ERR …` — the server refused; never a transport fault.
    Error(String),
    Int(i64),
    /// `None` is the nil bulk string: the key or field is absent.
    Bulk(Option<Vec<u8>>),
    /// `None` is the nil array: an aborted transaction.
    Array(Option<Vec<Self>>),
}

impl Reply {
    /// The integer, or why this is not one.
    ///
    /// # Errors
    /// Any other reply, the server's refusal included.
    pub fn int(self) -> Result<i64, String> {
        match self {
            Self::Int(n) => Ok(n),
            other => Err(format!("expected an integer, got {other:?}")),
        }
    }

    /// The array's elements, or why this is not one.
    ///
    /// # Errors
    /// Any other reply, a nil array (an aborted `EXEC`) included.
    pub fn array(self) -> Result<Vec<Self>, String> {
        match self {
            Self::Array(Some(items)) => Ok(items),
            other => Err(format!("expected an array, got {other:?}")),
        }
    }

    /// The bulk string's bytes, or why this is not one (absent included).
    ///
    /// # Errors
    /// Any other reply, the nil bulk string included.
    pub fn bulk(self) -> Result<Vec<u8>, String> {
        match self {
            Self::Bulk(Some(bytes)) => Ok(bytes),
            other => Err(format!("expected a value, got {other:?}")),
        }
    }
}

/// A connection over a unix socket.
pub struct Resp {
    reader: BufReader<UnixStream>,
    writer: BufWriter<UnixStream>,
}

impl Resp {
    /// # Errors
    /// The socket refused the connection.
    pub fn connect(path: &Path) -> Result<Self, String> {
        let stream = UnixStream::connect(path)
            .map_err(|e| format!("resp: connect {}: {e}", path.display()))?;
        let read = stream.try_clone().map_err(|e| format!("resp: {e}"))?;
        Ok(Self {
            reader: BufReader::new(read),
            writer: BufWriter::new(stream),
        })
    }

    /// Queue one command. Nothing is written until [`flush`](Self::flush).
    ///
    /// # Errors
    /// The write buffer could not drain into the socket.
    pub fn send(&mut self, args: &[&[u8]]) -> Result<(), String> {
        encode(&mut self.writer, args).map_err(|e| format!("resp: {e}"))
    }

    /// Put everything queued on the wire.
    ///
    /// # Errors
    /// The socket write failed.
    pub fn flush(&mut self) -> Result<(), String> {
        self.writer.flush().map_err(|e| format!("resp: {e}"))
    }

    /// Read one reply. A server refusal is a [`Reply::Error`], not an `Err`:
    /// inside a pipeline the caller decides which refusals matter.
    ///
    /// # Errors
    /// The connection broke or sent something that is not RESP2.
    pub fn reply(&mut self) -> Result<Reply, String> {
        read_reply(&mut self.reader)
    }

    /// One command, one round trip; a refusal is an `Err`.
    ///
    /// # Errors
    /// The connection failed, or the server refused the command.
    pub fn call(&mut self, args: &[&[u8]]) -> Result<Reply, String> {
        self.send(args)?;
        self.flush()?;
        refused(self.reply()?)
    }

    /// Flush what is queued and read `n` replies, refusals as `Err` — one
    /// round trip for a whole pipeline.
    ///
    /// # Errors
    /// The connection failed, or the server refused any of the commands.
    pub fn replies(&mut self, n: usize) -> Result<Vec<Reply>, String> {
        self.flush()?;
        (0..n).map(|_| refused(self.reply()?)).collect()
    }

    /// Flush a queued `MULTI … EXEC` of `queued` commands and return the
    /// `EXEC` answer: one reply per queued command.
    ///
    /// # Errors
    /// The connection failed, a command was refused while queueing, or the
    /// transaction aborted.
    pub fn exec(&mut self, queued: usize) -> Result<Vec<Reply>, String> {
        // `+OK` for MULTI, `+QUEUED` per command, then EXEC's array.
        let mut all = self.replies(queued + 2)?;
        let exec = all.pop().ok_or("resp: no EXEC reply")?;
        let results = exec.array()?;
        if results.len() != queued {
            return Err(format!(
                "resp: EXEC answered {} of {queued}",
                results.len()
            ));
        }
        results.into_iter().map(refused).collect()
    }
}

/// A [`Reply::Error`] as an `Err`; anything else passes through.
///
/// # Errors
/// The reply was the server's refusal.
pub fn refused(reply: Reply) -> Result<Reply, String> {
    match reply {
        Reply::Error(e) => Err(format!("resp: {e}")),
        other => Ok(other),
    }
}

/// `*<n>` then `$<len>` + bytes per argument, each line `\r\n`-terminated.
fn encode<W: Write>(w: &mut W, args: &[&[u8]]) -> std::io::Result<()> {
    write!(w, "*{}\r\n", args.len())?;
    for arg in args {
        write!(w, "${}\r\n", arg.len())?;
        w.write_all(arg)?;
        w.write_all(b"\r\n")?;
    }
    Ok(())
}

fn read_reply<R: BufRead>(r: &mut R) -> Result<Reply, String> {
    let line = read_line(r)?;
    let (kind, rest) = line
        .split_first()
        .ok_or_else(|| "resp: empty reply line".to_string())?;
    let text = || String::from_utf8_lossy(rest).into_owned();
    let len = || -> Result<i64, String> {
        text()
            .parse()
            .map_err(|e| format!("resp: bad length {:?}: {e}", text()))
    };
    match kind {
        b'+' => Ok(Reply::Status(text())),
        b'-' => Ok(Reply::Error(text())),
        b':' => len().map(Reply::Int),
        b'$' => match usize::try_from(len()?) {
            // `$-1`: the nil bulk string.
            Err(_) => Ok(Reply::Bulk(None)),
            Ok(n) => {
                let mut body = vec![0; n + 2];
                r.read_exact(&mut body).map_err(|e| format!("resp: {e}"))?;
                body.truncate(n);
                Ok(Reply::Bulk(Some(body)))
            }
        },
        b'*' => match usize::try_from(len()?) {
            Err(_) => Ok(Reply::Array(None)),
            Ok(n) => (0..n)
                .map(|_| read_reply(r))
                .collect::<Result<_, _>>()
                .map(|items| Reply::Array(Some(items))),
        },
        other => Err(format!("resp: unknown reply type {:?}", *other as char)),
    }
}

/// One `\r\n`-terminated line, without the terminator.
fn read_line<R: BufRead>(r: &mut R) -> Result<Vec<u8>, String> {
    let mut line = Vec::new();
    r.read_until(b'\n', &mut line)
        .map_err(|e| format!("resp: {e}"))?;
    if !line.ends_with(b"\r\n") {
        return Err("resp: connection closed mid-reply".into());
    }
    line.truncate(line.len() - 2);
    Ok(line)
}

#[cfg(test)]
mod tests {
    use super::{Reply, encode, read_reply, refused};

    fn parse(bytes: &[u8]) -> Result<Reply, String> {
        read_reply(&mut &bytes[..])
    }

    #[test]
    fn a_command_is_an_array_of_bulk_strings() {
        let mut out = Vec::new();
        encode(&mut out, &[b"HGET", b"thing:7", b"name"]).expect("encode");
        assert_eq!(out, b"*3\r\n$4\r\nHGET\r\n$7\r\nthing:7\r\n$4\r\nname\r\n");
    }

    /// A body carrying `\r\n` is length-delimited, so it survives intact.
    #[test]
    fn a_bulk_string_is_read_by_length_not_by_line() {
        assert_eq!(
            parse(b"$5\r\na\r\nbc\r\n"),
            Ok(Reply::Bulk(Some(b"a\r\nbc".to_vec())))
        );
    }

    #[test]
    fn nil_bulk_and_nil_array_are_absence_not_errors() {
        assert_eq!(parse(b"$-1\r\n"), Ok(Reply::Bulk(None)));
        assert_eq!(parse(b"*-1\r\n"), Ok(Reply::Array(None)));
    }

    /// The shape of an `EXEC` answer: one reply per queued command.
    #[test]
    fn arrays_nest() {
        assert_eq!(
            parse(b"*2\r\n:5\r\n*1\r\n+OK\r\n"),
            Ok(Reply::Array(Some(vec![
                Reply::Int(5),
                Reply::Array(Some(vec![Reply::Status("OK".into())])),
            ])))
        );
    }

    #[test]
    fn a_refusal_is_a_reply_until_the_caller_says_otherwise() {
        let reply = parse(b"-ERR wrong type\r\n").expect("parses");
        assert_eq!(reply, Reply::Error("ERR wrong type".into()));
        let err = refused(reply).expect_err("a refusal");
        assert!(err.contains("wrong type"), "{err}");
    }

    #[test]
    fn a_torn_reply_is_an_error_not_a_short_value() {
        let err = parse(b"$10\r\nabc").expect_err("torn");
        assert!(err.contains("resp"), "{err}");
        let err = parse(b"+OK").expect_err("no terminator");
        assert!(err.contains("closed mid-reply"), "{err}");
    }

    #[test]
    fn an_unknown_type_byte_is_named() {
        let err = parse(b"!x\r\n").expect_err("unknown");
        assert!(err.contains('!'), "{err}");
    }

    #[test]
    fn the_typed_accessors_refuse_the_wrong_shape() {
        assert_eq!(Reply::Int(3).int(), Ok(3));
        assert!(Reply::Bulk(None).int().is_err());
        assert!(Reply::Array(None).array().is_err());
        assert!(Reply::Bulk(None).bulk().is_err());
        assert_eq!(Reply::Bulk(Some(b"x".to_vec())).bulk(), Ok(b"x".to_vec()));
    }
}
