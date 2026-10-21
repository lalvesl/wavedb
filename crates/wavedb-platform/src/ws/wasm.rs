//! Browser `connect`: the platform `WebSocket` object (it frames RFC 6455
//! itself — no codec here), events bridged to an awaitable `recv` through
//! a local channel.

use std::cell::RefCell;
use std::rc::Rc;

use futures::StreamExt;
use futures::channel::{mpsc, oneshot};
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use web_sys::{CloseEvent, MessageEvent, WebSocket};

use crate::error::{Error, Result, js};

/// An open WebSocket connection — binary messages in both directions.
/// The browser answers pings itself; the identity handshake above this
/// (`Hello`) belongs to `wavedb-net`.
#[derive(Debug)]
pub struct Conn {
    ws: WebSocket,
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
    guard: CallbackGuard,
}

/// Keeps the event closures alive for the socket's lifetime (dropping them
/// while the socket lives would leave the browser calling freed callbacks)
/// and tears the socket down when the receiving side goes away. Travels
/// with the receiving half on [`Conn::split`].
#[derive(Debug)]
struct CallbackGuard {
    ws: WebSocket,
    _on_message: Closure<dyn FnMut(MessageEvent)>,
    _on_close: Closure<dyn FnMut(CloseEvent)>,
}

impl Drop for CallbackGuard {
    fn drop(&mut self) {
        // Detach the callbacks before their closures free, then let the
        // browser tear the socket down.
        self.ws.set_onmessage(None);
        self.ws.set_onclose(None);
        let _ = self.ws.close();
    }
}

/// Open a WebSocket to `addr`: a bare `host:port` gets `ws://` prepended,
/// a full URL passes through. Resolves once the socket is open.
///
/// # Errors
/// [`Error::Js`] when the browser refuses the socket or the connection
/// attempt fails.
pub async fn connect(addr: &str) -> Result<Conn> {
    let url = if addr.contains("://") {
        addr.to_owned()
    } else {
        format!("ws://{addr}/")
    };
    let ws = WebSocket::new(&url).map_err(|e| js("WebSocket::new", &e))?;
    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

    let (tx, rx) = mpsc::unbounded::<Vec<u8>>();
    let on_message = {
        let tx = tx.clone();
        Closure::<dyn FnMut(MessageEvent)>::new(move |ev: MessageEvent| {
            if let Ok(buf) = ev.data().dyn_into::<js_sys::ArrayBuffer>() {
                let bytes = js_sys::Uint8Array::new(&buf).to_vec();
                let _ = tx.unbounded_send(bytes);
            }
        })
    };
    ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    let on_close =
        Closure::<dyn FnMut(CloseEvent)>::new(move |_: CloseEvent| {
            tx.close_channel();
        });
    ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));

    // Await open-vs-error — whichever fires first consumes the sender.
    let (opened_tx, opened_rx) = oneshot::channel::<bool>();
    let opened_tx = Rc::new(RefCell::new(Some(opened_tx)));
    let on_open = {
        let opened_tx = Rc::clone(&opened_tx);
        Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            if let Some(tx) = opened_tx.borrow_mut().take() {
                let _ = tx.send(true);
            }
        })
    };
    let on_error = Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
        if let Some(tx) = opened_tx.borrow_mut().take() {
            let _ = tx.send(false);
        }
    });
    ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));
    ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));
    let opened = opened_rx.await.unwrap_or(false);
    ws.set_onopen(None);
    ws.set_onerror(None);
    if !opened {
        return Err(Error::Js(String::from("WebSocket failed to open")));
    }
    let guard = CallbackGuard {
        ws: ws.clone(),
        _on_message: on_message,
        _on_close: on_close,
    };
    Ok(Conn { ws, rx, guard })
}

impl Conn {
    /// Send one binary message.
    ///
    /// # Errors
    /// [`Error::Js`] when the browser refuses the send (closing socket).
    // `async` for parity with the native half (which awaits its write); the
    // browser `WebSocket.send` is fire-and-forget.
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn send(&mut self, bytes: &[u8]) -> Result<()> {
        self.ws
            .send_with_u8_array(bytes)
            .map_err(|e| js("WebSocket::send", &e))
    }

    /// The next binary message; `None` = the socket closed.
    ///
    /// # Errors
    /// None in the browser — a broken socket surfaces as the close event,
    /// i.e. `Ok(None)`. Fallible for parity with the native half.
    pub async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        Ok(self.rx.next().await)
    }

    /// Announce the close; the browser runs the close handshake.
    ///
    /// # Errors
    /// [`Error::Js`] when the browser refuses (already closing is fine).
    // `async` for parity with the native half; the browser call is sync.
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn close(&mut self) -> Result<()> {
        self.ws.close().map_err(|e| js("WebSocket::close", &e))
    }

    /// Split into independently owned halves — same surface as the native
    /// half. The browser answers pings itself, so the receiving half never
    /// yields [`Received::Ping`](super::Received::Ping).
    #[must_use]
    pub fn split(self) -> (RecvHalf, SendHalf) {
        let Self { ws, rx, guard } = self;
        (RecvHalf { rx, _guard: guard }, SendHalf { ws })
    }
}

/// The receiving half of a split [`Conn`] — carries the callback guard, so
/// dropping it tears the socket down.
#[derive(Debug)]
pub struct RecvHalf {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
    _guard: CallbackGuard,
}

/// The sending half of a split [`Conn`].
#[derive(Debug)]
pub struct SendHalf {
    ws: WebSocket,
}

impl RecvHalf {
    /// The next message; `None` = the socket closed.
    ///
    /// # Errors
    /// None in the browser — fallible for parity with the native half.
    pub async fn next(&mut self) -> Result<Option<super::Received>> {
        Ok(self.rx.next().await.map(super::Received::Binary))
    }
}

impl SendHalf {
    /// Send one binary message.
    ///
    /// # Errors
    /// [`Error::Js`] when the browser refuses the send (closing socket).
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn send(&mut self, bytes: &[u8]) -> Result<()> {
        self.ws
            .send_with_u8_array(bytes)
            .map_err(|e| js("WebSocket::send", &e))
    }

    /// Answer a ping — never needed in the browser (it pongs itself); the
    /// method exists so actor code reads the same on both targets.
    ///
    /// # Errors
    /// None in the browser.
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn pong(&mut self, _payload: &[u8]) -> Result<()> {
        Ok(())
    }

    /// Announce the close; the browser runs the close handshake.
    ///
    /// # Errors
    /// [`Error::Js`] when the browser refuses (already closing is fine).
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn close(&mut self) -> Result<()> {
        self.ws.close().map_err(|e| js("WebSocket::close", &e))
    }
}
