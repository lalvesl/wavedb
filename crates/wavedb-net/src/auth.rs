//! Stateless access tokens — the M8 identity primitive.
//!
//! A token is `[to_wire(AccessClaims)][HMAC-SHA256 (32 bytes)]`, signed with
//! the node's secret. The node verifies per request — no token store, no
//! session lookup on the hot path; expiry rides in the claims. The token
//! travels **inside the request envelope** ([`Auth::Token`]), never in an
//! HTTP header — the transport stays a dumb tunnel.
//!
//! [`Auth::Token`]: crate::frame::Auth::Token
//!
//! Two purposes share the shape: a short-TTL [`Access`](TokenPurpose::Access)
//! token authorises commands; a long-TTL [`Refresh`](TokenPurpose::Refresh)
//! token is only good for minting the next pair and is bound to a session
//! record (`session` = the record's raw `Id`) so rotation and revocation are
//! one record write (`wavedb::auth`).

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use wavedb_core::U48;
use wavedb_wire::WaveWire;

/// HMAC-SHA256 output length — the token's trailing MAC.
const MAC_LEN: usize = 32;

/// The node's signing secret — process-global, set once at node open
/// (mirrors the engine's one-`PageStore`-per-process stance). The node
/// builder writes it; the token-minting helpers (`wavedb::auth`) and the
/// verify gate read it.
static NODE_SECRET: std::sync::OnceLock<[u8; 32]> = std::sync::OnceLock::new();

/// Install the process's signing secret. Idempotent for the same value;
/// a second *different* secret is refused (`false`) — one node per process.
pub fn set_node_secret(secret: [u8; 32]) -> bool {
    *NODE_SECRET.get_or_init(|| secret) == secret
}

/// The installed signing secret, or `None` before the node opened.
#[must_use]
pub fn node_secret() -> Option<&'static [u8; 32]> {
    NODE_SECRET.get()
}

/// Unix seconds now — the clock token TTLs run on (platform-routed, so
/// expiry math works in the browser too).
#[must_use]
pub fn unix_now() -> u64 {
    wavedb_platform::time::unix_secs()
}

/// What a token is good for. Encoded in the claims (and so under the MAC):
/// a refresh token can never pass as an access token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, WaveWire)]
pub enum TokenPurpose {
    /// Authorises commands for its TTL.
    Access,
    /// Only mints the next token pair; bound to a session record.
    Refresh,
}

/// The signed identity a token carries.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct AccessClaims {
    /// The authenticated user (real authorship for `Metadata.user`).
    pub user: U48,
    /// The tenant the session is bound to — the node executes under this,
    /// ignoring any claimed tenant.
    pub tenant: U48,
    /// Unix seconds; a token at or past this instant is dead.
    pub expires_at: u64,
    /// Access or refresh — see [`TokenPurpose`].
    pub purpose: TokenPurpose,
    /// The raw `Id` of the session record backing this pair (`0` when the
    /// issuer tracks no session).
    pub session: u128,
    /// Uniquifier: two pairs minted in the same second must still differ
    /// byte-for-byte (refresh rotation compares token hashes).
    pub nonce: u64,
}

/// Why a token failed verification. Deliberately coarse — the node reports
/// all of these as one uniform `Unauthorized`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenError {
    /// Too short, or the claims did not decode.
    Malformed,
    /// The MAC did not match (wrong secret or tampered claims).
    BadSignature,
    /// `expires_at` is in the past.
    Expired,
    /// Valid, but the wrong [`TokenPurpose`] for this use.
    WrongPurpose,
}

fn mac(secret: &[u8; 32], claims_wire: &[u8]) -> [u8; MAC_LEN] {
    // HMAC accepts any key length; a 32-byte secret never errors.
    let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(secret)
        .unwrap_or_else(|_| unreachable!("32-byte HMAC key is always valid"));
    m.update(claims_wire);
    m.finalize().into_bytes().into()
}

/// Sign `claims` into token bytes: `[claims wire][mac]`.
#[must_use]
pub fn sign(secret: &[u8; 32], claims: &AccessClaims) -> Vec<u8> {
    let mut bytes = wavedb_wire::to_wire(claims);
    bytes.extend_from_slice(&mac(secret, &bytes));
    bytes
}

/// Verify `token` against `secret` at time `now` (unix seconds), requiring
/// `purpose`.
///
/// Returns the claims only when the MAC, expiry, and purpose all hold — the
/// MAC is checked first (constant-time), so nothing about the claims leaks
/// from an unsigned token.
///
/// # Errors
/// A [`TokenError`] naming the first check that failed.
pub fn verify(
    secret: &[u8; 32],
    token: &[u8],
    now: u64,
    purpose: TokenPurpose,
) -> Result<AccessClaims, TokenError> {
    let split = token
        .len()
        .checked_sub(MAC_LEN)
        .ok_or(TokenError::Malformed)?;
    let (claims_wire, tag) = token.split_at(split);
    let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(secret)
        .unwrap_or_else(|_| unreachable!("32-byte HMAC key is always valid"));
    m.update(claims_wire);
    m.verify_slice(tag).map_err(|_| TokenError::BadSignature)?;
    let claims: AccessClaims = wavedb_wire::from_wire(claims_wire)
        .map_err(|_| TokenError::Malformed)?;
    if now >= claims.expires_at {
        return Err(TokenError::Expired);
    }
    if claims.purpose != purpose {
        return Err(TokenError::WrongPurpose);
    }
    Ok(claims)
}

#[cfg(test)]
mod tests {
    use super::{AccessClaims, TokenError, TokenPurpose, sign, verify};
    use wavedb_core::U48;

    const SECRET: [u8; 32] = [7; 32];

    fn claims(purpose: TokenPurpose) -> AccessClaims {
        AccessClaims {
            user: U48::from(3u32),
            tenant: U48::from(9u32),
            expires_at: 1_000,
            purpose,
            session: 0xAB,
            nonce: 1,
        }
    }

    #[test]
    fn signed_token_verifies_and_roundtrips_claims() {
        let c = claims(TokenPurpose::Access);
        let token = sign(&SECRET, &c);
        let got = verify(&SECRET, &token, 999, TokenPurpose::Access).unwrap();
        assert_eq!(got, c);
    }

    #[test]
    fn tampered_claims_are_bad_signature() {
        let mut token = sign(&SECRET, &claims(TokenPurpose::Access));
        token[0] ^= 1;
        assert_eq!(
            verify(&SECRET, &token, 0, TokenPurpose::Access),
            Err(TokenError::BadSignature)
        );
    }

    #[test]
    fn wrong_secret_is_bad_signature() {
        let token = sign(&SECRET, &claims(TokenPurpose::Access));
        assert_eq!(
            verify(&[8; 32], &token, 0, TokenPurpose::Access),
            Err(TokenError::BadSignature)
        );
    }

    #[test]
    fn expiry_instant_is_already_dead() {
        let token = sign(&SECRET, &claims(TokenPurpose::Access));
        assert_eq!(
            verify(&SECRET, &token, 1_000, TokenPurpose::Access),
            Err(TokenError::Expired)
        );
    }

    #[test]
    fn refresh_token_never_passes_as_access() {
        let token = sign(&SECRET, &claims(TokenPurpose::Refresh));
        assert_eq!(
            verify(&SECRET, &token, 0, TokenPurpose::Access),
            Err(TokenError::WrongPurpose)
        );
    }

    #[test]
    fn truncated_token_is_malformed() {
        assert_eq!(
            verify(&SECRET, &[1, 2, 3], 0, TokenPurpose::Access),
            Err(TokenError::Malformed)
        );
    }
}
