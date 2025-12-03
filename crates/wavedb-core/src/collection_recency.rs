//! [`Collection`]'s recency half — the instant keying behind the record
//! chain and the removal log, and the floor every mint clears.
//!
//! Both chains are keyed `[instant BE (8 B)][anchor LocalId]` (a [`SecKey`]):
//!
//! - the **record chain** holds exactly one entry per **living** record,
//!   keyed by the instant its live version was authored. An insert adds it,
//!   a save relocates it, a remove takes it out — so a tail scan from a
//!   cursor is precisely "every record changed since", each one once. The
//!   entry carries **no payload** (RFC 0054): its key already names the
//!   anchor, and the record lives there.
//! - the **removal log** holds one entry per removed record, keyed by the
//!   instant of the **removal** — "when was this killed", which is what that
//!   same tail scan pairs with, and the half of the floor that survives a
//!   record leaving the living set.
//!
//! Both are maintained inside the ops' single atomic batch. Their tails
//! form the collection's **instant floor**: every instant minted for the
//! collection lands strictly above it (see [`crate::mint`]), so a rewound
//! clock can never author below a cursor a client already advanced past.
//!
//! Until RFC 0050 phase 5c this described two *B+trees* of the same shape,
//! written alongside the chains. The chains absorbed them.

use crate::collection::Collection;
use crate::error::Result;
use crate::index::{Chain, SecKey};
use crate::local_id::LocalId;
use crate::metadata::{Metadata, Succession};
use crate::overlay::Overlay;
use crate::store::{Store, Write};
use crate::traits::NonUniqueStruct;

impl<T: NonUniqueStruct> Collection<T> {
    /// The `[instant BE][anchor]` key both chains use: instant-major (they
    /// are time-ordered), the anchor breaking ties and naming the record.
    pub(crate) fn instant_key(instant: u64, rec: LocalId) -> SecKey {
        SecKey {
            field: instant.to_be_bytes().to_vec(),
            rec,
        }
    }

    /// The instant a chain key encodes.
    fn key_instant(key: &SecKey) -> u64 {
        // These chains are engine-written only; a non-8-byte field would
        // mean a corrupt node, which decode surfaces long before this.
        key.field
            .as_slice()
            .try_into()
            .map_or(0, u64::from_be_bytes)
    }

    /// The collection's instant floor: the greatest instant its record
    /// chain and removal log record (`0` when both are empty). Every
    /// instant minted for this collection must land strictly above it.
    ///
    /// Both are instant-ordered, so this is **two reads of one entry** —
    /// each chain's greatest key, read from its end. The floor is taken on
    /// every mint.
    pub(crate) async fn instant_floor<S: Store>(
        &self,
        store: &S,
    ) -> Result<u64> {
        let live = self.recency_chain().last_key(store).await?;
        // The removal log is the other half and not an optimisation: a
        // removed record leaves the chain, so its instant survives only
        // here — without it a rewound clock could re-mint under a cursor
        // a client already advanced past.
        let gone = self.dead_log().last_key(store).await?;
        Ok(live
            .iter()
            .chain(&gone)
            .map(Self::key_instant)
            .max()
            .unwrap_or(0))
    }

    /// Relocate the record's **chain** copy from `old_key` to its new
    /// modification instant.
    ///
    /// That movement *is* the modification log (RFC 0050): the chain is ordered
    /// by the live version's authoring instant, so a save necessarily takes the
    /// record out of wherever it sat and puts it back at the growth end.
    ///
    /// Returns the freshly encoded envelope the chain now holds, or `None` when
    /// the record was not in the chain to begin with. Both halves of that are
    /// used: the caller's declared lists hold the same bytes, so returning them
    /// saves a second encode, and `None` is the **liveness gate** every derived
    /// chain rides on — a record the built-in chain does not hold must not enter
    /// any other one.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure, or
    /// [`Error::ChainCorrupt`](crate::Error::ChainCorrupt) if the freshly
    /// authored metadata does not name a live version.
    pub(crate) async fn plan_chain_move<S: Store>(
        &self,
        view: &mut Overlay<'_, S>,
        batch: &mut Vec<Write>,
        records: &Chain<()>,
        old_key: SecKey,
        live_meta: &Metadata,
        value: &T,
    ) -> Result<Option<Vec<u8>>> {
        // The anchor rides in the key's trailing pointer, so the record's `Id`
        // needs no parameter of its own.
        let id = old_key.rec.to_id(self.tenant());
        let Succession::CreatedAt(instant) = live_meta.succession else {
            return Err(crate::Error::ChainCorrupt(id));
        };
        // Same guard `plan_recency_rekey` applies, and for the same reason: a
        // record the chain does not hold — a dead one saved by address, a
        // chainless first version — must not *enter* the living set through a
        // save. Without this the record would reappear in `all()` while its own
        // anchor still reads removed.
        let Some(write) = records.plan_remove(&*view, &old_key).await? else {
            return Ok(None);
        };
        let insert = records
            .insert(&Self::instant_key(instant, LocalId::from_id(id)), &());
        let writes = [write, insert];
        view.stage(&writes);
        batch.extend(writes);
        // The chain entry carries nothing: its key already names the anchor,
        // and the record is at that anchor. The envelope is still encoded here
        // because the caller's declared lists DO hold it, and encoding once is
        // the point of handing it back.
        Ok(Some(crate::record::encode_record(
            T::STRUCT_HASH,
            live_meta,
            value,
        )))
    }
}
