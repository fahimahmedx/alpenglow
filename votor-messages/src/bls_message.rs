//! Put BLS message here so all clients can agree on the format
use {
    crate::vote::Vote,
    bitvec::prelude::*,
    serde::{Deserialize, Serialize},
    solana_bls_signatures::Signature as BLSSignature,
    solana_sdk::{clock::Slot, hash::Hash},
};

/// The seed used to derive the BLS keypair
pub const BLS_KEYPAIR_DERIVE_SEED: &[u8; 9] = b"alpenglow";

/// Block, a (slot, hash) tuple
pub type Block = (Slot, Hash);

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
/// BLS vote message, we need rank to look up pubkey
pub struct VoteMessage {
    /// The vote
    pub vote: Vote,
    /// The signature
    pub signature: BLSSignature,
    /// The rank of the validator
    pub rank: u16,
}

/// Certificate details
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum Certificate {
    /// Finalize certificate
    Finalize(Slot),
    /// Fast finalize certificate
    FinalizeFast(Slot, Hash),
    /// Notarize certificate
    Notarize(Slot, Hash),
    /// Notarize fallback certificate
    NotarizeFallback(Slot, Hash),
    /// Skip certificate
    Skip(Slot),
}

/// Certificate type
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum CertificateType {
    /// Finalize certificate
    Finalize,
    /// Fast finalize certificate
    FinalizeFast,
    /// Notarize certificate
    Notarize,
    /// Notarize fallback certificate
    NotarizeFallback,
    /// Skip certificate
    Skip,
}

impl Certificate {
    /// Create a new certificate ID from a CertificateType, Option<Slot>, and Option<Hash>
    pub fn new(certificate_type: CertificateType, slot: Slot, hash: Option<Hash>) -> Self {
        match (certificate_type, hash) {
            (CertificateType::Finalize, None) => Certificate::Finalize(slot),
            (CertificateType::FinalizeFast, Some(hash)) => Certificate::FinalizeFast(slot, hash),
            (CertificateType::Notarize, Some(hash)) => Certificate::Notarize(slot, hash),
            (CertificateType::NotarizeFallback, Some(hash)) => {
                Certificate::NotarizeFallback(slot, hash)
            }
            (CertificateType::Skip, None) => Certificate::Skip(slot),
            _ => panic!("Invalid certificate type and hash combination"),
        }
    }

    /// Get the certificate type
    pub fn certificate_type(&self) -> CertificateType {
        match self {
            Certificate::Finalize(_) => CertificateType::Finalize,
            Certificate::FinalizeFast(_, _) => CertificateType::FinalizeFast,
            Certificate::Notarize(_, _) => CertificateType::Notarize,
            Certificate::NotarizeFallback(_, _) => CertificateType::NotarizeFallback,
            Certificate::Skip(_) => CertificateType::Skip,
        }
    }

    /// Get the slot of the certificate
    pub fn slot(&self) -> Slot {
        match self {
            Certificate::Finalize(slot)
            | Certificate::FinalizeFast(slot, _)
            | Certificate::Notarize(slot, _)
            | Certificate::NotarizeFallback(slot, _)
            | Certificate::Skip(slot) => *slot,
        }
    }

    /// Is this a fast finalize certificate?
    pub fn is_fast_finalization(&self) -> bool {
        matches!(self, Self::FinalizeFast(_, _))
    }

    /// Is this a finalize / fast finalize certificate?
    pub fn is_finalization(&self) -> bool {
        matches!(self, Self::Finalize(_) | Self::FinalizeFast(_, _))
    }

    /// Is this a notarize fallback certificate?
    pub fn is_notarize_fallback(&self) -> bool {
        matches!(self, Self::NotarizeFallback(_, _))
    }

    /// Is this a skip certificate?
    pub fn is_skip(&self) -> bool {
        matches!(self, Self::Skip(_))
    }

    /// Gets the block associated with this certificate, if present
    pub fn to_block(self) -> Option<Block> {
        match self {
            Certificate::Finalize(_) | Certificate::Skip(_) => None,
            Certificate::Notarize(slot, block_id)
            | Certificate::NotarizeFallback(slot, block_id)
            | Certificate::FinalizeFast(slot, block_id) => Some((slot, block_id)),
        }
    }

    /// "Critical" certs are the certificates necessary to make progress
    /// We do not consider the next slot for voting until we've seen either
    /// a Skip certificate or a NotarizeFallback certificate for ParentReady
    ///
    /// Note: Notarization certificates necessarily generate a
    /// NotarizeFallback certificate as well
    pub fn is_critical(&self) -> bool {
        matches!(self, Self::NotarizeFallback(_, _) | Self::Skip(_))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// BLS vote message, we need rank to look up pubkey
pub struct CertificateMessage {
    /// The certificate
    pub certificate: Certificate,
    /// The signature
    pub signature: BLSSignature,
    /// The bitmap for validators, little endian byte order
    pub bitmap: BitVec<u8, Lsb0>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
/// BLS message data in Alpenglow
pub enum BLSMessage {
    /// Vote message, with the vote and the rank of the validator.
    Vote(VoteMessage),
    /// Certificate message
    Certificate(CertificateMessage),
}

impl BLSMessage {
    /// Create a new vote message
    pub fn new_vote(vote: Vote, signature: BLSSignature, rank: u16) -> Self {
        Self::Vote(VoteMessage {
            vote,
            signature,
            rank,
        })
    }

    /// Create a new certificate message
    pub fn new_certificate(
        certificate: Certificate,
        bitmap: BitVec<u8, Lsb0>,
        signature: BLSSignature,
    ) -> Self {
        Self::Certificate(CertificateMessage {
            certificate,
            signature,
            bitmap,
        })
    }
}
