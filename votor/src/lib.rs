#![cfg_attr(feature = "frozen-abi", feature(min_specialization))]
use {
    solana_votor_messages::{bls_message::Certificate, vote::Vote},
    std::time::Duration,
};

pub mod certificate_pool;
mod certificate_pool_service;
pub mod commitment;
pub mod event;
mod event_handler;
pub mod root_utils;
mod skip_timer;
pub mod vote_history;
pub mod vote_history_storage;
pub mod voting_utils;
pub mod votor;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

#[cfg_attr(feature = "frozen-abi", macro_use)]
#[cfg(feature = "frozen-abi")]
extern crate solana_frozen_abi_macro;

// Core consensus types and constants
pub type Stake = u64;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VoteType {
    Finalize,
    Notarize,
    NotarizeFallback,
    Skip,
    SkipFallback,
}

impl VoteType {
    #[allow(dead_code)]
    pub fn is_notarize_type(&self) -> bool {
        matches!(self, Self::Notarize | Self::NotarizeFallback)
    }
}

pub const fn conflicting_types(vote_type: VoteType) -> &'static [VoteType] {
    match vote_type {
        VoteType::Finalize => &[VoteType::NotarizeFallback, VoteType::Skip],
        VoteType::Notarize => &[VoteType::Skip, VoteType::NotarizeFallback],
        VoteType::NotarizeFallback => &[VoteType::Finalize, VoteType::Notarize],
        VoteType::Skip => &[
            VoteType::Finalize,
            VoteType::Notarize,
            VoteType::SkipFallback,
        ],
        VoteType::SkipFallback => &[VoteType::Skip],
    }
}

/// Lookup from `CertificateId` to the `VoteType`s that contribute,
/// as well as the stake fraction required for certificate completion.
///
/// Must be in sync with `vote_to_certificate_ids`
pub const fn certificate_limits_and_vote_types(
    cert_type: Certificate,
) -> (f64, &'static [VoteType]) {
    match cert_type {
        Certificate::Notarize(_, _) => (0.6, &[VoteType::Notarize]),
        Certificate::NotarizeFallback(_, _) => {
            (0.6, &[VoteType::Notarize, VoteType::NotarizeFallback])
        }
        Certificate::FinalizeFast(_, _) => (0.8, &[VoteType::Notarize]),
        Certificate::Finalize(_) => (0.6, &[VoteType::Finalize]),
        Certificate::Skip(_) => (0.6, &[VoteType::Skip, VoteType::SkipFallback]),
    }
}

/// Lookup from `Vote` to the `CertificateId`s the vote accounts for
///
/// Must be in sync with `certificate_limits_and_vote_types` and `VoteType::get_type`
pub fn vote_to_certificate_ids(vote: &Vote) -> Vec<Certificate> {
    match vote {
        Vote::Notarize(vote) => vec![
            Certificate::Notarize(vote.slot(), *vote.block_id()),
            Certificate::NotarizeFallback(vote.slot(), *vote.block_id()),
            Certificate::FinalizeFast(vote.slot(), *vote.block_id()),
        ],
        Vote::NotarizeFallback(vote) => {
            vec![Certificate::NotarizeFallback(vote.slot(), *vote.block_id())]
        }
        Vote::Finalize(vote) => vec![Certificate::Finalize(vote.slot())],
        Vote::Skip(vote) => vec![Certificate::Skip(vote.slot())],
        Vote::SkipFallback(vote) => vec![Certificate::Skip(vote.slot())],
    }
}

pub const MAX_ENTRIES_PER_PUBKEY_FOR_OTHER_TYPES: usize = 1;
pub const MAX_ENTRIES_PER_PUBKEY_FOR_NOTARIZE_LITE: usize = 3;

pub const SAFE_TO_NOTAR_MIN_NOTARIZE_ONLY: f64 = 0.4;
pub const SAFE_TO_NOTAR_MIN_NOTARIZE_FOR_NOTARIZE_OR_SKIP: f64 = 0.2;
pub const SAFE_TO_NOTAR_MIN_NOTARIZE_AND_SKIP: f64 = 0.6;

pub const SAFE_TO_SKIP_THRESHOLD: f64 = 0.4;

pub const STANDSTILL_TIMEOUT: Duration = Duration::from_secs(10);

/// Alpenglow block constants
/// The amount of time a leader has to build their block
pub const BLOCKTIME: Duration = Duration::from_millis(400);

/// The maximum message delay
pub const DELTA: Duration = Duration::from_millis(200);

/// The maximum delay a node can observe between entering the loop iteration
/// for a window and receiving any shred of the first block of the leader.
/// As a conservative global constant we set this to 3 * DELTA
pub const DELTA_TIMEOUT: Duration = DELTA.saturating_mul(3);

/// The timeout in ms for the leader block index within the leader window
#[inline]
pub fn skip_timeout(leader_block_index: usize) -> Duration {
    DELTA_TIMEOUT
        .saturating_add(
            BLOCKTIME
                .saturating_mul(leader_block_index as u32)
                .saturating_add(BLOCKTIME),
        )
        .saturating_add(DELTA)
}

/// Block timeout, when we should publish the final shred for the leader block index
/// within the leader window
#[inline]
pub fn block_timeout(leader_block_index: usize) -> Duration {
    // TODO: based on testing, perhaps adjust this
    BLOCKTIME.saturating_mul((leader_block_index as u32).saturating_add(1))
}
