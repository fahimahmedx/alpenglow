//! Vote data types for use by clients
use {
    serde::{Deserialize, Serialize},
    solana_hash::Hash,
    solana_program::clock::Slot,
};

/// Enum that clients can use to parse and create the vote
/// structures expected by the program
#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample, AbiEnumVisitor),
    frozen_abi(digest = "9LBQSptrSydT3MwtfHo7qU9J3u6ep9wFyqJiDtYzDqVF")
)]
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Vote {
    /// A notarization vote
    Notarize(NotarizationVote),
    /// A finalization vote
    Finalize(FinalizationVote),
    /// A skip vote
    Skip(SkipVote),
    /// A notarization fallback vote
    NotarizeFallback(NotarizationFallbackVote),
    /// A skip fallback vote
    SkipFallback(SkipFallbackVote),
}

impl Vote {
    /// Create a new notarization vote
    pub fn new_notarization_vote(slot: Slot, block_id: Hash, bank_hash: Hash) -> Self {
        Self::from(NotarizationVote::new(
            slot, block_id, 0, /*_replayed_slot not used */
            bank_hash,
        ))
    }

    /// Create a new finalization vote
    pub fn new_finalization_vote(slot: Slot) -> Self {
        Self::from(FinalizationVote::new(slot))
    }

    /// Create a new skip vote
    pub fn new_skip_vote(slot: Slot) -> Self {
        Self::from(SkipVote::new(slot))
    }

    /// Create a new notarization fallback vote
    pub fn new_notarization_fallback_vote(slot: Slot, block_id: Hash, bank_hash: Hash) -> Self {
        Self::from(NotarizationFallbackVote::new(
            slot, block_id, 0, /*_replayed_slot not used */
            bank_hash,
        ))
    }

    /// Create a new skip fallback vote
    pub fn new_skip_fallback_vote(slot: Slot) -> Self {
        Self::from(SkipFallbackVote::new(slot))
    }

    /// The slot which was voted for
    pub fn slot(&self) -> Slot {
        match self {
            Self::Notarize(vote) => vote.slot(),
            Self::Finalize(vote) => vote.slot(),
            Self::Skip(vote) => vote.slot(),
            Self::NotarizeFallback(vote) => vote.slot(),
            Self::SkipFallback(vote) => vote.slot(),
        }
    }

    /// The block id associated with the block which was voted for
    pub fn block_id(&self) -> Option<&Hash> {
        match self {
            Self::Notarize(vote) => Some(vote.block_id()),
            Self::NotarizeFallback(vote) => Some(vote.block_id()),
            Self::Finalize(_) | Self::Skip(_) | Self::SkipFallback(_) => None,
        }
    }

    /// The replayed bank hash associated with the block which was voted for
    pub fn replayed_bank_hash(&self) -> Option<&Hash> {
        match self {
            Self::Notarize(vote) => Some(vote.replayed_bank_hash()),
            Self::NotarizeFallback(vote) => Some(vote.replayed_bank_hash()),
            Self::Finalize(_) | Self::Skip(_) | Self::SkipFallback(_) => None,
        }
    }

    /// Whether the vote is a notarization vote
    pub fn is_notarization(&self) -> bool {
        matches!(self, Self::Notarize(_))
    }

    /// Whether the vote is a finalization vote
    pub fn is_finalize(&self) -> bool {
        matches!(self, Self::Finalize(_))
    }

    /// Whether the vote is a skip vote
    pub fn is_skip(&self) -> bool {
        matches!(self, Self::Skip(_))
    }

    /// Whether the vote is a notarization fallback vote
    pub fn is_notarize_fallback(&self) -> bool {
        matches!(self, Self::NotarizeFallback(_))
    }

    /// Whether the vote is a skip fallback vote
    pub fn is_skip_fallback(&self) -> bool {
        matches!(self, Self::SkipFallback(_))
    }

    /// Whether the vote is a notarization or finalization
    pub fn is_notarization_or_finalization(&self) -> bool {
        matches!(self, Self::Notarize(_) | Self::Finalize(_))
    }
}

impl From<NotarizationVote> for Vote {
    fn from(vote: NotarizationVote) -> Self {
        Self::Notarize(vote)
    }
}

impl From<FinalizationVote> for Vote {
    fn from(vote: FinalizationVote) -> Self {
        Self::Finalize(vote)
    }
}

impl From<SkipVote> for Vote {
    fn from(vote: SkipVote) -> Self {
        Self::Skip(vote)
    }
}

impl From<NotarizationFallbackVote> for Vote {
    fn from(vote: NotarizationFallbackVote) -> Self {
        Self::NotarizeFallback(vote)
    }
}

impl From<SkipFallbackVote> for Vote {
    fn from(vote: SkipFallbackVote) -> Self {
        Self::SkipFallback(vote)
    }
}

/// A notarization vote
#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample),
    frozen_abi(digest = "AfTX2mg2e3L433SgswtskptGYXLpWGXYDcR4QcgSzRC5")
)]
#[derive(Clone, Copy, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct NotarizationVote {
    slot: Slot,
    block_id: Hash,
    _replayed_slot: Slot, // NOTE: replayed_slot will be unused until we support APE
    replayed_bank_hash: Hash,
}

impl NotarizationVote {
    /// Construct a notarization vote for `slot`
    pub fn new(slot: Slot, block_id: Hash, replayed_slot: Slot, replayed_bank_hash: Hash) -> Self {
        Self {
            slot,
            block_id,
            _replayed_slot: replayed_slot,
            replayed_bank_hash,
        }
    }

    /// The slot to notarize
    pub fn slot(&self) -> Slot {
        self.slot
    }

    /// The block_id of the notarization slot
    pub fn block_id(&self) -> &Hash {
        &self.block_id
    }

    /// The bank hash of the latest replayed slot
    pub fn replayed_bank_hash(&self) -> &Hash {
        &self.replayed_bank_hash
    }
}

/// A finalization vote
#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample),
    frozen_abi(digest = "2XQ5N6YLJjF28w7cMFFUQ9SDgKuf9JpJNtAiXSPA8vR2")
)]
#[derive(Clone, Copy, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct FinalizationVote {
    slot: Slot,
}

impl FinalizationVote {
    /// Construct a finalization vote for `slot`
    pub fn new(slot: Slot) -> Self {
        Self { slot }
    }

    /// The slot to finalize
    pub fn slot(&self) -> Slot {
        self.slot
    }
}

/// A skip vote
/// Represents a range of slots to skip
/// inclusive on both ends
#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample),
    frozen_abi(digest = "G8Nrx3sMYdnLpHsCNark3BGA58BmW2sqNnqjkYhQHtN")
)]
#[derive(Clone, Copy, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct SkipVote {
    pub(crate) slot: Slot,
}

impl SkipVote {
    /// Construct a skip vote for `slot`
    pub fn new(slot: Slot) -> Self {
        Self { slot }
    }

    /// The slot to skip
    pub fn slot(&self) -> Slot {
        self.slot
    }
}

/// A notarization fallback vote
#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample),
    frozen_abi(digest = "2eD1FTtZb6e86j3WEYCkzG9Yer36jA98B4RiuvFgwZ7d")
)]
#[derive(Clone, Copy, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct NotarizationFallbackVote {
    slot: Slot,
    block_id: Hash,
    _replayed_slot: Slot, // NOTE: replayed_slot will be unused until we support APE
    replayed_bank_hash: Hash,
}

impl NotarizationFallbackVote {
    /// Construct a notarization vote for `slot`
    pub fn new(slot: Slot, block_id: Hash, replayed_slot: Slot, replayed_bank_hash: Hash) -> Self {
        Self {
            slot,
            block_id,
            _replayed_slot: replayed_slot,
            replayed_bank_hash,
        }
    }

    /// The slot to notarize
    pub fn slot(&self) -> Slot {
        self.slot
    }

    /// The block_id of the notarization slot
    pub fn block_id(&self) -> &Hash {
        &self.block_id
    }

    /// The bank hash of the latest replayed slot
    pub fn replayed_bank_hash(&self) -> &Hash {
        &self.replayed_bank_hash
    }
}

/// A skip fallback vote
#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample),
    frozen_abi(digest = "WsUNum8V62gjRU1yAnPuBMAQui4YvMwD1RwrzHeYkeF")
)]
#[derive(Clone, Copy, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct SkipFallbackVote {
    pub(crate) slot: Slot,
}

impl SkipFallbackVote {
    /// Construct a skip fallback vote for `slot`
    pub fn new(slot: Slot) -> Self {
        Self { slot }
    }

    /// The slot to skip
    pub fn slot(&self) -> Slot {
        self.slot
    }
}
