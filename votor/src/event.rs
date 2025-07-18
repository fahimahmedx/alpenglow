use {
    crate::Block,
    crossbeam_channel::{Receiver, Sender},
    solana_runtime::bank::Bank,
    solana_sdk::clock::Slot,
    std::{sync::Arc, time::Instant},
};

pub struct CompletedBlock {
    pub slot: Slot,
    // TODO: once we have the async execution changes this can be (block_id, parent_block_id) instead
    pub bank: Arc<Bank>,
}
pub type CompletedBlockSender = Sender<CompletedBlock>;
pub type CompletedBlockReceiver = Receiver<CompletedBlock>;

/// Context for the block creation loop to start a leader window
#[derive(Copy, Clone, Debug)]
pub struct LeaderWindowInfo {
    pub start_slot: Slot,
    pub end_slot: Slot,
    pub parent_block: Block,
    pub skip_timer: Instant,
}

/// Events that trigger actions in Votor
/// TODO: remove bank hash once we update votes
pub enum VotorEvent {
    /// A block has completed replay and is ready for voting
    Block(CompletedBlock),

    /// The block has received a notarization certificate
    BlockNotarized(Block),

    /// The pool has marked the given block as a ready parent for `slot`
    ParentReady { slot: Slot, parent_block: Block },

    /// The skip timer has fired for the given slot
    Timeout(Slot),

    /// The given block has reached the safe to notar status
    SafeToNotar(Block),

    /// The given slot has reached the safe to skip status
    SafeToSkip(Slot),

    /// We are the leader for this window and have reached the parent ready status
    /// Produce the window
    ProduceWindow(LeaderWindowInfo),

    /// The block has received a slow or fast finalization certificate and is eligble for rooting
    Finalized(Block),

    /// We have not observed a finalization and reached the standstill timeout
    /// The slot is the highest finalized slot
    Standstill(Slot),

    /// The identity keypair has changed due to an operator calling set-identity
    SetIdentity,
}
