//! The Alpenglow voting loop, handles all three types of votes as well as
//! rooting, leader logic, and dumping and repairing the notarized versions.
use {
    crate::{
        certificate_pool::{
            self, parent_ready_tracker::BlockProductionParent, AddVoteError, CertificatePool,
        },
        commitment::{
            alpenglow_update_commitment_cache, AlpenglowCommitmentAggregationData,
            AlpenglowCommitmentType,
        },
        event::{CompletedBlock, CompletedBlockReceiver, LeaderWindowInfo},
        root_utils::maybe_set_root,
        skip_timeout,
        vote_history::VoteHistory,
        vote_history_storage::VoteHistoryStorage,
        voting_utils::{add_message_and_maybe_update_commitment, send_vote, BLSOp, VotingContext},
        Block, CertificateId,
    },
    alpenglow_vote::{
        bls_message::{BLSMessage, CertificateMessage},
        vote::Vote,
    },
    crossbeam_channel::{RecvTimeoutError, Sender},
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        blockstore::Blockstore,
        leader_schedule_cache::LeaderScheduleCache,
        leader_schedule_utils::{
            first_of_consecutive_leader_slots, last_of_consecutive_leader_slots, leader_slot_index,
        },
    },
    solana_rpc::{
        optimistically_confirmed_bank_tracker::BankNotificationSenderConfig,
        rpc_subscriptions::RpcSubscriptions,
    },
    solana_runtime::{
        accounts_background_service::AbsRequestSender, bank::Bank, bank_forks::BankForks,
        installed_scheduler_pool::BankWithScheduler, root_bank_cache::RootBankCache,
        vote_sender_types::BLSVerifiedMessageReceiver as VoteReceiver,
    },
    solana_sdk::{
        clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
        pubkey::Pubkey,
        signature::{Keypair, Signer},
    },
    std::{
        collections::{BTreeMap, HashMap},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Condvar, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

/// Banks that have completed replay, but are yet to be voted on
pub(crate) type PendingBlocks = BTreeMap<Slot, Arc<Bank>>;

/// Communication with the block creation loop to notify leader window
#[derive(Default)]
pub struct LeaderWindowNotifier {
    pub window_info: Mutex<Option<LeaderWindowInfo>>,
    pub window_notification: Condvar,
}

// Implement a destructor for the VotingLoop thread to signal it exited
// even on panics
pub(crate) struct Finalizer {
    exit_sender: Arc<AtomicBool>,
}

impl Finalizer {
    pub(crate) fn new(exit_sender: Arc<AtomicBool>) -> Self {
        Finalizer { exit_sender }
    }
}

// Implement a destructor for Finalizer.
impl Drop for Finalizer {
    fn drop(&mut self) {
        self.exit_sender.clone().store(true, Ordering::Relaxed);
    }
}

/// Inputs to the voting loop
pub struct VotingLoopConfig {
    pub exit: Arc<AtomicBool>,
    // Validator config
    pub vote_account: Pubkey,
    pub wait_to_vote_slot: Option<Slot>,
    pub wait_for_vote_to_start_leader: bool,
    pub vote_history: VoteHistory,
    pub vote_history_storage: Arc<dyn VoteHistoryStorage>,

    // Shared state
    pub authorized_voter_keypairs: Arc<RwLock<Vec<Arc<Keypair>>>>,
    pub blockstore: Arc<Blockstore>,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub cluster_info: Arc<ClusterInfo>,
    pub leader_schedule_cache: Arc<LeaderScheduleCache>,
    pub rpc_subscriptions: Arc<RpcSubscriptions>,

    // Senders / Notifiers
    pub accounts_background_request_sender: AbsRequestSender,
    pub bls_sender: Sender<BLSOp>,
    pub commitment_sender: Sender<AlpenglowCommitmentAggregationData>,
    pub drop_bank_sender: Sender<Vec<BankWithScheduler>>,
    pub bank_notification_sender: Option<BankNotificationSenderConfig>,
    pub leader_window_notifier: Arc<LeaderWindowNotifier>,
    pub certificate_sender: Sender<(CertificateId, CertificateMessage)>,

    // Receivers
    pub completed_block_receiver: CompletedBlockReceiver,
    pub vote_receiver: VoteReceiver,
}

/// Context shared with replay, gossip, banking stage etc
struct SharedContext {
    blockstore: Arc<Blockstore>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    bank_forks: Arc<RwLock<BankForks>>,
    rpc_subscriptions: Arc<RpcSubscriptions>,
    vote_receiver: VoteReceiver,
    my_pubkey: Pubkey,
}

pub fn log_leader_change(
    my_pubkey: &Pubkey,
    bank_slot: Slot,
    current_leader: &mut Option<Pubkey>,
    new_leader: &Pubkey,
) {
    if let Some(ref current_leader) = current_leader {
        if current_leader != new_leader {
            let msg = if current_leader == my_pubkey {
                ". I am no longer the leader"
            } else if new_leader == my_pubkey {
                ". I am now the leader"
            } else {
                ""
            };
            info!(
                "LEADER CHANGE at slot: {} leader: {}{}",
                bank_slot, new_leader, msg
            );
        }
    }
    current_leader.replace(new_leader.to_owned());
}

pub struct VotingLoop {
    t_voting_loop: JoinHandle<()>,
}

impl VotingLoop {
    pub fn new(config: VotingLoopConfig) -> Self {
        let voting_loop = move || {
            Self::voting_loop(config);
        };

        let t_voting_loop = Builder::new()
            .name("solVotingLoop".to_string())
            .spawn(voting_loop)
            .unwrap();

        Self { t_voting_loop }
    }

    fn voting_loop(config: VotingLoopConfig) {
        let VotingLoopConfig {
            exit,
            vote_account,
            wait_to_vote_slot,
            wait_for_vote_to_start_leader,
            mut vote_history,
            vote_history_storage,
            authorized_voter_keypairs,
            blockstore,
            bank_forks,
            cluster_info,
            leader_schedule_cache,
            rpc_subscriptions,
            accounts_background_request_sender,
            bls_sender,
            commitment_sender,
            drop_bank_sender,
            bank_notification_sender,
            leader_window_notifier,
            certificate_sender,
            completed_block_receiver,
            vote_receiver,
        } = config;

        let _exit = Finalizer::new(exit.clone());
        let mut root_bank_cache = RootBankCache::new(bank_forks.clone());

        let mut identity_keypair = cluster_info.keypair().clone();
        let mut my_pubkey = identity_keypair.pubkey();
        let has_new_vote_been_rooted = !wait_for_vote_to_start_leader;
        // TODO(ashwin): handle set identity here and in loop
        // reminder prev 3 need to be mutable
        if my_pubkey != vote_history.node_pubkey {
            // set-identity has been called during startup
            let my_old_pubkey = vote_history.node_pubkey;
            vote_history = match VoteHistory::restore(vote_history_storage.as_ref(), &my_pubkey) {
                Ok(vote_history) => vote_history,
                Err(err) => {
                    error!(
                            "Unable to load new vote history when attempting to change identity from {} \
                             to {} on voting loop startup, Exiting: {}",
                            my_old_pubkey, my_pubkey, err
                        );
                    return;
                }
            };
            warn!(
                "Identity changed during startup from {} to {}",
                my_old_pubkey, my_pubkey
            );
        }

        let mut current_leader = None;
        let mut current_slot = {
            let bank_forks_slot = root_bank_cache.root_bank().slot();
            let vote_history_slot = vote_history.root();
            if bank_forks_slot != vote_history_slot {
                panic!(
                    "{my_pubkey}: Mismatch bank forks root {bank_forks_slot} vs vote history root {vote_history_slot}"
                );
            }
            let slot = bank_forks_slot.saturating_add(1);
            info!(
                "{my_pubkey}: Starting voting loop from {slot}: root {}",
                root_bank_cache.root_bank().slot()
            );
            slot
        };

        let mut cert_pool = certificate_pool::load_from_blockstore(
            &my_pubkey,
            &root_bank_cache.root_bank(),
            blockstore.as_ref(),
            Some(certificate_sender),
        );
        let mut pending_blocks = PendingBlocks::default();

        let mut voting_context = VotingContext {
            vote_history,
            vote_account_pubkey: vote_account,
            identity_keypair: identity_keypair.clone(),
            authorized_voter_keypairs,
            derived_bls_keypairs: HashMap::new(),
            has_new_vote_been_rooted,
            bls_sender,
            commitment_sender,
            wait_to_vote_slot,
            voted_signatures: vec![],
            root_bank_cache: root_bank_cache.clone(),
        };
        let mut shared_context = SharedContext {
            blockstore: blockstore.clone(),
            leader_schedule_cache: leader_schedule_cache.clone(),
            bank_forks: bank_forks.clone(),
            rpc_subscriptions: rpc_subscriptions.clone(),
            vote_receiver,
            my_pubkey,
        };

        // TODO(ashwin): Start loop once migration is complete current_slot from vote history
        loop {
            // Check if set-identity was called during runtime. Do this before the `is_leader` check.
            if my_pubkey != cluster_info.id() {
                identity_keypair = cluster_info.keypair().clone();
                let my_old_pubkey = my_pubkey;
                my_pubkey = identity_keypair.pubkey();

                voting_context.vote_history = match VoteHistory::restore(
                    vote_history_storage.as_ref(),
                    &my_pubkey,
                ) {
                    Ok(vote_history) => vote_history,
                    Err(err) => {
                        error!(
                                "Unable to load new vote history when attempting to change identity from {} \
                                 to {} in voting loop, Exiting: {}",
                                my_old_pubkey, my_pubkey, err
                            );
                        return;
                    }
                };

                // Update contexts with new identity
                voting_context.identity_keypair = identity_keypair.clone();
                shared_context.my_pubkey = my_pubkey;

                // Update the certificate pool's pubkey (which is used for logging purposes)
                cert_pool.update_pubkey(my_pubkey);

                warn!("{my_pubkey}: Identity changed from {my_old_pubkey} to {my_pubkey}");
            }

            let leader_end_slot = last_of_consecutive_leader_slots(current_slot);

            let Some(leader_pubkey) = leader_schedule_cache
                .slot_leader_at(current_slot, Some(root_bank_cache.root_bank().as_ref()))
            else {
                error!("Unable to compute the leader at slot {current_slot}. Something is wrong, exiting");
                return;
            };
            let is_leader = leader_pubkey == my_pubkey;

            // Create a timer for the leader window
            let skip_timer = Instant::now();
            let timeouts: Vec<_> = (0..(NUM_CONSECUTIVE_LEADER_SLOTS as usize))
                .map(skip_timeout)
                .collect();

            if is_leader {
                let start_slot = first_of_consecutive_leader_slots(current_slot).max(1);
                // Let the block creation loop know it is time for it to produce the window
                match cert_pool
                    .parent_ready_tracker
                    .block_production_parent(current_slot)
                {
                    BlockProductionParent::MissedWindow => {
                        warn!(
                            "{my_pubkey}: Leader slot {start_slot} has already been certified, \
                            skipping production of {start_slot}-{leader_end_slot}"
                        );
                    }
                    BlockProductionParent::ParentNotReady => {
                        // This can't happen before we start optimistically producing blocks
                        // When optimistically producing blocks, we can check for the parent in the block creation loop
                        panic!(
                            "Must have a block production parent in sequential voting loop: {:#?}",
                            cert_pool.parent_ready_tracker
                        );
                    }
                    BlockProductionParent::Parent(parent_block) => {
                        Self::notify_block_creation_loop_of_leader_window(
                            &my_pubkey,
                            &leader_window_notifier,
                            start_slot,
                            leader_end_slot,
                            parent_block,
                            skip_timer,
                        );
                    }
                };
            }

            log_leader_change(
                &my_pubkey,
                current_slot,
                &mut current_leader,
                &leader_pubkey,
            );

            while current_slot <= leader_end_slot {
                let leader_slot_index = leader_slot_index(current_slot);
                let timeout = timeouts[leader_slot_index];
                let cert_log_timer = Instant::now();

                info!(
                    "{my_pubkey}: Entering voting loop for slot: {current_slot}, is_leader: {is_leader}. Skip timer {} vs timeout {}",
                    skip_timer.elapsed().as_millis(), timeout.as_millis()
                );

                // Wait until we either vote notarize or skip
                while !voting_context.vote_history.voted(current_slot) {
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }

                    // If timer is reached, vote skip
                    if skip_timer.elapsed().as_millis() > timeout.as_millis() {
                        Self::try_skip_window(
                            &my_pubkey,
                            current_slot,
                            leader_end_slot,
                            &mut root_bank_cache,
                            &mut cert_pool,
                            &mut voting_context,
                        );
                        debug_assert!(voting_context.vote_history.voted(current_slot));
                        break;
                    }

                    // Check if replay has successfully completed
                    if let Some(bank) = pending_blocks.get(&current_slot) {
                        // Vote notarize
                        if Self::try_notar(
                            &my_pubkey,
                            bank.as_ref(),
                            &mut cert_pool,
                            &mut voting_context,
                        ) {
                            debug_assert!(voting_context.vote_history.voted(current_slot));
                            pending_blocks.remove(&current_slot);
                            break;
                        }
                    } else {
                        // Block on replay ingest
                        match completed_block_receiver
                            .recv_timeout(timeout.saturating_sub(skip_timer.elapsed()))
                        {
                            Ok(CompletedBlock { slot, bank }) => {
                                pending_blocks.insert(slot, bank);
                                // Reassess if we can vote
                                continue;
                            }
                            Err(RecvTimeoutError::Timeout) => continue,
                            Err(RecvTimeoutError::Disconnected) => return,
                        }
                    }

                    // Block on certificate ingest
                    if let Err(e) = Self::ingest_votes_into_certificate_pool(
                        &my_pubkey,
                        &shared_context.vote_receiver,
                        &mut cert_pool,
                        &voting_context.commitment_sender,
                        timeout.saturating_sub(skip_timer.elapsed()),
                    ) {
                        error!("{my_pubkey}: error ingesting votes into certificate pool, exiting: {e:?}");
                        // Finalizer will set exit flag
                        return;
                    }
                }

                // Wait for certificates to indicate we can move to the next slot
                let mut refresh_timer = Instant::now();
                while cert_pool.parent_ready_tracker.highest_parent_ready() <= current_slot {
                    if exit.load(Ordering::Relaxed) {
                        return;
                    }

                    if let Err(e) = Self::ingest_votes_into_certificate_pool(
                        &my_pubkey,
                        &shared_context.vote_receiver,
                        &mut cert_pool,
                        &voting_context.commitment_sender,
                        // Need to provide a timeout to check exit flag
                        Duration::from_secs(1),
                    ) {
                        error!("{my_pubkey}: error ingesting votes into certificate pool, exiting: {e:?}");
                        // Finalizer will set exit flag
                        return;
                    }

                    // Send fallback votes
                    Self::try_notar_fallback(
                        &my_pubkey,
                        current_slot,
                        leader_end_slot,
                        &mut root_bank_cache,
                        &mut cert_pool,
                        &mut voting_context,
                    );
                    Self::try_skip_fallback(
                        &my_pubkey,
                        current_slot,
                        leader_end_slot,
                        &mut root_bank_cache,
                        &mut cert_pool,
                        &mut voting_context,
                    );

                    // Refresh votes and latest finalization certificate if no progress is made
                    if refresh_timer.elapsed() > Duration::from_secs(1) {
                        Self::refresh_votes_and_cert(
                            &my_pubkey,
                            &mut cert_pool,
                            &mut voting_context,
                        );
                        refresh_timer = Instant::now();
                    }
                }

                info!(
                    "{my_pubkey}: Slot {current_slot} certificate observed in {} ms. Skip timer {} vs timeout {}",
                    cert_log_timer.elapsed().as_millis(),
                    skip_timer.elapsed().as_millis(),
                    timeout.as_millis(),
                );

                Self::try_final(
                    &my_pubkey,
                    current_slot,
                    root_bank_cache.root_bank().as_ref(),
                    &mut cert_pool,
                    &mut voting_context,
                );
                current_slot = current_slot.saturating_add(1);
            }

            // Set new root
            maybe_set_root(
                leader_end_slot,
                &mut cert_pool,
                &mut pending_blocks,
                &accounts_background_request_sender,
                &bank_notification_sender,
                &drop_bank_sender,
                &shared_context.blockstore,
                &shared_context.leader_schedule_cache,
                &shared_context.bank_forks,
                &shared_context.rpc_subscriptions,
                &shared_context.my_pubkey,
                &mut voting_context.vote_history,
                &mut voting_context.has_new_vote_been_rooted,
                &mut voting_context.voted_signatures,
            );

            // TODO(ashwin): If we were the leader for `current_slot` and the bank has not completed,
            // we can abandon the bank now
        }
    }

    /// Attempts to create and send a skip vote for all unvoted slots in `[start, end]`
    fn try_skip_window(
        my_pubkey: &Pubkey,
        start: Slot,
        end: Slot,
        root_bank_cache: &mut RootBankCache,
        cert_pool: &mut CertificatePool,
        voting_context: &mut VotingContext,
    ) {
        let bank = root_bank_cache.root_bank();

        for slot in start..=end {
            if !voting_context.vote_history.voted(slot) {
                info!(
                    "{my_pubkey}: Voting skip for slot {slot} with blockhash from {}",
                    bank.slot()
                );
                let vote = Vote::new_skip_vote(slot);
                if !send_vote(vote, false, bank.as_ref(), cert_pool, voting_context) {
                    warn!("send_vote failed for {:?}", vote);
                }
            }
        }
    }

    /// Check if we can vote finalize for `slot` at this time.
    /// If so send the vote, update vote history and return `true`.
    /// The conditions are:
    /// - We have not voted `Skip`, `SkipFallback` or `NotarizeFallback` in `slot`
    /// - We voted notarize for some block `b` in `slot`
    /// - Block `b` has a notarization certificate
    fn try_final(
        my_pubkey: &Pubkey,
        slot: Slot,
        bank: &Bank,
        cert_pool: &mut CertificatePool,
        voting_context: &mut VotingContext,
    ) -> bool {
        if voting_context.vote_history.its_over(slot) {
            // Already voted finalize
            return false;
        }
        if voting_context.vote_history.skipped(slot) {
            // Cannot have voted a skip variant
            return false;
        }
        let Some((block_id, bank_hash)) = voting_context.vote_history.voted_notar(slot) else {
            // Must have voted notarize in order to vote finalize
            return false;
        };
        if !cert_pool.is_notarized(slot, block_id, bank_hash) {
            // Must have a notarization certificate
            return false;
        }
        info!(
            "{my_pubkey}: Voting finalize for slot {slot} with blockhash from {}",
            bank.slot()
        );
        let vote = Vote::new_finalization_vote(slot);
        send_vote(vote, false, bank, cert_pool, voting_context)
    }

    /// Check if the stored certificates and block id allow us to vote notarize for `bank` at this time.
    /// If so update vote history and attempt to vote finalize.
    /// The conditions are:
    /// - If this is the first leader block of this window, check for notarization and skip certificates
    /// - Else ensure that this is a consecutive slot and that we have voted notarize on the parent
    ///
    /// Returns false if these conditions fail
    fn try_notar(
        my_pubkey: &Pubkey,
        bank: &Bank,
        cert_pool: &mut CertificatePool,
        voting_context: &mut VotingContext,
    ) -> bool {
        debug_assert!(bank.is_frozen());
        let slot = bank.slot();
        let leader_slot_index = leader_slot_index(slot);

        let parent_slot = bank.parent_slot();
        let parent_bank_hash = bank.parent_hash();
        let parent_block_id = bank
            .parent_block_id()
            // To account for child of genesis and snapshots we allow default block id
            .unwrap_or_default();

        // Check if the certificates are valid for us to vote notarize.
        // - If this is the first leader slot (leader index = 0 or slot = 1) check
        //   that we are parent ready:
        //      - The parent is notarized fallback
        //      - OR the parent is genesis / slot 1 (WFSM hack)
        //      - All slots between the parent and this slot are skip certified
        // - If this is not the first leader slot check
        //      - The slot is consecutive to the parent slot
        //      - We voted notarize on the parent block
        if leader_slot_index == 0 || slot == 1 {
            if !cert_pool
                .parent_ready_tracker
                .parent_ready(slot, (parent_slot, parent_block_id, parent_bank_hash))
            {
                // Need to ingest more votes
                return false;
            }
        } else {
            if parent_slot.saturating_add(1) != slot {
                // Non consecutive
                return false;
            }
            if voting_context.vote_history.voted_notar(parent_slot)
                != Some((parent_block_id, parent_bank_hash))
            {
                // Voted skip or notarize on a different version of the parent
                return false;
            }
        }

        // Broadcast notarize vote
        let voted = Self::vote_notarize(my_pubkey, bank, cert_pool, voting_context);

        // Try to finalize
        Self::try_final(my_pubkey, slot, bank, cert_pool, voting_context);
        voted
    }

    /// Create and send a Notarization or Finalization vote
    fn vote_notarize(
        my_pubkey: &Pubkey,
        bank: &Bank,
        cert_pool: &mut CertificatePool,
        voting_context: &mut VotingContext,
    ) -> bool {
        debug_assert!(bank.is_frozen());
        debug_assert!(bank.block_id().is_some());
        let slot = bank.slot();
        let hash = bank.hash();
        let block_id = bank
            .block_id()
            .expect("Block id must be present for all banks in pending_blocks");

        info!("{my_pubkey}: Voting notarize for slot {slot} hash {hash} block_id {block_id}");
        let vote = Vote::new_notarization_vote(slot, block_id, hash);
        if !send_vote(vote, false, bank, cert_pool, voting_context) {
            return false;
        }

        alpenglow_update_commitment_cache(
            AlpenglowCommitmentType::Notarize,
            slot,
            &voting_context.commitment_sender,
        );
        true
    }

    /// Consider voting notarize fallback for this slot
    /// for each b' = safeToNotar(slot)
    /// try to skip the window, and vote notarize fallback b'
    fn try_notar_fallback(
        my_pubkey: &Pubkey,
        slot: Slot,
        leader_end_slot: Slot,
        root_bank_cache: &mut RootBankCache,
        cert_pool: &mut CertificatePool,
        voting_context: &mut VotingContext,
    ) -> bool {
        let blocks = cert_pool.safe_to_notar(&voting_context.vote_account_pubkey, slot);
        if blocks.is_empty() {
            return false;
        }
        Self::try_skip_window(
            my_pubkey,
            slot,
            leader_end_slot,
            root_bank_cache,
            cert_pool,
            voting_context,
        );
        if voting_context.vote_history.its_over(slot) {
            return false;
        }
        for (block_id, bank_hash) in blocks.into_iter() {
            if voting_context
                .vote_history
                .voted_notar_fallback(slot, block_id, bank_hash)
            {
                continue;
            }
            info!("{my_pubkey}: Voting notarize fallback for slot {slot} hash {bank_hash} block_id {block_id}");
            let vote = Vote::new_notarization_fallback_vote(slot, block_id, bank_hash);
            if !send_vote(
                vote,
                false,
                root_bank_cache.root_bank().as_ref(),
                cert_pool,
                voting_context,
            ) {
                warn!("send_vote failed for {:?}", vote);
            }
        }
        true
    }

    /// Consider voting skip fallback for this slot
    /// if safeToSkip(slot)
    /// then try to skip the window, and vote skip fallback
    fn try_skip_fallback(
        my_pubkey: &Pubkey,
        slot: Slot,
        leader_end_slot: Slot,
        root_bank_cache: &mut RootBankCache,
        cert_pool: &mut CertificatePool,
        voting_context: &mut VotingContext,
    ) -> bool {
        if !cert_pool.safe_to_skip(&voting_context.vote_account_pubkey, slot) {
            return false;
        }
        Self::try_skip_window(
            my_pubkey,
            slot,
            leader_end_slot,
            root_bank_cache,
            cert_pool,
            voting_context,
        );
        if voting_context.vote_history.its_over(slot)
            || voting_context.vote_history.voted_skip_fallback(slot)
        {
            return false;
        }
        info!("{my_pubkey}: Voting skip fallback for slot {slot}");
        let vote = Vote::new_skip_fallback_vote(slot);
        send_vote(
            vote,
            false,
            root_bank_cache.root_bank().as_ref(),
            cert_pool,
            voting_context,
        )
    }

    /// Refresh the highest recent finalization certificate
    /// For each slot past this up to our current slot `slot`, refresh our votes
    fn refresh_votes_and_cert(
        my_pubkey: &Pubkey,
        cert_pool: &mut CertificatePool,
        voting_context: &mut VotingContext,
    ) {
        let highest_finalization_slot = cert_pool
            .highest_finalized_slot()
            .max(voting_context.root_bank_cache.root_bank().slot());
        // TODO: rebroadcast finalization cert for block once we have BLS
        // This includes the notarized fallback cert if it was a slow finalization

        // Refresh votes for all slots up to our current slot
        for vote in voting_context
            .vote_history
            .votes_cast_since(highest_finalization_slot)
        {
            info!("{my_pubkey}: Refreshing vote {vote:?}");
            if !send_vote(
                vote,
                true,
                voting_context.root_bank_cache.root_bank().as_ref(),
                cert_pool,
                voting_context,
            ) {
                warn!("send_vote failed for {:?}", vote);
            }
        }
    }

    /// Ingest votes from all to all, tpu, and gossip into the certificate pool
    ///
    /// Returns the highest slot of the newly created notarization/skip certificates
    fn ingest_votes_into_certificate_pool(
        my_pubkey: &Pubkey,
        vote_receiver: &VoteReceiver,
        cert_pool: &mut CertificatePool,
        commitment_sender: &Sender<AlpenglowCommitmentAggregationData>,
        timeout: Duration,
    ) -> Result<(), AddVoteError> {
        let add_to_cert_pool = |bls_message: BLSMessage| {
            trace!("{my_pubkey}: ingesting BLS Message: {bls_message:?}");
            add_message_and_maybe_update_commitment(
                my_pubkey,
                &bls_message,
                cert_pool,
                commitment_sender,
            )
            .or_else(|e| match e {
                AddVoteError::CertificateSenderError => Err(e),
                e => {
                    // TODO(ashwin): increment metrics on non duplicate failures
                    trace!("{my_pubkey}: unable to push vote into the pool {}", e);
                    Ok(())
                }
            })
        };

        let Ok(first) = vote_receiver.recv_timeout(timeout) else {
            // Either timeout or sender disconnected, return so we can check exit
            return Ok(());
        };
        std::iter::once(first)
            .chain(vote_receiver.try_iter())
            .try_for_each(add_to_cert_pool)
    }

    /// Notifies the block creation loop of a new leader window to produce.
    /// Caller should use the highest certified slot (not skip) as the `parent_slot`
    ///
    /// Fails to notify and returns false if the leader window has already
    /// been skipped, or if the parent is greater than or equal to the first leader slot
    fn notify_block_creation_loop_of_leader_window(
        my_pubkey: &Pubkey,
        leader_window_notifier: &LeaderWindowNotifier,
        start_slot: Slot,
        end_slot: Slot,
        parent_block @ (parent_slot, _, _): Block,
        skip_timer: Instant,
    ) -> bool {
        debug_assert!(parent_slot < start_slot);

        // Notify the block creation loop.
        let mut l_window_info = leader_window_notifier.window_info.lock().unwrap();
        if let Some(window_info) = l_window_info.as_ref() {
            error!(
                "{my_pubkey}: Attempting to start leader window for {start_slot}-{end_slot}, however there is \
                already a pending window to produce {}-{}. Something has gone wrong, Overwriting in favor \
                of the newer window",
                window_info.start_slot,
                window_info.end_slot,
            );
        }
        *l_window_info = Some(LeaderWindowInfo {
            start_slot,
            end_slot,
            parent_block,
            skip_timer,
        });
        leader_window_notifier.window_notification.notify_one();
        true
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_voting_loop.join().map(|_| ())
    }
}
