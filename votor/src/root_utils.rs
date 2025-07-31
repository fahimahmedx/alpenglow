use {
    crate::{event_handler::PendingBlocks, voting_utils::VotingContext, votor::SharedContext},
    crossbeam_channel::Sender,
    solana_ledger::{blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache},
    solana_rpc::{
        optimistically_confirmed_bank_tracker::{BankNotification, BankNotificationSenderConfig},
        rpc_subscriptions::RpcSubscriptions,
    },
    solana_runtime::{
        accounts_background_service::AbsRequestSender,
        bank_forks::{BankForks, SetRootError},
        installed_scheduler_pool::BankWithScheduler,
    },
    solana_sdk::{
        clock::Slot, hash::Hash, pubkey::Pubkey, signature::Signature, timing::timestamp,
    },
    solana_votor_messages::bls_message::Block,
    std::{
        collections::BTreeSet,
        sync::{Arc, RwLock},
    },
};

/// Structures that are not used in the event loop, but need to be updated
/// or notified when setting root
pub(crate) struct RootContext {
    pub(crate) leader_schedule_cache: Arc<LeaderScheduleCache>,
    pub(crate) accounts_background_request_sender: AbsRequestSender,
    pub(crate) bank_notification_sender: Option<BankNotificationSenderConfig>,
    pub(crate) drop_bank_sender: Sender<Vec<BankWithScheduler>>,
}

/// Sets the root for the votor event handling loop. Handles rooting all things
/// except the certificate pool
pub(crate) fn set_root(
    my_pubkey: &Pubkey,
    new_root: Slot,
    ctx: &SharedContext,
    vctx: &mut VotingContext,
    rctx: &RootContext,
    pending_blocks: &mut PendingBlocks,
    finalized_blocks: &mut BTreeSet<Block>,
) -> Result<(), SetRootError> {
    info!("{my_pubkey}: setting root {new_root}");
    vctx.vote_history.set_root(new_root);
    *pending_blocks = pending_blocks.split_off(&new_root);
    *finalized_blocks = finalized_blocks.split_off(&(new_root, Hash::default()));

    check_and_handle_new_root(
        new_root,
        new_root,
        &rctx.accounts_background_request_sender,
        Some(new_root),
        &rctx.bank_notification_sender,
        &rctx.drop_bank_sender,
        &ctx.blockstore,
        &rctx.leader_schedule_cache,
        &ctx.bank_forks,
        &ctx.rpc_subscriptions,
        my_pubkey,
        &mut false,
        &mut vec![],
        |_| {},
    )?;

    // Distinguish between duplicate versions of same slot
    let hash = ctx.bank_forks.read().unwrap().bank_hash(new_root).unwrap();
    if let Err(e) =
        ctx.blockstore
            .insert_optimistic_slot(new_root, &hash, timestamp().try_into().unwrap())
    {
        error!(
            "failed to record optimistic slot in blockstore: slot={}: {:?}",
            new_root, &e
        );
    }

    // It is critical to send the OC notification in order to keep compatibility with
    // the RPC API. Additionally the PrioritizationFeeCache relies on this notification
    // in order to perform cleanup. In the future we will look to deprecate OC and remove
    // these code paths.
    if let Some(config) = &rctx.bank_notification_sender {
        // TODO: propagate error
        let _ = config
            .sender
            .send(BankNotification::OptimisticallyConfirmed(new_root));
    }
    Ok(())
}

/// Sets the new root, additionally performs the callback after setting the bank forks root
/// During this transition period where both replay stage and voting loop can root depending on the feature flag we
/// have a callback that cleans up progress map and other tower bft structures. Then the callgraph is
///
/// ReplayStage::check_and_handle_new_root -> root_utils::check_and_handle_new_root(callback)
///                                                             |
///                                                             v
/// ReplayStage::handle_new_root           -> root_utils::set_bank_forks_root(callback) -> callback()
#[allow(clippy::too_many_arguments)]
pub fn check_and_handle_new_root<CB>(
    parent_slot: Slot,
    new_root: Slot,
    accounts_background_request_sender: &AbsRequestSender,
    highest_super_majority_root: Option<Slot>,
    bank_notification_sender: &Option<BankNotificationSenderConfig>,
    drop_bank_sender: &Sender<Vec<BankWithScheduler>>,
    blockstore: &Blockstore,
    leader_schedule_cache: &Arc<LeaderScheduleCache>,
    bank_forks: &RwLock<BankForks>,
    rpc_subscriptions: &Arc<RpcSubscriptions>,
    my_pubkey: &Pubkey,
    has_new_vote_been_rooted: &mut bool,
    voted_signatures: &mut Vec<Signature>,
    callback: CB,
) -> Result<(), SetRootError>
where
    CB: FnOnce(&BankForks),
{
    // get the root bank before squash
    let root_bank = bank_forks
        .read()
        .unwrap()
        .get(new_root)
        .expect("Root bank doesn't exist");
    let mut rooted_banks = root_bank.parents();
    let oldest_parent = rooted_banks.last().map(|last| last.parent_slot());
    rooted_banks.push(root_bank.clone());
    let rooted_slots: Vec<_> = rooted_banks.iter().map(|bank| bank.slot()).collect();
    // The following differs from rooted_slots by including the parent slot of the oldest parent bank.
    let rooted_slots_with_parents = bank_notification_sender
        .as_ref()
        .is_some_and(|sender| sender.should_send_parents)
        .then(|| {
            let mut new_chain = rooted_slots.clone();
            new_chain.push(oldest_parent.unwrap_or(parent_slot));
            new_chain
        });

    // Call leader schedule_cache.set_root() before blockstore.set_root() because
    // bank_forks.root is consumed by repair_service to update gossip, so we don't want to
    // get shreds for repair on gossip before we update leader schedule, otherwise they may
    // get dropped.
    leader_schedule_cache.set_root(rooted_banks.last().unwrap());
    blockstore
        .set_roots(rooted_slots.iter())
        .expect("Ledger set roots failed");
    set_bank_forks_root(
        new_root,
        bank_forks,
        accounts_background_request_sender,
        highest_super_majority_root,
        has_new_vote_been_rooted,
        voted_signatures,
        drop_bank_sender,
        callback,
    )?;
    blockstore.slots_stats.mark_rooted(new_root);
    rpc_subscriptions.notify_roots(rooted_slots);
    if let Some(sender) = bank_notification_sender {
        sender
            .sender
            .send(BankNotification::NewRootBank(root_bank))
            .unwrap_or_else(|err| warn!("bank_notification_sender failed: {:?}", err));

        if let Some(new_chain) = rooted_slots_with_parents {
            sender
                .sender
                .send(BankNotification::NewRootedChain(new_chain))
                .unwrap_or_else(|err| warn!("bank_notification_sender failed: {:?}", err));
        }
    }
    info!("{} new root {}", my_pubkey, new_root);
    Ok(())
}

/// Sets the bank forks root:
/// - Prune the program cache
/// - Prune bank forks and drop the removed banks
/// - Calls the callback for use in replay stage and tests
pub fn set_bank_forks_root<CB>(
    new_root: Slot,
    bank_forks: &RwLock<BankForks>,
    accounts_background_request_sender: &AbsRequestSender,
    highest_super_majority_root: Option<Slot>,
    has_new_vote_been_rooted: &mut bool,
    voted_signatures: &mut Vec<Signature>,
    drop_bank_sender: &Sender<Vec<BankWithScheduler>>,
    callback: CB,
) -> Result<(), SetRootError>
where
    CB: FnOnce(&BankForks),
{
    bank_forks.read().unwrap().prune_program_cache(new_root);
    let removed_banks = bank_forks.write().unwrap().set_root(
        new_root,
        accounts_background_request_sender,
        highest_super_majority_root,
    )?;

    drop_bank_sender
        .send(removed_banks)
        .unwrap_or_else(|err| warn!("bank drop failed: {:?}", err));

    // Dropping the bank_forks write lock and reacquiring as a read lock is
    // safe because updates to bank_forks are only made by a single thread.
    // TODO(ashwin): Once PR #245 lands move this back to ReplayStage
    let r_bank_forks = bank_forks.read().unwrap();
    let new_root_bank = &r_bank_forks[new_root];
    if !*has_new_vote_been_rooted {
        for signature in voted_signatures.iter() {
            if new_root_bank.get_signature_status(signature).is_some() {
                *has_new_vote_been_rooted = true;
                break;
            }
        }
        if *has_new_vote_been_rooted {
            std::mem::take(voted_signatures);
        }
    }
    callback(&r_bank_forks);
    Ok(())
}
