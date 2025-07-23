use {
    crate::{
        certificate_pool::{AddVoteError, CertificatePool},
        commitment::{
            alpenglow_update_commitment_cache, AlpenglowCommitmentAggregationData,
            AlpenglowCommitmentType,
        },
        event::VotorEvent,
        vote_history::VoteHistory,
        vote_history_storage::{SavedVoteHistory, SavedVoteHistoryVersions},
    },
    alpenglow_vote::{
        bls_message::{BLSMessage, CertificateMessage, VoteMessage, BLS_KEYPAIR_DERIVE_SEED},
        vote::Vote,
    },
    crossbeam_channel::{SendError, Sender},
    solana_bls_signatures::{keypair::Keypair as BLSKeypair, BlsError, Pubkey as BLSPubkey},
    solana_runtime::{
        bank::Bank, root_bank_cache::RootBankCache, vote_sender_types::BLSVerifiedMessageSender,
    },
    solana_sdk::{
        clock::Slot,
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
        transaction::Transaction,
    },
    std::{collections::HashMap, sync::Arc},
    thiserror::Error,
};

// TODO(ashwin): This will be removed in PR #245
const MAX_VOTE_SIGNATURES: usize = 200;

#[derive(Debug)]
pub enum GenerateVoteTxResult {
    // non voting validator, not eligible for refresh
    // until authorized keypair is overriden
    NonVoting,
    // hot spare validator, not eligble for refresh
    // until set identity is invoked
    HotSpare,
    // failed generation, eligible for refresh
    Failed,
    // no rank found.
    NoRankFound,
    // Generated a vote transaction
    Tx(Transaction),
    // Generated a BLS message
    BLSMessage(BLSMessage),
}

impl GenerateVoteTxResult {
    pub fn is_non_voting(&self) -> bool {
        matches!(self, Self::NonVoting)
    }

    pub fn is_hot_spare(&self) -> bool {
        matches!(self, Self::HotSpare)
    }
}

pub enum BLSOp {
    PushVote {
        bls_message: Arc<BLSMessage>,
        slot: Slot,
        saved_vote_history: SavedVoteHistoryVersions,
    },
    PushCertificate {
        certificate: Arc<CertificateMessage>,
    },
}

#[derive(Debug, Error)]
pub enum VoteError {
    #[error("Unable to generate bls vote message")]
    GenerationError(Box<GenerateVoteTxResult>),

    #[error("Unable to send to certificate pool")]
    CertificatePoolError(#[from] SendError<()>),
}

/// Context required to construct vote transactions
pub struct VotingContext {
    pub vote_history: VoteHistory,
    pub vote_account_pubkey: Pubkey,
    pub identity_keypair: Arc<Keypair>,
    pub authorized_voter_keypairs: Arc<std::sync::RwLock<Vec<Arc<Keypair>>>>,
    // The BLS keypair should always change with authorized_voter_keypairs.
    pub derived_bls_keypairs: HashMap<Pubkey, Arc<BLSKeypair>>,
    pub has_new_vote_been_rooted: bool,
    pub own_vote_sender: BLSVerifiedMessageSender,
    pub bls_sender: Sender<BLSOp>,
    pub commitment_sender: Sender<AlpenglowCommitmentAggregationData>,
    pub wait_to_vote_slot: Option<u64>,
    pub voted_signatures: Vec<Signature>,
    pub root_bank_cache: RootBankCache,
}

pub fn get_bls_keypair(
    context: &mut VotingContext,
    authorized_voter_keypair: &Arc<Keypair>,
) -> Result<Arc<BLSKeypair>, BlsError> {
    let pubkey = authorized_voter_keypair.pubkey();
    if let Some(existing) = context.derived_bls_keypairs.get(&pubkey) {
        return Ok(existing.clone());
    }

    let bls_keypair = Arc::new(BLSKeypair::derive_from_signer(
        authorized_voter_keypair,
        BLS_KEYPAIR_DERIVE_SEED,
    )?);

    context
        .derived_bls_keypairs
        .insert(pubkey, bls_keypair.clone());

    Ok(bls_keypair)
}

pub fn generate_vote_tx(
    vote: &Vote,
    bank: &Bank,
    context: &mut VotingContext,
) -> GenerateVoteTxResult {
    let vote_account_pubkey = context.vote_account_pubkey;
    let authorized_voter_keypair;
    let bls_pubkey_in_vote_account;
    {
        let authorized_voter_keypairs = context.authorized_voter_keypairs.read().unwrap();
        if !bank.is_startup_verification_complete() {
            info!("startup verification incomplete, so unable to vote");
            return GenerateVoteTxResult::Failed;
        }
        if authorized_voter_keypairs.is_empty() {
            return GenerateVoteTxResult::NonVoting;
        }
        if let Some(slot) = context.wait_to_vote_slot {
            if vote.slot() < slot {
                return GenerateVoteTxResult::Failed;
            }
        }
        let Some(vote_account) = bank.get_vote_account(&context.vote_account_pubkey) else {
            warn!("Vote account {vote_account_pubkey} does not exist.  Unable to vote");
            return GenerateVoteTxResult::Failed;
        };
        let Some(vote_state) = vote_account.alpenglow_vote_state() else {
            warn!(
                "Vote account {vote_account_pubkey} does not have an Alpenglow vote state.  Unable to vote",
            );
            return GenerateVoteTxResult::Failed;
        };
        if *vote_state.node_pubkey() != context.identity_keypair.pubkey() {
            info!(
                "Vote account node_pubkey mismatch: {} (expected: {}).  Unable to vote",
                vote_state.node_pubkey(),
                context.identity_keypair.pubkey()
            );
            return GenerateVoteTxResult::HotSpare;
        }
        bls_pubkey_in_vote_account = match vote_account.bls_pubkey() {
            None => {
                panic!(
                    "No BLS pubkey in vote account {}",
                    context.identity_keypair.pubkey()
                );
            }
            Some(key) => *key,
        };

        let Some(authorized_voter_pubkey) = vote_state.get_authorized_voter(bank.epoch()) else {
            warn!("Vote account {vote_account_pubkey} has no authorized voter for epoch {}.  Unable to vote",
                bank.epoch()
            );
            return GenerateVoteTxResult::Failed;
        };

        let Some(keypair) = authorized_voter_keypairs
            .iter()
            .find(|keypair| keypair.pubkey() == authorized_voter_pubkey)
        else {
            warn!(
                "The authorized keypair {authorized_voter_pubkey} for vote account \
                 {vote_account_pubkey} is not available.  Unable to vote"
            );
            return GenerateVoteTxResult::NonVoting;
        };

        authorized_voter_keypair = keypair.clone();
    }

    let bls_keypair = get_bls_keypair(context, &authorized_voter_keypair)
        .unwrap_or_else(|e| panic!("Failed to derive my own BLS keypair: {e:?}"));
    let my_bls_pubkey: BLSPubkey = bls_keypair.public.into();
    if my_bls_pubkey != bls_pubkey_in_vote_account {
        panic!(
            "Vote account bls_pubkey mismatch: {:?} (expected: {:?}).  Unable to vote",
            bls_pubkey_in_vote_account, my_bls_pubkey
        );
    }
    let vote_serialized = bincode::serialize(&vote).unwrap();
    let signature = authorized_voter_keypair.sign_message(&vote_serialized);
    if !context.has_new_vote_been_rooted {
        context.voted_signatures.push(signature);
        if context.voted_signatures.len() > MAX_VOTE_SIGNATURES {
            context.voted_signatures.remove(0);
        }
    } else {
        context.voted_signatures.clear();
    }

    let Some(epoch_stakes) = bank.epoch_stakes(bank.epoch()) else {
        panic!(
            "The bank {} doesn't have its own epoch_stakes for {}",
            bank.slot(),
            bank.epoch()
        );
    };
    let Some(my_rank) = epoch_stakes
        .bls_pubkey_to_rank_map()
        .get_rank(&my_bls_pubkey)
    else {
        return GenerateVoteTxResult::NoRankFound;
    };
    GenerateVoteTxResult::BLSMessage(BLSMessage::Vote(VoteMessage {
        vote: *vote,
        signature: bls_keypair.sign(&vote_serialized).into(),
        rank: *my_rank,
    }))
}

/// Send an alpenglow vote as a BLSMessage
/// `bank` will be used for:
/// - startup verification
/// - vote account checks
/// - authorized voter checks
///
/// We also update the vote history and send the vote to
/// the certificate pool thread for ingestion.
///
/// Returns false if we are currently a non-voting node
pub(crate) fn insert_vote_and_create_bls_message(
    my_pubkey: &Pubkey,
    vote: Vote,
    is_refresh: bool,
    context: &mut VotingContext,
) -> Result<BLSOp, VoteError> {
    // Update and save the vote history
    if !is_refresh {
        context.vote_history.add_vote(vote);
    }

    let bank = context.root_bank_cache.root_bank();
    let bls_message = match generate_vote_tx(&vote, &bank, context) {
        GenerateVoteTxResult::BLSMessage(bls_message) => bls_message,
        e => {
            return Err(VoteError::GenerationError(Box::new(e)));
        }
    };
    context
        .own_vote_sender
        .send(bls_message.clone())
        .map_err(|_| SendError(()))?;

    // TODO: for refresh votes use a different BLSOp so we don't have to rewrite the same vote history to file
    let saved_vote_history =
        SavedVoteHistory::new(&context.vote_history, &context.identity_keypair).unwrap_or_else(
            |err| {
                error!(
                    "{my_pubkey}: Unable to create saved vote history: {:?}",
                    err
                );
                // TODO: maybe unify this with exit flag instead
                std::process::exit(1);
            },
        );

    // Return vote for sending
    Ok(BLSOp::PushVote {
        bls_message: Arc::new(bls_message),
        slot: vote.slot(),
        saved_vote_history: SavedVoteHistoryVersions::from(saved_vote_history),
    })
}

/// Adds a vote to the certificate pool and updates the commitment cache if necessary
///
/// If a new finalization slot was recognized, returns the slot
pub fn add_message_and_maybe_update_commitment(
    my_pubkey: &Pubkey,
    my_vote_pubkey: &Pubkey,
    message: &BLSMessage,
    cert_pool: &mut CertificatePool,
    votor_events: &mut Vec<VotorEvent>,
    commitment_sender: &Sender<AlpenglowCommitmentAggregationData>,
) -> Result<(Option<Slot>, Vec<Arc<CertificateMessage>>), AddVoteError> {
    let (new_finalized_slot, new_certificates_to_send) =
        cert_pool.add_message(my_vote_pubkey, message, votor_events)?;
    let Some(new_finalized_slot) = new_finalized_slot else {
        return Ok((None, new_certificates_to_send));
    };
    trace!("{my_pubkey}: new finalization certificate for {new_finalized_slot}");
    alpenglow_update_commitment_cache(
        AlpenglowCommitmentType::Finalized,
        new_finalized_slot,
        commitment_sender,
    );
    Ok((Some(new_finalized_slot), new_certificates_to_send))
}
