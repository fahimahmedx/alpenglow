use {
    crate::{certificate_pool::vote_certificate::VoteCertificate, Stake},
    alpenglow_vote::bls_message::VoteMessage,
    solana_pubkey::Pubkey,
    solana_sdk::hash::Hash,
    std::collections::{HashMap, HashSet},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct VotedBlockKey {
    pub(crate) bank_hash: Hash,
    pub(crate) block_id: Hash,
}

#[derive(Debug)]
pub(crate) struct VoteEntry {
    pub(crate) transactions: Vec<VoteMessage>,
    pub(crate) total_stake_by_key: Stake,
}

impl VoteEntry {
    pub fn new() -> Self {
        Self {
            transactions: Vec::new(),
            total_stake_by_key: 0,
        }
    }
}

pub(crate) trait VotePool {
    fn total_stake(&self) -> Stake;
    fn has_prev_validator_vote(&self, validator_vote_key: &Pubkey) -> bool;
}

/// There are two types of vote pools:
/// - SimpleVotePool: Tracks all votes of a specfic vote type made by validators for some slot N, but only one vote per block.
/// - DuplicateBlockVotePool: Tracks all votes of a specfic vote type made by validators for some slot N,
///   but allows votes for different blocks by the same validator. Only relevant for VotePool's that are of type
///   Notarization or NotarizationFallback
pub(crate) enum VotePoolType {
    SimpleVotePool(SimpleVotePool),
    DuplicateBlockVotePool(DuplicateBlockVotePool),
}

impl VotePoolType {
    pub(crate) fn total_stake(&self) -> Stake {
        match self {
            VotePoolType::SimpleVotePool(pool) => pool.total_stake(),
            VotePoolType::DuplicateBlockVotePool(pool) => pool.total_stake(),
        }
    }

    pub(crate) fn has_prev_validator_vote(&self, validator_vote_key: &Pubkey) -> bool {
        match self {
            VotePoolType::SimpleVotePool(pool) => pool.has_prev_validator_vote(validator_vote_key),
            VotePoolType::DuplicateBlockVotePool(pool) => {
                pool.has_prev_validator_vote(validator_vote_key)
            }
        }
    }

    pub(crate) fn unwrap_duplicate_block_vote_pool(
        &self,
        error_message: &str,
    ) -> &DuplicateBlockVotePool {
        match self {
            VotePoolType::SimpleVotePool(_pool) => panic!("{}", error_message),
            VotePoolType::DuplicateBlockVotePool(pool) => pool,
        }
    }
}

pub(crate) struct SimpleVotePool {
    /// Tracks all votes of a specfic vote type made by validators for some slot N.
    pub(crate) vote_entry: VoteEntry,
    prev_voted_validators: HashSet<Pubkey>,
}

impl SimpleVotePool {
    pub fn new() -> Self {
        Self {
            vote_entry: VoteEntry::new(),
            prev_voted_validators: HashSet::new(),
        }
    }

    pub fn add_vote(
        &mut self,
        validator_vote_key: &Pubkey,
        validator_stake: Stake,
        transaction: &VoteMessage,
    ) -> bool {
        if self.prev_voted_validators.contains(validator_vote_key) {
            return false;
        }
        self.prev_voted_validators.insert(*validator_vote_key);
        self.vote_entry.transactions.push(*transaction);
        self.vote_entry.total_stake_by_key = self
            .vote_entry
            .total_stake_by_key
            .saturating_add(validator_stake);
        true
    }

    pub fn add_to_certificate(&self, output: &mut VoteCertificate) {
        output.aggregate(self.vote_entry.transactions.iter())
    }
}

impl VotePool for SimpleVotePool {
    fn total_stake(&self) -> Stake {
        self.vote_entry.total_stake_by_key
    }
    fn has_prev_validator_vote(&self, validator_vote_key: &Pubkey) -> bool {
        self.prev_voted_validators.contains(validator_vote_key)
    }
}

pub(crate) struct DuplicateBlockVotePool {
    max_entries_per_pubkey: usize,
    pub(crate) votes: HashMap<VotedBlockKey, VoteEntry>,
    total_stake: Stake,
    prev_voted_block_keys: HashMap<Pubkey, Vec<VotedBlockKey>>,
    top_entry_stake: Stake,
}

impl DuplicateBlockVotePool {
    pub fn new(max_entries_per_pubkey: usize) -> Self {
        Self {
            max_entries_per_pubkey,
            votes: HashMap::new(),
            total_stake: 0,
            prev_voted_block_keys: HashMap::new(),
            top_entry_stake: 0,
        }
    }

    pub fn add_vote(
        &mut self,
        validator_vote_key: &Pubkey,
        voted_block_key: VotedBlockKey,
        transaction: &VoteMessage,
        validator_stake: Stake,
    ) -> bool {
        // Check whether the validator_vote_key already used the same voted_block_key or exceeded max_entries_per_pubkey
        // If so, return false, otherwise add the voted_block_key to the prev_votes
        let prev_voted_block_keys = self
            .prev_voted_block_keys
            .entry(*validator_vote_key)
            .or_default();
        if prev_voted_block_keys.contains(&voted_block_key) {
            return false;
        }
        let inserted_first_time = prev_voted_block_keys.is_empty();
        if prev_voted_block_keys.len() >= self.max_entries_per_pubkey {
            return false;
        }
        prev_voted_block_keys.push(voted_block_key.clone());

        let vote_entry = self
            .votes
            .entry(voted_block_key)
            .or_insert_with(VoteEntry::new);
        vote_entry.transactions.push(*transaction);
        vote_entry.total_stake_by_key = vote_entry
            .total_stake_by_key
            .saturating_add(validator_stake);

        if inserted_first_time {
            self.total_stake = self.total_stake.saturating_add(validator_stake);
        }
        if vote_entry.total_stake_by_key > self.top_entry_stake {
            self.top_entry_stake = vote_entry.total_stake_by_key;
        }
        true
    }

    pub fn total_stake_by_voted_block_key(&self, voted_block_key: &VotedBlockKey) -> Stake {
        self.votes
            .get(voted_block_key)
            .map_or(0, |vote_entries| vote_entries.total_stake_by_key)
    }

    pub fn add_to_certificate(
        &self,
        voted_block_key: &VotedBlockKey,
        output: &mut VoteCertificate,
    ) {
        if let Some(vote_entries) = self.votes.get(voted_block_key) {
            output.aggregate(vote_entries.transactions.iter())
        }
    }

    // Get the previous notarization vote, only used for safe to notar to figure out previous notar vote
    pub(crate) fn get_prev_vote(&self, validator_vote_key: &Pubkey) -> Option<VotedBlockKey> {
        self.prev_voted_block_keys
            .get(validator_vote_key)
            .and_then(|vs| vs.first())
            .map(|vk| VotedBlockKey {
                block_id: vk.block_id,
                bank_hash: vk.bank_hash,
            })
    }

    pub fn has_prev_validator_vote_for_block(
        &self,
        validator_vote_key: &Pubkey,
        vote_key: &VotedBlockKey,
    ) -> bool {
        self.prev_voted_block_keys
            .get(validator_vote_key)
            .is_some_and(|vs| vs.contains(vote_key))
    }

    pub fn top_entry_stake(&self) -> Stake {
        self.top_entry_stake
    }
}

impl VotePool for DuplicateBlockVotePool {
    fn total_stake(&self) -> Stake {
        self.total_stake
    }
    fn has_prev_validator_vote(&self, validator_vote_key: &Pubkey) -> bool {
        self.prev_voted_block_keys.contains_key(validator_vote_key)
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        alpenglow_vote::{bls_message::VoteMessage, vote::Vote},
        solana_bls_signatures::Signature as BLSSignature,
    };

    #[test]
    fn test_skip_vote_pool() {
        let mut vote_pool = SimpleVotePool::new();
        let vote = Vote::new_skip_vote(5);
        let transaction = VoteMessage {
            vote,
            signature: BLSSignature::default(),
            rank: 1,
        };
        let my_pubkey = Pubkey::new_unique();

        assert!(vote_pool.add_vote(&my_pubkey, 10, &transaction));
        assert_eq!(vote_pool.total_stake(), 10);

        // Adding the same key again should fail
        assert!(!vote_pool.add_vote(&my_pubkey, 10, &transaction));
        assert_eq!(vote_pool.total_stake(), 10);

        // Adding a different key should succeed
        let new_pubkey = Pubkey::new_unique();
        assert!(vote_pool.add_vote(&new_pubkey, 60, &transaction),);
        assert_eq!(vote_pool.total_stake(), 70);
    }

    #[test]
    fn test_notarization_pool() {
        let mut vote_pool = DuplicateBlockVotePool::new(1);
        let my_pubkey = Pubkey::new_unique();
        let block_id = Hash::new_unique();
        let bank_hash = Hash::new_unique();
        let vote = Vote::new_notarization_vote(3, block_id, bank_hash);
        let transaction = VoteMessage {
            vote,
            signature: BLSSignature::default(),
            rank: 1,
        };
        assert!(vote_pool.add_vote(
            &my_pubkey,
            VotedBlockKey {
                bank_hash,
                block_id,
            },
            &transaction,
            10,
        ));
        assert_eq!(vote_pool.total_stake(), 10);
        assert_eq!(
            vote_pool.total_stake_by_voted_block_key(&VotedBlockKey {
                bank_hash,
                block_id,
            }),
            10
        );

        // Adding the same key again should fail
        assert!(!vote_pool.add_vote(
            &my_pubkey,
            VotedBlockKey {
                bank_hash,
                block_id,
            },
            &transaction,
            10
        ));
        assert_eq!(vote_pool.total_stake(), 10);

        // Adding a different bankhash should fail
        assert!(!vote_pool.add_vote(
            &my_pubkey,
            VotedBlockKey {
                bank_hash: Hash::new_unique(),
                block_id,
            },
            &transaction,
            10
        ));
        assert_eq!(vote_pool.total_stake(), 10);

        // Adding a different key should succeed
        let new_pubkey = Pubkey::new_unique();
        assert!(vote_pool.add_vote(
            &new_pubkey,
            VotedBlockKey {
                bank_hash,
                block_id,
            },
            &transaction,
            60
        ),);
        assert_eq!(vote_pool.total_stake(), 70);
        assert_eq!(
            vote_pool.total_stake_by_voted_block_key(&VotedBlockKey {
                bank_hash,
                block_id,
            }),
            70
        );
    }

    #[test]
    fn test_notarization_fallback_pool() {
        solana_logger::setup();
        let mut vote_pool = DuplicateBlockVotePool::new(3);
        let vote = Vote::new_notarization_fallback_vote(7, Hash::new_unique(), Hash::new_unique());
        let transaction = VoteMessage {
            vote,
            signature: BLSSignature::default(),
            rank: 1,
        };
        let my_pubkey = Pubkey::new_unique();

        let block_ids: Vec<Hash> = (0..4).map(|_| Hash::new_unique()).collect();
        let bank_hashes: Vec<Hash> = (0..4).map(|_| Hash::new_unique()).collect();

        // Adding the first 3 votes should succeed, but total_stake should remain at 10
        for i in 0..3 {
            assert!(vote_pool.add_vote(
                &my_pubkey,
                VotedBlockKey {
                    bank_hash: bank_hashes[i],
                    block_id: block_ids[i],
                },
                &transaction,
                10
            ));
            assert_eq!(vote_pool.total_stake(), 10);
            assert_eq!(
                vote_pool.total_stake_by_voted_block_key(&VotedBlockKey {
                    bank_hash: bank_hashes[i],
                    block_id: block_ids[i],
                }),
                10
            );
        }
        // Adding the 4th vote should fail
        assert!(!vote_pool.add_vote(
            &my_pubkey,
            VotedBlockKey {
                bank_hash: bank_hashes[3],
                block_id: block_ids[3],
            },
            &transaction,
            10
        ));
        assert_eq!(vote_pool.total_stake(), 10);
        assert_eq!(
            vote_pool.total_stake_by_voted_block_key(&VotedBlockKey {
                bank_hash: bank_hashes[3],
                block_id: block_ids[3],
            }),
            0
        );

        // Adding a different key should succeed
        let new_pubkey = Pubkey::new_unique();
        for i in 1..3 {
            assert!(vote_pool.add_vote(
                &new_pubkey,
                VotedBlockKey {
                    bank_hash: bank_hashes[i],
                    block_id: block_ids[i],
                },
                &transaction,
                60
            ));
            assert_eq!(vote_pool.total_stake(), 70);
            assert_eq!(
                vote_pool.total_stake_by_voted_block_key(&VotedBlockKey {
                    bank_hash: bank_hashes[i],
                    block_id: block_ids[i],
                }),
                70
            );
        }

        // The new key only added 2 votes, so adding bank_hashes[3] should succeed
        assert!(vote_pool.add_vote(
            &new_pubkey,
            VotedBlockKey {
                bank_hash: bank_hashes[3],
                block_id: block_ids[3],
            },
            &transaction,
            60
        ));
        assert_eq!(vote_pool.total_stake(), 70);
        assert_eq!(
            vote_pool.total_stake_by_voted_block_key(&VotedBlockKey {
                bank_hash: bank_hashes[3],
                block_id: block_ids[3],
            }),
            60
        );

        // Now if adding the same key again, it should fail
        assert!(!vote_pool.add_vote(
            &new_pubkey,
            VotedBlockKey {
                bank_hash: bank_hashes[0],
                block_id: block_ids[0],
            },
            &transaction,
            60
        ));
    }
}
