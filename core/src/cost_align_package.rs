//! `cost_aligned_package` is a collection of *ref* of  HashedTransactions that complies to
//! the chain_max_cost and block_max_cost. 
//! It does not own or copy `banking_stage` owned HashedTransactions. 
use solana_runtime::hashed_transaction::HashedTransaction;
use solana_sdk::{pubkey::Pubkey};
use std::{collections::HashMap};

#[derive(Debug)]
pub struct CostAlignedPackage<'a> {
    chain_max_cost: u32,
    package_max_cost: u32,
    // NOTE - holding a reference of transactions that are owned by banking_stage (where it created
    // from `msg`, and continiously using in 'retry'
    package: Vec<&'a HashedTransaction<'a>>,
    chained_costs: HashMap<Pubkey, u32>,
    package_cost: u32,
}

impl<'a> CostAlignedPackage<'a> {
    pub fn new(chain_max: u32, package_max: u32) -> Self {
        Self {
            chain_max_cost: chain_max,
            package_max_cost: package_max,
            package: vec![],
            chained_costs: HashMap::new(),
            package_cost: 0,
        }
    }

    pub fn would_exceed_limit(&self, keys: &[Pubkey], cost: &u32) -> bool {
        // check against the total package cost
        if self.package_cost + cost > self.package_max_cost {
            return true;
        }

        // check each account against chain_max_cost, 
        for account_key in keys.iter() {
            match self.chained_costs.get(&account_key) {
                Some(chained_cost) => {
                    if chained_cost + cost > self.chain_max_cost {
                        return true;
                    } else {
                        continue;
                    }
                }
                None => continue,
            }
        }

        return false;
    }

    // NOTE - somewhere we need to have a way to prevent the case that a single transaction
    //        exceeds limit(s); for now, we will always allow first transaction, regardless cost,
    //        be successfully inserted into a new CostAlignedPackage
    pub fn add_transaction(&mut self, transaction: &'a HashedTransaction<'a>, keys: &[Pubkey], cost: &u32) {
        // NOTE - making copy here :( what is the better way? Or maybe copying-once here is
        // unavailable?
        self.package.push(transaction);
        for account_key in keys.iter() {
            *self.chained_costs.entry(*account_key).or_insert(0) += cost;
        }
        self.package_cost += cost;
    }

    pub fn package( &self ) -> &Vec<&HashedTransaction> {
        &self.package
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_ledger::genesis_utils::{create_genesis_config, GenesisConfigInfo};
    use solana_runtime::bank::Bank;
    use solana_sdk::{
        hash::Hash,
        instruction::CompiledInstruction,
        message::Message,
        signature::{Keypair, Signer},
        system_instruction::{self},
        system_transaction,
    };
    use std::{
        sync::Arc,
        cmp,
    };

    fn test_setup() -> (Keypair, Hash) {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10);
        let bank = Arc::new(Bank::new_no_wallclock_throttle(&genesis_config));
        let start_hash = bank.last_blockhash();
        (mint_keypair, start_hash)
    }

    fn build_simple_transaction(mint_keypair: &Keypair, start_hash: &Hash 
        ) -> (HashedTransaction<'static>, Vec<Pubkey>, u32) {
        let keypair = Keypair::new();
        let simple_transaction =
            system_transaction::transfer(&mint_keypair, &keypair.pubkey(), 2, *start_hash);

        ( HashedTransaction::from(simple_transaction), 
          vec![mint_keypair.pubkey()],
          5,
        ) 
    }

    #[test]
    fn test_cost_aligned_package_initialization() {
        let testee = CostAlignedPackage::new(10, 11);
        assert_eq!(10, testee.chain_max_cost);
        assert_eq!(11, testee.package_max_cost);
        assert_eq!(0, testee.package.len());
        assert_eq!(0, testee.chained_costs.len());
        assert_eq!(0, testee.package_cost);
    }

    #[test]
    fn test_cost_aligned_package_ok_add_one() {
        let (mint_keypair, start_hash ) = test_setup();
        let (tx, keys, cost) = build_simple_transaction(&mint_keypair, &start_hash);

        // build testee to have capacity for one simple transaction
        let mut testee = CostAlignedPackage::new( cost, cost );
        assert_eq!( false, testee.would_exceed_limit( &keys, &cost ) );
        testee.add_transaction( &tx, &keys, &cost );
        assert_eq!( 1, testee.package().len() );
    }

    #[test]
    fn test_cost_aligned_package_ok_add_two_same_accounts() {
        let (mint_keypair, start_hash ) = test_setup();
        // build two transactions with same signed account
        let (tx1, keys1, cost1) = build_simple_transaction(&mint_keypair, &start_hash);
        let (tx2, keys2, cost2) = build_simple_transaction(&mint_keypair, &start_hash);

        // build testee to have capacity for two simple transactions, with same accounts
        let mut testee = CostAlignedPackage::new( cost1 + cost2, cost1 + cost2 );
        {
            assert_eq!( false, testee.would_exceed_limit( &keys1, &cost1 ) );
            testee.add_transaction( &tx1, &keys1, &cost1 );
        }
        {
            assert_eq!( false, testee.would_exceed_limit( &keys2, &cost2 ) );
            testee.add_transaction( &tx2, &keys2, &cost2 );
        }
        assert_eq!( 2, testee.package().len() );
    }

    #[test]
    fn test_cost_aligned_package_ok_add_two_diff_accounts() {
        let (mint_keypair, start_hash ) = test_setup();
        // build two transactions with diff accounts
        let (tx1, keys1, cost1) = build_simple_transaction(&mint_keypair, &start_hash);
        let second_account = Keypair::new();
        let (tx2, keys2, cost2) = build_simple_transaction(&second_account, &start_hash);

        // build testee to have capacity for two simple transactions, with same accounts
        let mut testee = CostAlignedPackage::new( cmp::max(cost1, cost2), cost1 + cost2 );
        {
            assert_eq!( false, testee.would_exceed_limit( &keys1, &cost1 ) );
            testee.add_transaction( &tx1, &keys1, &cost1 );
        }
        {
            assert_eq!( false, testee.would_exceed_limit( &keys2, &cost2 ) );
            testee.add_transaction( &tx2, &keys2, &cost2 );
        }
        assert_eq!( 2, testee.package().len() );
    }


    #[test]
    fn test_cost_aligned_package_chain_reach_limit() {
        let (mint_keypair, start_hash ) = test_setup();
        // build two transactions with same signed account
        let (tx1, keys1, cost1) = build_simple_transaction(&mint_keypair, &start_hash);
        let (tx2, keys2, cost2) = build_simple_transaction(&mint_keypair, &start_hash);

        // build testee to have capacity for two simple transactions, but not for same accounts
        let mut testee = CostAlignedPackage::new( cmp::min(cost1, cost2), cost1 + cost2 );
        // should have room for first transaction 
        {
            assert_eq!( false, testee.would_exceed_limit( &keys1, &cost1 ) );
            testee.add_transaction( &tx1, &keys1, &cost1 );
        }
        // but no more sapce on the same chain (same signer account)
        {
            assert_eq!( true, testee.would_exceed_limit( &keys2, &cost2 ) );
        }
    }

    #[test]
    fn test_cost_aligned_package_reach_limit() {
        let (mint_keypair, start_hash ) = test_setup();
        // build two transactions with diff accounts
        let (tx1, keys1, cost1) = build_simple_transaction(&mint_keypair, &start_hash);
        let second_account = Keypair::new();
        let (tx2, keys2, cost2) = build_simple_transaction(&second_account, &start_hash);

        // build testee to have capacity for each chain, but not enough room for both transactions
        let mut testee = CostAlignedPackage::new( cmp::max(cost1, cost2), cost1 + cost2 - 1 );
        // should have room for first transaction
        {
            assert_eq!( false, testee.would_exceed_limit( &keys1, &cost1 ) );
            testee.add_transaction( &tx1, &keys1, &cost1 );
        }
        // but no more room for package as whole
        {
            assert_eq!( true, testee.would_exceed_limit( &keys2, &cost2 ) );
        }
    }
}
