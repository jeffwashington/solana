//! `cost_model` aiming to limit the size of broadcasting sets, and reducing the number
//! of un-parallelizeble transactions (eg, transactions as same writable key sets).
//! By doing so to improve leader performance.

use crate::cost_tracker::CostTracker;
use solana_sdk::{
    bpf_loader, bpf_loader_deprecated, bpf_loader_upgradeable, pubkey::Pubkey, system_program,
    transaction::Transaction,
};
use solana_runtime::{
    hashed_transaction::HashedTransaction,
}
use std::{collections::HashMap, str::FromStr};

// TODO  revisit these hardcoded numbers, better get from mainnet log
const COST_UNIT: u32 = 1;
const DEFAULT_PROGRAM_COST: u32 = COST_UNIT * 500;
const CHAIN_MAX_COST: u32 = COST_UNIT * 100_000;
const BLOCK_MAX_COST: u32 = COST_UNIT * 100_000_000;

use u32 as Cost;
use Pubkey as ProgramKey;

#[derive(Debug)]
pub struct CostModel {
    cost_metrics: HashMap<ProgramKey, Cost>,
    cost_tracker: CostTracker,
    chain_max_cost: Cost,
    block_max_cost: Cost,
}

macro_rules! costmetrics {
    ($( $key: expr => $val: expr ),*) => {{
        let mut hashmap: HashMap< Pubkey, u32 > = HashMap::new();
        $( hashmap.insert( $key, $val); )*
        hashmap
    }}
}

impl CostModel {
    pub fn new() -> Self {
        // NOTE: message.rs has following lazy_static program ids. Can probably use them to define
        // `cost` for each type.
        // NOTE: since each instruction has `compute budget`, possible to derive `cost` from `budget`?
        let parse = |s| Pubkey::from_str(s).unwrap();
        Self {
            cost_metrics: costmetrics![
                parse("Config1111111111111111111111111111111111111") => COST_UNIT * 1,
                parse("Feature111111111111111111111111111111111111") => COST_UNIT * 1,
                parse("NativeLoader1111111111111111111111111111111") => COST_UNIT * 1,
                parse("Stake11111111111111111111111111111111111111") => COST_UNIT * 1,
                parse("StakeConfig11111111111111111111111111111111") => COST_UNIT * 1,
                parse("Vote111111111111111111111111111111111111111") => COST_UNIT * 5,
                system_program::id()                                 => COST_UNIT * 1,
                bpf_loader::id()                                     => COST_UNIT * 1_000,
                bpf_loader_deprecated::id()                          => COST_UNIT * 1_000,
                bpf_loader_upgradeable::id()                         => COST_UNIT * 1_000
            ],
            chain_max_cost: CHAIN_MAX_COST,
            block_max_cost: BLOCK_MAX_COST,
        }
    }

    pub fn find_instruction_cost(
        &self,
        program_key: &Pubkey
    ) -> &Cost {
        match self.cost_metrics.get( &program_key ) {
            Some(cost) => { cost }
            None => {
                debug!("Program key {:?} does not have assigned cost, using default {}", program_key, DEFAULT_PROGRAM_COST ); 
                &DEFAULT_PROGRAM_COST
            }
        }
    }

    pub fn find_transaction_cost(
        &self,
        transaction: &HashedTransaction
    ) -> &Cost {
        // TODO - iter through instructions in `transaction`, return sum( cost(instruction) )
        //self.find_instruction_cost( &system_program::id() )
        &DEFAULT_PROGRAM_COST
    }

    // NOTE - main function that breaks a collection of transactions into several smaller
    //        collection of transaction, each complies the cost limits (on both Chain_cost and
    //        block_cost). 
    pub fn pack_transactions_by_cost (
        transactions: &[HashedTransaction],
    ) -> Vec<Vec<HashedTransaction<'static>>> {
        let packages: Vec<Vec<HashedTransaction<'static>>> = vec![];
        for tx in transactions {
            /* TODO - pseudo code of breaking transactions into smaller collection that complies
            //        cost model
            let transaction_key = get_writeble_account( tx );
            let transaction_cost = find_transaction_cost( tx );
            let transaction_added: bool = false;
            for &package in packages {
                if can_accept_transaction( package, transaction_key, transaction_cost ) {
                    add_transaction_to_package( tx, package );
                    transaction_added = true;
                    break;
                }
            }
            if !transaction_add {
                let new_package: Vec<HashedTransaction<'static>> = vec![];
                add_transaction_to_package( tx, new_package );
                packages.push( new_package );
            }
            // */
        }
        packages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_model_initialization() {
        let testee = CostModel::new();
        assert_eq!( CHAIN_MAX_COST, testee.chain_max_cost );
        assert_eq!( BLOCK_MAX_COST, testee.block_max_cost );
    }

    #[test]
    fn test_cost_model_instruction_cost() {
        let testee = CostModel::new();

        // find cost for known programs
        assert_eq!( COST_UNIT * 5, *testee.find_instruction_cost( & Pubkey::from_str("Vote111111111111111111111111111111111111111").unwrap() ) );
        assert_eq!( COST_UNIT * 1_000, *testee.find_instruction_cost( &bpf_loader::id() ) );
        
        // unknown program is assigned with default cost
        assert_eq!( DEFAULT_PROGRAM_COST, *testee.find_instruction_cost( & Pubkey::from_str("unknown111111111111111111111111111111111111").unwrap() ) );
    }
}
