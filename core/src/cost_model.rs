//! `cost_model` aiming to limit the size of broadcasting sets, and reducing the number
//! of un-parallelizeble transactions (eg, transactions as same writable key sets).
//! By doing so to improve leader performance.

use solana_sdk::{
    bpf_loader, bpf_loader_deprecated, bpf_loader_upgradeable, system_program,
    pubkey::Pubkey, 
    transaction::Transaction,
};
use solana_runtime::{
    hashed_transaction::HashedTransaction,
};
use std::{collections::HashMap, str::FromStr};

// TODO  revisit these hardcoded numbers, better get from mainnet log
const COST_UNIT: u32 = 1;
const DEFAULT_PROGRAM_COST: u32 = COST_UNIT * 500;
const CHAIN_MAX_COST: u32 = COST_UNIT * 100_000;
const BLOCK_MAX_COST: u32 = COST_UNIT * 100_000_000;

use Pubkey as ProgramKey;
use u32 as Cost;

#[derive(Debug)]
pub struct CostModel {
    cost_metrics: HashMap<ProgramKey, Cost>,
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

    fn find_instruction_cost(
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

    fn find_transaction_cost(
        &self,
        transaction: &Transaction
    ) -> Cost {
        let mut cost: Cost = 0;

        for instruction in &transaction.message().instructions {
            let program_id = transaction.message().account_keys[instruction.program_id_index as usize];
            let instruction_cost =  self.find_instruction_cost( &program_id );
            debug!("instruction {:?} has cost of {}", instruction, instruction_cost);
            cost += instruction_cost;
        }
        cost
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::{
        signature::{Keypair, Signer},
        message::{Message},
        hash::{Hash},
        system_transaction,
        system_instruction::{self},
        instruction::{CompiledInstruction},
    };
    use solana_runtime::{
        bank::{ Bank },
    };
    use solana_ledger::{
        genesis_utils::{create_genesis_config, GenesisConfigInfo},
    };
    use std::{
        sync::{Arc},
    };

    fn test_setup() -> (
        Keypair,
        Hash,
    ) {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10);
        let bank = Arc::new(Bank::new_no_wallclock_throttle(&genesis_config));
        let start_hash = bank.last_blockhash();
        (
            mint_keypair,
            start_hash,
        )
    }

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

    #[test]
    fn test_cost_model_simple_transaction() {
        let (mint_keypair, start_hash ) = test_setup();

        let keypair = Keypair::new();
        let simple_transaction =
                system_transaction::transfer(&mint_keypair, &keypair.pubkey(), 2, start_hash);
        debug!("system_transaction simple_transaction {:?}", simple_transaction);

        // expected cost for one system transfer instructions
        let expected_cost = COST_UNIT * 1; 

        let testee = CostModel::new();
        assert_eq!( expected_cost, testee.find_transaction_cost( &simple_transaction ) );
    }

    #[test]
    fn test_cost_model_transaction_many_transfer_instructions() {
        let (mint_keypair, start_hash ) = test_setup();

        let key1 = solana_sdk::pubkey::new_rand();
        let key2 = solana_sdk::pubkey::new_rand();
        let instructions =
            system_instruction::transfer_many(&mint_keypair.pubkey(), &[(key1, 1), (key2, 1)]);
        let message = Message::new(&instructions, Some(&mint_keypair.pubkey()));
        let tx = Transaction::new(&[&mint_keypair], message, start_hash);
        debug!("many transfer transaction {:?}", tx);

        // expected cost for two system transfer instructions
        let expected_cost = COST_UNIT * 2; 

        let testee = CostModel::new();
        assert_eq!( expected_cost, testee.find_transaction_cost( &tx) );
    }

    #[test]
    fn test_cost_model_message_many_different_instructions() {
        let (mint_keypair, start_hash ) = test_setup();

        // construct a transaction with multiple random instructions
        let key1 = solana_sdk::pubkey::new_rand();
        let key2 = solana_sdk::pubkey::new_rand();
        let prog1 = solana_sdk::pubkey::new_rand();
        let prog2 = solana_sdk::pubkey::new_rand();
        let instructions = vec![
            CompiledInstruction::new(3, &(), vec![0, 1]),
            CompiledInstruction::new(4, &(), vec![0, 2]),
        ];
        let tx = Transaction::new_with_compiled_instructions(
            &[&mint_keypair],
            &[key1, key2],
            start_hash,
            vec![prog1, prog2],
            instructions,
        );
        debug!("many random transaction {:?}", tx);

        // expected cost for two random/unknown program is
        let expected_cost = DEFAULT_PROGRAM_COST * 2; 

        let testee = CostModel::new();
        assert_eq!( expected_cost, testee.find_transaction_cost( &tx) );
    }

}

