//! `cost_model` aiming to limit the size of broadcasting sets, and reducing the number
//! of un-parallelizeble transactions (eg, transactions as same writable key sets).
//! By doing so to improve leader performance.

use crate::{
    cost_aligned_package::CostAlignedPackage,
};
use solana_sdk::{
    bpf_loader, bpf_loader_deprecated, bpf_loader_upgradeable, pubkey::Pubkey, system_program,
    transaction::Transaction,
};
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
    cost_aligned_package: CostAlignedPackage,
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
        Self::new_with_config(CHAIN_MAX_COST, BLOCK_MAX_COST)
    }
        /*
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
            cost_aligned_package: CostAlignedPackage::new(CHAIN_MAX_COST, BLOCK_MAX_COST),
        }
    }
    // */

    pub fn can_fit_this (
        &mut self,
        transaction: &Transaction,
    ) -> Option<()> {
        // NOTE: taking a simplistic approach - chaining transactions by signer-accounts, which
        //       says transactions share same signer accounts can not be parallelized.
        //       Notice each transaction can have more than one signer accounts, in this case,
        //       the cost of this transaction is added to all accounts.
        let signed_keys =
            &transaction.message().account_keys[0..transaction.message().header.num_required_signatures as usize];
        let transaction_cost = self.find_transaction_cost(&transaction);

        if self.cost_aligned_package.would_exceed_limit(&signed_keys, &transaction_cost) {
            debug!("can not fit transaction {:?}", transaction );
            None
        } else {
            debug!("transaction {:?} added to block", transaction );
            self.cost_aligned_package.add_transaction(&signed_keys, &transaction_cost);
            Some(())
        }
    }

    fn new_with_config(chain_max: u32, block_max: u32) -> Self {
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
            cost_aligned_package: CostAlignedPackage::new(chain_max, block_max),
        }
    }


    fn find_instruction_cost(&self, program_key: &Pubkey) -> &Cost {
        match self.cost_metrics.get(&program_key) {
            Some(cost) => cost,
            None => {
                debug!(
                    "Program key {:?} does not have assigned cost, using default {}",
                    program_key, DEFAULT_PROGRAM_COST
                );
                &DEFAULT_PROGRAM_COST
            }
        }
    }

    fn find_transaction_cost(&self, transaction: &Transaction) -> Cost {
        let mut cost: Cost = 0;

        for instruction in &transaction.message().instructions {
            let program_id =
                transaction.message().account_keys[instruction.program_id_index as usize];
            let instruction_cost = self.find_instruction_cost(&program_id);
            debug!(
                "instruction {:?} has cost of {}",
                instruction, instruction_cost
            );
            cost += instruction_cost;
        }
        cost
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
    use std::sync::Arc;

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

    #[test]
    fn test_cost_model_instruction_cost() {
        let testee = CostModel::new();

        // find cost for known programs
        assert_eq!(
            COST_UNIT * 5,
            *testee.find_instruction_cost(
                &Pubkey::from_str("Vote111111111111111111111111111111111111111").unwrap()
            )
        );
        assert_eq!(
            COST_UNIT * 1_000,
            *testee.find_instruction_cost(&bpf_loader::id())
        );

        // unknown program is assigned with default cost
        assert_eq!(
            DEFAULT_PROGRAM_COST,
            *testee.find_instruction_cost(
                &Pubkey::from_str("unknown111111111111111111111111111111111111").unwrap()
            )
        );
    }

    #[test]
    fn test_cost_model_simple_transaction() {
        let (mint_keypair, start_hash) = test_setup();

        let keypair = Keypair::new();
        let simple_transaction =
            system_transaction::transfer(&mint_keypair, &keypair.pubkey(), 2, start_hash);
        debug!(
            "system_transaction simple_transaction {:?}",
            simple_transaction
        );

        // expected cost for one system transfer instructions
        let expected_cost = COST_UNIT * 1;

        let testee = CostModel::new();
        assert_eq!(
            expected_cost,
            testee.find_transaction_cost(&simple_transaction)
        );
    }

    #[test]
    fn test_cost_model_transaction_many_transfer_instructions() {
        let (mint_keypair, start_hash) = test_setup();

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
        assert_eq!(expected_cost, testee.find_transaction_cost(&tx));
    }

    #[test]
    fn test_cost_model_message_many_different_instructions() {
        let (mint_keypair, start_hash) = test_setup();

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
        assert_eq!(expected_cost, testee.find_transaction_cost(&tx));
    }

    #[test]
    fn test_cost_model_can_fit_transaction() {
        let (mint_keypair, start_hash) = test_setup();

        // construct a transaction with a random instructions
        let mut accounts: Vec<Pubkey> = vec![];
        let mut program_ids: Vec<Pubkey> = vec![];
        let mut instructions: Vec<CompiledInstruction> = vec![];

        accounts.push( solana_sdk::pubkey::new_rand() );
        program_ids.push(solana_sdk::pubkey::new_rand());
        instructions.push(CompiledInstruction::new(2, &(), vec![0, 1]));
        let tx = Transaction::new_with_compiled_instructions(
            &[&mint_keypair],
            &accounts[..],
            start_hash,
            program_ids,
            instructions,
        );
        debug!("A random transaction {:?}", tx);

        let mut testee = CostModel::new();
        assert!(testee.can_fit_this(&tx).is_some());
    }

    #[test]
    fn test_cost_model_cannot_fit_transaction_on_chain_limit() {
        let (mint_keypair, start_hash) = test_setup();

        // construct a transaction with two random instructions with same signer
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

        // build model allows three transaction in total, but chain max is 1
        let mut testee = CostModel::new_with_config(DEFAULT_PROGRAM_COST*1, DEFAULT_PROGRAM_COST*3);
        assert!(testee.can_fit_this(&tx).is_none());
    }

    #[test]
    fn test_cost_model_cannot_fit_transaction_on_block_limit() {
        let (_mint_keypair, start_hash) = test_setup();

        // build model allows one transaction in total
        let mut testee = CostModel::new_with_config(DEFAULT_PROGRAM_COST*1, DEFAULT_PROGRAM_COST*1);

        {
            let signer_account = Keypair::new();
            let tx = Transaction::new_with_compiled_instructions(
                &[&signer_account],
                &[solana_sdk::pubkey::new_rand()],
                start_hash,
                vec![solana_sdk::pubkey::new_rand()],
                vec![CompiledInstruction::new(2, &(), vec![0, 1])],
            );
            debug!("Some random transaction {:?}", tx);
            // the first transaction will fit
            assert!(testee.can_fit_this(&tx).is_some());
        }

        {
            let signer_account = Keypair::new();
            let tx = Transaction::new_with_compiled_instructions(
                &[&signer_account],
                &[solana_sdk::pubkey::new_rand()],
                start_hash,
                vec![solana_sdk::pubkey::new_rand()],
                vec![CompiledInstruction::new(2, &(), vec![0, 1])],
            );
            debug!("Some random transaction {:?}", tx);
            // the second transaction will not fit
            assert!(testee.can_fit_this(&tx).is_none());
        }
    }
}
