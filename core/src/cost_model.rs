//! `cost_model` aiming to limit the size of broadcasting sets, and reducing the number
//! of un-parallelizeble transactions (eg, transactions as same writable key sets).
//! By doing so to improve leader performance.

use solana_runtime::hashed_transaction::HashedTransaction;
use solana_sdk::{
    bpf_loader, bpf_loader_deprecated, bpf_loader_upgradeable, pubkey::Pubkey, system_program,
    transaction::Transaction,
};
use std::{collections::HashMap, str::FromStr};
use thiserror::Error;

// TODO  revisit these hardcoded numbers, better get from mainnet log
const COST_UNIT: u32 = 1;
const DEFAULT_PROGRAM_COST: u32 = COST_UNIT * 500;
const CHAIN_MAX_COST: u32 = COST_UNIT * 100_000;
const BLOCK_MAX_COST: u32 = COST_UNIT * 100_000_000;

use u32 as Cost;
use Pubkey as ProgramKey;

/*
// TODO - move this to separate file (crate?)
#[derive(
    Error, Serialize, Deserialize, Debug, PartialEq, Eq, Clone, AbiExample, AbiEnumVisitor,
)]
pub enum PackageOptimizerError {
    /// inconsistent limit setting prevent items being added to package
    #[error("Bad Config")]
    BadConfig,
}

#[derive(Debug)]
pub struct PackageOptimizer {
    chain_max_cost: u32,
    package_max_cost: u32,
    packed_packages: Vec<CostAlignedPackage>,
}

impl PackageOptimizer {
    pub fn new(chain_max: u32, package_max: u32) -> Self {
        Self {
            chain_max_cost: chain_max,
            package_max_cost: package_max,
            packed_packages: vec![],
        }
    }

    // TODO rust-lang return copy of ref , with lifetime?
    pub fn get_packages(
        &self,
    ) -> Result<&Vec<Vec<HashedTransaction<'static>>>, PackageOptimizerError> {
        if self.sanity_check() {
            Ok(&self.packed_packages)
        } else {
            Err(PackageOptimizerError::BadConfig)
        }
    }

    pub fn add_transaction(&self, transaction: &HashedTransaction, keys: &[Pubkey], cost: &u32) {
        for &mut package in packed_packages {
            if self.try_to_add(&package, transaction, keys, cost) {
                return;
            }
        }
        self.add_to_new_package(transaction, eys, cost);
    }

    fn sanity_check(&self) -> bool {
        // TODO - simply check if there is any package, can dig into detail to
        // make sure all added transactions are counted
        self.packed_packages.len() > 0
    }

    fn try_to_add(
        &self,
        package: &mut CostAlignedPackage,
        transaction: &HashedTransaction,
        keys: &[Pubkey],
        cost: &u32,
    ) -> bool {
        if package.would_exceed_limit(keys, cost) {
            return false;
        }
        package.add_transaction(transaction, keys, cost);
        return true;
    }

    fn add_to_new_package(&self, transaction: &HashedTransaction, keys: &[Pubkey], cost: &u32) {
        let cost_aligned_package =
            CostAlgnedPackage::new(self.chain_max_cost, self.package_max_cost);
        cost_aligned_package.add_transaction(transaction, keys, cost);
        self.packed_packages.insert(cost_aligned_package);
    }
}

// */

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
    pub fn pack_transactions_by_cost(
        &self,
        transactions: &[HashedTransaction],
    ) -> Vec<Vec<HashedTransaction<'static>>> {
        let packages: Vec<Vec<HashedTransaction<'static>>> = vec![];
        packages
        /*
                // TODO - should it panic is the given parameters are invalid (say chain_max > block_max)
                let package_optimizer = PackageOptimizer::new(self.chain_max_cost, self.block_max_cost);

                for hashed_transaction in transactions {
                    let tx = hashed_transaction.transaction();
                    // NOTE: taking a simplistic approach - chaining transactions by signer-accounts, which
                    //       says transactions share same signer accounts can not be parallelized.
                    //       Notice each transaction can have more than one signer accounts, in this case,
                    //       the cost of this transaction is added to all accounts.
                    let signed_keys =
                        &tx.message().account_keys[0..tx.message().header.num_required_signatures as usize];
                    let transaction_cost = self.find_transaction_cost(&tx);
                    package_optimizer.add_transaction(&hashed_transaction, &signed_keys, &transaction_cost);
                }
                match package_optimizer.get_packages() {
                    Ok(packages) => packages,
                    Err(why) => {
                        debug!("Failed to packing transaction by cost, reason {}; returning the original inputs", why);
                        vec![transactions.to_vec()] as Vec<Vec<HashedTransaction<'static>>>
                    }
                }
        // */
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
    fn test_cost_model_initialization() {
        let testee = CostModel::new();
        assert_eq!(CHAIN_MAX_COST, testee.chain_max_cost);
        assert_eq!(BLOCK_MAX_COST, testee.block_max_cost);
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
}
