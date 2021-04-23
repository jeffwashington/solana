//! `cost_model` aiming to limit the size of broadcasting sets, and reducing the number
//! of un-parallelizeble transactions (eg, transactions as same writable key sets).
//! By doing so to improve leader performance.

use crate::cost_aligned_package::CostAlignedPackage;
use solana_runtime::hashed_transaction::HashedTransaction;
use solana_sdk::pubkey::Pubkey;
use thiserror::Error;

#[derive(
    Error, Serialize, Deserialize, Debug, PartialEq, Eq, Clone, AbiExample, AbiEnumVisitor,
)]
pub enum PackagingOptimizerError {
    /// inconsistent limit setting prevent items being added to package
    #[error("Bad Config")]
    BadConfig,
}

#[derive(Debug)]
pub struct PackagingOptimizer<'a> {
    chain_max_cost: u32,
    package_max_cost: u32,
    packed_packages: Vec<CostAlignedPackage<'a>>,
}

impl<'a> PackagingOptimizer<'a> {
    pub fn new(chain_max: u32, package_max: u32) -> Self {
        Self {
            chain_max_cost: chain_max,
            package_max_cost: package_max,
            packed_packages: vec![],
        }
    }

    pub fn get_packages(&self) -> Result<Vec<Vec<&HashedTransaction>>, PackagingOptimizerError> {
        if self.sanity_check() {
            // TODO - gonna be a better way
            let mut result: Vec<Vec<&HashedTransaction>> = vec![];
            for cost_aligned_package in &self.packed_packages {
                result.push(cost_aligned_package.package().to_vec());
            }
            Ok(result)
        } else {
            Err(PackagingOptimizerError::BadConfig)
        }
    }

    pub fn add_transaction(
        &mut self,
        transaction: &'a HashedTransaction<'a>,
        keys: &[Pubkey],
        cost: &u32,
    ) {
        for cost_aligned_package in &mut self.packed_packages {
            if !&cost_aligned_package.would_exceed_limit(&keys, &cost) {
                cost_aligned_package.add_transaction(&transaction, &keys, &cost);
                return;
            }
        }
        self.add_to_new_package(&transaction, &keys, &cost);
    }

    fn sanity_check(&self) -> bool {
        // TODO - simply check if there is any package, can dig into detail to
        // make sure all added transactions are counted
        self.packed_packages.len() > 0
    }

    fn add_to_new_package(
        &mut self,
        transaction: &'a HashedTransaction<'a>,
        keys: &[Pubkey],
        cost: &u32,
    ) {
        let mut cost_aligned_package =
            CostAlignedPackage::new(self.chain_max_cost, self.package_max_cost);
        cost_aligned_package.add_transaction(&transaction, &keys, &cost);
        self.packed_packages.push(cost_aligned_package);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_ledger::genesis_utils::{create_genesis_config, GenesisConfigInfo};
    use solana_runtime::bank::Bank;
    use solana_sdk::{
        hash::Hash,
        signature::{Keypair, Signer},
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

    fn build_simple_transaction(
        mint_keypair: &Keypair,
        start_hash: &Hash,
    ) -> (HashedTransaction<'static>, Vec<Pubkey>, u32) {
        let keypair = Keypair::new();
        let simple_transaction =
            system_transaction::transfer(&mint_keypair, &keypair.pubkey(), 2, *start_hash);

        (
            HashedTransaction::from(simple_transaction),
            vec![mint_keypair.pubkey()],
            5,
        )
    }

    #[test]
    fn test_packaging_optimizer_initialization() {
        let testee = PackagingOptimizer::new(10, 11);
        assert_eq!(10, testee.chain_max_cost);
        assert_eq!(11, testee.package_max_cost);
        assert_eq!(0, testee.packed_packages.len());
    }

    #[test]
    fn test_packaging_optimizer_one_tx() -> Result<(), PackagingOptimizerError> {
        let (mint_keypair, start_hash) = test_setup();
        let (tx, keys, cost) = build_simple_transaction(&mint_keypair, &start_hash);
        let mut testee = PackagingOptimizer::new(cost, cost);
        testee.add_transaction(&tx, &keys, &cost);
        let result = testee.get_packages()?;
        assert_eq!(1, result.len());
        assert_eq!(1, result[0].len());
        assert_eq!(tx.transaction(), result[0][0].transaction());
        Ok(())
    }

    #[test]
    fn test_packaging_optimizer_2_tx_1_package() -> Result<(), PackagingOptimizerError> {
        let (mint_keypair, start_hash) = test_setup();
        let (tx, keys, cost) = build_simple_transaction(&mint_keypair, &start_hash);
        // expect 2 tx chains in single package
        let mut testee = PackagingOptimizer::new(cost * 2, cost * 2);
        testee.add_transaction(&tx, &keys, &cost);
        testee.add_transaction(&tx, &keys, &cost);
        let result = testee.get_packages()?;
        assert_eq!(1, result.len()); // one package
        assert_eq!(2, result[0].len()); // ... has two transactions
        assert_eq!(tx.transaction(), result[0][0].transaction());
        assert_eq!(tx.transaction(), result[0][1].transaction());
        Ok(())
    }

    #[test]
    fn test_packaging_optimizer_2_tx_2_packages() -> Result<(), PackagingOptimizerError> {
        let (mint_keypair, start_hash) = test_setup();
        let (tx, keys, cost) = build_simple_transaction(&mint_keypair, &start_hash);
        // expect 2 tx in 2 seperate packages
        let mut testee = PackagingOptimizer::new(cost, cost);
        testee.add_transaction(&tx, &keys, &cost);
        testee.add_transaction(&tx, &keys, &cost);
        let result = testee.get_packages()?;
        assert_eq!(2, result.len()); // two package
        assert_eq!(1, result[0].len()); // ... each has one transactions
        assert_eq!(1, result[1].len()); // ...
        assert_eq!(tx.transaction(), result[0][0].transaction());
        assert_eq!(tx.transaction(), result[1][0].transaction());
        Ok(())
    }
}
