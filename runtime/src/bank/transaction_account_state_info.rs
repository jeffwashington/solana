use {
    crate::{
        account_rent_state::{check_rent_state, RentState},
        bank::Bank,
    },
    solana_sdk::{
        pubkey::Pubkey,
        account::ReadableAccount, feature_set, message::SanitizedMessage, native_loader,
        transaction::Result, transaction_context::TransactionContext,
    },
};

pub(crate) struct TransactionAccountStateInfo {
    rent_state: Option<RentState>, // None: readonly account
}

impl Bank {
    pub(crate) fn get_transaction_account_state_info(
        &self,
        transaction_context: &TransactionContext,
        message: &SanitizedMessage,
    ) -> Vec<TransactionAccountStateInfo> {
        (0..message.account_keys().len())
            .map(|i| {
                let rent_state = if message.is_writable(i) {
                    let state = if let Ok(account) = transaction_context.get_account_at_index(i) {
                        let account = account.borrow();

                        // Native programs appear to be RentPaying because they carry low lamport
                        // balances; however they will never be loaded as writable
                        debug_assert!(!native_loader::check_id(account.owner()));

                    let fa = RentState::from_account(
                        &account,
                        &self.rent_collector().rent,
                    );
                    use log::*;
                    use std::str::FromStr;
                    let key =  transaction_context.get_key_of_account_at_index(i);
                    let mut interesting = key
                    == &Pubkey::from_str("3CKKAoVi94EnfX8QcVxEmk8CAvZTc6nAYzXp1WkSUofX")
                        .unwrap();
                                                        if interesting {
                        error!("get_transaction_account_state_info: {}, {:?}, {:?}", key, account, fa);
                    }
    
                    Some(fa)
                } else {
                    None
                };
                TransactionAccountStateInfo { rent_state }
            })
            .collect()
    }

    pub(crate) fn verify_transaction_account_state_changes(
        &self,
        pre_state_infos: &[TransactionAccountStateInfo],
        post_state_infos: &[TransactionAccountStateInfo],
        transaction_context: &TransactionContext,
    ) -> Result<()> {
        let require_rent_exempt_accounts = self
            .feature_set
            .is_active(&feature_set::require_rent_exempt_accounts::id());
        for (i, (pre_state_info, post_state_info)) in
            pre_state_infos.iter().zip(post_state_infos).enumerate()
        {
            if let Err(err) = check_rent_state(
                pre_state_info.rent_state.as_ref(),
                post_state_info.rent_state.as_ref(),
                transaction_context,
                i,
            ) {
                // Feature gate only wraps the actual error return so that the metrics and debug
                // logging generated by `check_rent_state()` can be examined before feature
                // activation
                if require_rent_exempt_accounts {
                    return Err(err);
                }
            }
        }
        Ok(())
    }
}
