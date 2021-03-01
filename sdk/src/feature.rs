use crate::account::Account;
use crate::account::AccountNoData;
pub use solana_program::feature::*;
use std::sync::Arc;

pub fn from_account(account: &Account) -> Option<Feature> {
    if account.owner != id() {
        None
    } else {
        bincode::deserialize(&account.data).ok()
    }
}

pub fn from_account_no_data(account: &AccountNoData) -> Option<Feature> {
    if account.owner != id() {
        None
    } else {
        bincode::deserialize(&account.data).ok()
    }
}

pub fn to_account(feature: &Feature, account: &mut Account) -> Option<()> {
    bincode::serialize_into(&mut account.data[..], feature).ok()
}

pub fn to_account_no_data(feature: &Feature, account: &mut AccountNoData) -> Option<()> {
    bincode::serialize_into(&mut Arc::make_mut(&mut account.data)[..], feature).ok()
}

pub fn create_account(feature: &Feature, lamports: u64) -> Account {
    let data_len = Feature::size_of().max(bincode::serialized_size(feature).unwrap() as usize);
    let mut account = Account::new(lamports, data_len, &id());
    to_account(feature, &mut account).unwrap();
    account
}

pub fn create_account_no_data(feature: &Feature, lamports: u64) -> AccountNoData {
    let data_len = Feature::size_of().max(bincode::serialized_size(feature).unwrap() as usize);
    let mut account = Account::new(lamports, data_len, &id());
    to_account(feature, &mut account).unwrap();
    Account::to_account_no_data(account)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn feature_deserialize_none() {
        let just_initialized = Account::new(42, Feature::size_of(), &id());
        assert_eq!(
            from_account(&just_initialized),
            Some(Feature { activated_at: None })
        );
    }
}
