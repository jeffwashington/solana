use serde::de::{Deserialize, Deserializer};
use crate::{clock::Epoch, pubkey::Pubkey};
use solana_program::{account_info::AccountInfo, sysvar::Sysvar};
use std::{borrow::Cow, cell::RefCell, cmp, fmt, rc::Rc, sync::Arc};

/// An Account with data that is stored on chain
#[repr(C)]
#[frozen_abi(digest = "AXJTWWXfp49rHb34ayFzFLSEuaRbMUsVPNzBDyP3UPjc")]
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Default, AbiExample)]
#[serde(rename_all = "camelCase")]
pub struct Account {
    /// lamports in the account
    pub lamports: u64,
    /// data held in this account
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    /// the program that owns this account. If executable, the program that loads this account.
    pub owner: Pubkey,
    /// this account's data contains a loaded program (and is now read-only)
    pub executable: bool,
    /// the epoch at which this account will next owe rent
    pub rent_epoch: Epoch,
}

#[derive(Clone, Default, Debug, PartialEq, Eq, AbiExample)]
pub struct AccountNoData {
    /// lamports in the account
    pub lamports: u64,
    /// data held in this account
    pub data: Arc<Vec<u8>>,
    /// the program that owns this account. If executable, the program that loads this account.
    pub owner: Pubkey,
    /// this account's data contains a loaded program (and is now read-only)
    pub executable: bool,
    /// the epoch at which this account will next owe rent
    pub rent_epoch: Epoch,
    pub from_cache: bool,
}

impl std::cmp::PartialEq<Account> for AccountNoData {
    fn eq(&self, other: &Account) -> bool
    {
        self.lamports == other.lamports &&
        other.data == *self.data() &&
        self.owner == other.owner &&
        self.executable == other.executable &&
        self.rent_epoch == other.rent_epoch
    }
  }

  impl std::cmp::PartialEq<AccountNoData> for Account {
    fn eq(&self, other: &AccountNoData) -> bool
    {
        self.lamports == other.lamports &&
        self.data == *other.data() && 
        self.owner == other.owner &&
        self.executable == other.executable &&
        self.rent_epoch == other.rent_epoch
    }
  }

impl serde::Serialize for AccountNoData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let d = AccountNoData::to_account(self.clone());
        d.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AccountNoData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let account = Account::deserialize(deserializer)?;
        Ok(Account::to_account_no_data(account))
    }
}

impl AccountNoData {
    pub fn to_account(mut account: AccountNoData) -> Account {
        let mut result = Account {
            lamports: account.lamports,
            data: vec![],
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
        };
        let d = Arc::make_mut(&mut account.data);
        std::mem::swap(d, &mut result.data);
        result
    }

    pub fn new_data<T: serde::Serialize>(
        lamports: u64,
        state: &T,
        owner: &Pubkey,
    ) -> Result<Self, bincode::Error> {
        let data = bincode::serialize(state)?;
        Ok(Self {
            lamports,
            data: Arc::new(data),
            owner: *owner,
            ..Self::default()
        })
    }

    pub fn to_accounts(accounts: Vec<AccountNoData>) -> Vec<Account> {
        accounts.into_iter().map(|account| {
            AccountNoData::to_account(account)
        }).collect::<Vec<_>>()
    }

    pub fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, bincode::Error> {
        bincode::deserialize(&self.data)
    }

    pub fn serialize_data<T: serde::Serialize>(&mut self, state: &T) -> Result<(), bincode::Error> {
        if bincode::serialized_size(state)? > self.data.len() as u64 {
            return Err(Box::new(bincode::ErrorKind::SizeLimit));
        }
        bincode::serialize_into(&mut Arc::make_mut(&mut self.data)[..], state)
    }

}

impl Account {
    pub fn to_account_no_data(account: Account) -> AccountNoData {
        let result = AccountNoData {
            lamports: account.lamports,
            data: Arc::new(account.data),
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            from_cache: false,
        };
        result
    }
    pub fn to_account_no_data2(account: &mut Account) -> AccountNoData {
        let mut data = vec![];
        std::mem::swap(&mut data, &mut account.data);
        let result = AccountNoData {
            lamports: account.lamports,
            data: Arc::new(data),
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            from_cache: false,
        };
        result
    }
}

pub trait AnAccountConcrete : Default + Clone + AnAccount {}

pub trait AnAccount: /*Default + Clone +*/ Sized {
    fn lamports(&self) -> u64;
    fn set_lamports(&mut self, lamports: u64);
    fn data(&self) -> &Vec<u8>;
    fn set_data(&mut self, data: Vec<u8>);
    fn owner(&self) -> &Pubkey;
    fn set_owner(&mut self, owner: Pubkey);
    fn executable(&self) -> bool;
    fn rent_epoch(&self) -> Epoch;
    fn set_rent_epoch(&mut self, epoch: Epoch);
    //fn clone(&self) -> AnAccount;
    fn clone_as_account_no_data(&self) -> AccountNoData;
    fn clone_as_account(&self) -> Account;
    fn from_account_no_data(item: AccountNoData) -> Self;
    fn from_cache(&self) -> bool;
    fn to_account_no_data(&mut self) -> AccountNoData;
}

impl AnAccountConcrete for Account {}
impl AnAccountConcrete for AccountNoData {}

impl AnAccount for Account {
    fn lamports(&self) -> u64 {self.lamports}
    fn set_lamports(&mut self, lamports: u64) {self.lamports = lamports;}
    fn data(&self) -> &Vec<u8> {&self.data}
    fn set_data(&mut self, data: Vec<u8>) {self.data = data;}
    fn owner(&self) -> &Pubkey {&self.owner}
    fn set_owner(&mut self, owner: Pubkey) {self.owner = owner;}
    fn executable(&self) -> bool {self.executable}
    fn rent_epoch(&self) -> Epoch {self.rent_epoch}
    fn set_rent_epoch(&mut self, epoch: Epoch) {self.rent_epoch = epoch;}
    //fn clone(&self) -> AnAccount {self.clone()}
    fn clone_as_account_no_data(&self) -> AccountNoData {Account::to_account_no_data2(&mut self.clone())}
    fn clone_as_account(&self) -> Account {self.clone()}
    fn from_account_no_data(item: AccountNoData) -> Self {AccountNoData::to_account(item)}
    fn from_cache(&self) -> bool {false}
    fn to_account_no_data(&mut self) -> AccountNoData {panic!("unexpectedsdf");}
}

impl<'a> AnAccount for &'a mut Account {
    fn lamports(&self) -> u64 {self.lamports}
    fn set_lamports(&mut self, lamports: u64) {self.lamports = lamports;}
    fn data(&self) -> &Vec<u8> {&self.data}
    fn set_data(&mut self, data: Vec<u8>) {self.data = data;}
    fn owner(&self) -> &Pubkey {&self.owner}
    fn set_owner(&mut self, owner: Pubkey) {self.owner = owner;}
    fn executable(&self) -> bool {self.executable}
    fn rent_epoch(&self) -> Epoch {self.rent_epoch}
    fn set_rent_epoch(&mut self, epoch: Epoch) {self.rent_epoch = epoch;}
    //fn clone(&self) -> AnAccount {self.clone()}
    fn clone_as_account_no_data(&self) -> AccountNoData {
        let result = AccountNoData {
            lamports: self.lamports,
            data: Arc::new(self.data().clone()),
            owner: self.owner,
            executable: self.executable,
            rent_epoch: self.rent_epoch,
            from_cache: false,
        };
        result
    }
    fn clone_as_account(&self) -> Account {
        let account = self;
        let result = Account {
            lamports: account.lamports,
            data: self.data().clone(),
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
        };
        result
    }
    fn from_account_no_data(_item: AccountNoData) -> Self {
        panic!("");
        /*
        let account = item;
        let mut result = Account {
            lamports: account.lamports,
            data: *account.data(),
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
        };
        &mut result
        */
    }
    fn from_cache(&self) -> bool {false}
    fn to_account_no_data(&mut self) -> AccountNoData {panic!("unexpectedsdf");}
}

impl AnAccount for AccountNoData {
    fn lamports(&self) -> u64 {self.lamports}
    fn set_lamports(&mut self, lamports: u64) {self.lamports = lamports;}
    fn data(&self) -> &Vec<u8> {&self.data}
    fn set_data(&mut self, data: Vec<u8>) {self.data = Arc::new(data);}
    fn owner(&self) -> &Pubkey {&self.owner}
    fn set_owner(&mut self, owner: Pubkey) {self.owner = owner;}
    fn executable(&self) -> bool {self.executable}
    fn rent_epoch(&self) -> Epoch {self.rent_epoch}
    fn set_rent_epoch(&mut self, epoch: Epoch) {self.rent_epoch = epoch;}
    //fn clone(&self) -> AnAccount {self.clone()}
    fn clone_as_account_no_data(&self) -> AccountNoData {self.clone()}
    fn clone_as_account(&self) -> Account {AccountNoData::to_account(self.clone())}
    fn from_account_no_data(item: AccountNoData) -> Self {item}
    fn from_cache(&self) -> bool {self.from_cache}
    fn to_account_no_data(&mut self) -> AccountNoData {
        self.clone()
    }
}

// same as account, but with data as Cow
#[derive(PartialEq, Eq, Clone, Default)]
pub struct AccountWithCowData<'a> {
    /// lamports in the account
    pub lamports: u64,
    /// data held in this account
    pub data: Cow<'a, Vec<u8>>,
    /// the program that owns this account. If executable, the program that loads this account.
    pub owner: Pubkey,
    /// this account's data contains a loaded program (and is now read-only)
    pub executable: bool,
    /// the epoch at which this account will next owe rent
    pub rent_epoch: Epoch,
}

pub struct AccountCow<'a> {
    pub account: Cow<'a, Account>,
}

impl AccountCow<'_> {
    pub fn new(account: &Account) -> Self {
        Self {
            account: Cow::Owned(account.clone())
            //account: Cow::new(^account.clone())
        }
    }
}

impl fmt::Debug for Account {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let data_len = cmp::min(64, self.data.len());
        let data_str = if data_len > 0 {
            format!(" data: {}", hex::encode(self.data[..data_len].to_vec()))
        } else {
            "".to_string()
        };
        write!(
            f,
            "Account {{ lamports: {} data.len: {} owner: {} executable: {} rent_epoch: {}{} }}",
            self.lamports,
            self.data.len(),
            self.owner,
            self.executable,
            self.rent_epoch,
            data_str,
        )
    }
}

impl AccountNoData {
    pub fn new(lamports: u64, space: usize, owner: &Pubkey) -> Self {
        Self {
            lamports,
            data: Arc::new(vec![0u8; space]),
            owner: *owner,
            ..Self::default()
        }
    }
}

impl Account {
    pub fn new(lamports: u64, space: usize, owner: &Pubkey) -> Self {
        Self {
            lamports,
            data: vec![0u8; space],
            owner: *owner,
            ..Self::default()
        }
    }
    pub fn new_ref(lamports: u64, space: usize, owner: &Pubkey) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self::new(lamports, space, owner)))
    }

    pub fn new_data<T: serde::Serialize>(
        lamports: u64,
        state: &T,
        owner: &Pubkey,
    ) -> Result<Self, bincode::Error> {
        let data = bincode::serialize(state)?;
        Ok(Self {
            lamports,
            data,
            owner: *owner,
            ..Self::default()
        })
    }
    pub fn new_ref_data<T: serde::Serialize>(
        lamports: u64,
        state: &T,
        owner: &Pubkey,
    ) -> Result<RefCell<Self>, bincode::Error> {
        Ok(RefCell::new(Self::new_data(lamports, state, owner)?))
    }

    pub fn new_data_with_space<T: serde::Serialize>(
        lamports: u64,
        state: &T,
        space: usize,
        owner: &Pubkey,
    ) -> Result<Self, bincode::Error> {
        let mut account = Self::new(lamports, space, owner);

        account.serialize_data(state)?;

        Ok(account)
    }
    pub fn new_ref_data_with_space<T: serde::Serialize>(
        lamports: u64,
        state: &T,
        space: usize,
        owner: &Pubkey,
    ) -> Result<RefCell<Self>, bincode::Error> {
        Ok(RefCell::new(Self::new_data_with_space(
            lamports, state, space, owner,
        )?))
    }

    pub fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, bincode::Error> {
        bincode::deserialize(&self.data)
    }

    pub fn serialize_data<T: serde::Serialize>(&mut self, state: &T) -> Result<(), bincode::Error> {
        if bincode::serialized_size(state)? > self.data.len() as u64 {
            return Err(Box::new(bincode::ErrorKind::SizeLimit));
        }
        bincode::serialize_into(&mut self.data[..], state)
    }
}

/// Create an `Account` from a `Sysvar`.
pub fn create_account<S: Sysvar>(sysvar: &S, lamports: u64) -> AccountNoData {
    let data_len = S::size_of().max(bincode::serialized_size(sysvar).unwrap() as usize);
    let mut account = AccountNoData::new(lamports, data_len, &solana_program::sysvar::id());
    to_account_no_data::<S>(sysvar, &mut account).unwrap();
    account
}

/// Create a `Sysvar` from an `Account`'s data.
pub fn from_account<S: Sysvar>(account: &Account) -> Option<S> {
    bincode::deserialize(&account.data).ok()
}

/// Create a `Sysvar` from an `Account`'s data.
pub fn from_account_no_data<S: Sysvar>(account: &AccountNoData) -> Option<S> {
    bincode::deserialize(&account.data).ok()
}

/// Serialize a `Sysvar` into an `Account`'s data.
pub fn to_account<S: Sysvar>(sysvar: &S, account: &mut Account) -> Option<()> {
    bincode::serialize_into(&mut account.data[..], sysvar).ok()
}

/// Serialize a `Sysvar` into an `Account`'s data.
pub fn to_account_no_data<S: Sysvar>(sysvar: &S, account: &mut AccountNoData) -> Option<()> {
    let data: &mut Vec<u8> = Arc::make_mut(&mut account.data);
    bincode::serialize_into(&mut data[..], sysvar).ok()
}

/// Return the information required to construct an `AccountInfo`.  Used by the
/// `AccountInfo` conversion implementations.
impl solana_program::account_info::Account for Account {
    fn get(&mut self) -> (&mut u64, &mut [u8], &Pubkey, bool, Epoch) {
        (
            &mut self.lamports,
            &mut self.data,
            &self.owner,
            self.executable,
            self.rent_epoch,
        )
    }
}

/// Create `AccountInfo`s
pub fn create_account_infos(accounts: &mut [(Pubkey, Account)]) -> Vec<AccountInfo> {
    accounts.iter_mut().map(Into::into).collect()
}

/// Create `AccountInfo`s
pub fn create_is_signer_account_infos<'a>(
    accounts: &'a mut [(&'a Pubkey, bool, &'a mut Account)],
) -> Vec<AccountInfo<'a>> {
    accounts
        .iter_mut()
        .map(|(key, is_signer, account)| {
            AccountInfo::new(
                key,
                *is_signer,
                false,
                &mut account.lamports,
                &mut account.data,
                &account.owner,
                account.executable,
                account.rent_epoch,
            )
        })
        .collect()
}
