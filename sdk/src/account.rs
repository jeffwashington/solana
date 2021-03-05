use crate::{clock::Epoch, pubkey::Pubkey};
use solana_program::{account_info::AccountInfo, sysvar::Sysvar};
use std::{cell::Ref, cell::RefCell, cmp, fmt, rc::Rc};

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

/// An Account with data that is stored on chain
/// This will become a new in-memory representation of the 'Account' struct data.
/// The existing 'Account' structure cannot easily change due to downstream projects.
/// This struct will shortly rely on something like the AnAccount trait for access to the fields.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Default, AbiExample)]
pub struct AccountNoData {
    /// lamports in the account
    pub lamports: u64,
    /// data held in this account
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>, // will be: Arc<Vec<u8>>,
    /// the program that owns this account. If executable, the program that loads this account.
    pub owner: Pubkey,
    /// this account's data contains a loaded program (and is now read-only)
    pub executable: bool,
    /// the epoch at which this account will next owe rent
    pub rent_epoch: Epoch,
}

pub fn accounts_equal<T: AnAccount, U: AnAccount>(me: &T, other: &U) -> bool {
    me.lamports() == other.lamports()
        && me.data() == other.data()
        && me.owner() == other.owner()
        && me.executable() == other.executable()
        && me.rent_epoch() == other.rent_epoch()
}

impl From<AccountNoData> for Account {
    fn from(other: AccountNoData) -> Self {
        Self {
            lamports: other.lamports,
            data: other.data,
            owner: other.owner,
            executable: other.executable,
            rent_epoch: other.rent_epoch,
        }
    }
}

impl From<Account> for AccountNoData {
    fn from(other: Account) -> Self {
        Self {
            lamports: other.lamports,
            data: other.data,
            owner: other.owner,
            executable: other.executable,
            rent_epoch: other.rent_epoch,
        }
    }
}

pub trait AnAccountWritable: AnAccount {
    fn set_lamports(&mut self, lamports: u64);
    fn data_as_mut_slice(&mut self) -> &mut [u8];
    fn set_owner(&mut self, owner: Pubkey);
    fn set_rent_epoch(&mut self, epoch: Epoch);
}

pub trait AnAccount: Sized {
    fn lamports(&self) -> u64;
    fn data(&self) -> &Vec<u8>;
    fn owner(&self) -> &Pubkey;
    fn executable(&self) -> bool;
    fn rent_epoch(&self) -> Epoch;
}

impl AnAccount for Account {
    fn lamports(&self) -> u64 {
        self.lamports
    }
    fn data(&self) -> &Vec<u8> {
        &self.data
    }
    fn owner(&self) -> &Pubkey {
        &self.owner
    }
    fn executable(&self) -> bool {
        self.executable
    }
    fn rent_epoch(&self) -> Epoch {
        self.rent_epoch
    }
}

impl AnAccountWritable for Account {
    fn set_lamports(&mut self, lamports: u64) {
        self.lamports = lamports;
    }
    fn data_as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
    fn set_owner(&mut self, owner: Pubkey) {
        self.owner = owner;
    }
    fn set_rent_epoch(&mut self, epoch: Epoch) {
        self.rent_epoch = epoch;
    }
}

impl AnAccountWritable for AccountNoData {
    fn set_lamports(&mut self, lamports: u64) {
        self.lamports = lamports;
    }
    fn data_as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
    fn set_owner(&mut self, owner: Pubkey) {
        self.owner = owner;
    }
    fn set_rent_epoch(&mut self, epoch: Epoch) {
        self.rent_epoch = epoch;
    }
}

impl AnAccount for AccountNoData {
    fn lamports(&self) -> u64 {
        self.lamports
    }
    fn data(&self) -> &Vec<u8> {
        &self.data
    }
    fn owner(&self) -> &Pubkey {
        &self.owner
    }
    fn executable(&self) -> bool {
        self.executable
    }
    fn rent_epoch(&self) -> Epoch {
        self.rent_epoch
    }
}

impl AnAccount for Ref<'_, AccountNoData> {
    fn lamports(&self) -> u64 {
        self.lamports
    }
    fn data(&self) -> &Vec<u8> {
        &self.data
    }
    fn owner(&self) -> &Pubkey {
        &self.owner
    }
    fn executable(&self) -> bool {
        self.executable
    }
    fn rent_epoch(&self) -> Epoch {
        self.rent_epoch
    }
}

fn debug_fmt<T: AnAccount>(item: &T, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let data_len = cmp::min(64, item.data().len());
    let data_str = if data_len > 0 {
        format!(" data: {}", hex::encode(item.data()[..data_len].to_vec()))
    } else {
        "".to_string()
    };
    write!(
        f,
        "Account {{ lamports: {} data.len: {} owner: {} executable: {} rent_epoch: {}{} }}",
        item.lamports(),
        data_len,
        item.owner(),
        item.executable(),
        item.rent_epoch(),
        data_str,
    )
}

impl fmt::Debug for Account {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_fmt(self, f)
    }
}

impl fmt::Debug for AccountNoData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_fmt(self, f)
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

impl AccountNoData {
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
pub fn create_account<S: Sysvar>(sysvar: &S, lamports: u64) -> Account {
    let data_len = S::size_of().max(bincode::serialized_size(sysvar).unwrap() as usize);
    let mut account = Account::new(lamports, data_len, &solana_program::sysvar::id());
    to_account::<S, Account>(sysvar, &mut account).unwrap();
    account
}

/// Create an `Account` from a `Sysvar`.
pub fn create_account_no_data<S: Sysvar>(sysvar: &S, lamports: u64) -> AccountNoData {
    AccountNoData::from(create_account(sysvar, lamports))
}

/// Create a `Sysvar` from an `Account`'s data.
pub fn from_account<S: Sysvar, T: AnAccount>(account: &T) -> Option<S> {
    bincode::deserialize(account.data()).ok()
}

/// Serialize a `Sysvar` into an `Account`'s data.
pub fn to_account<S: Sysvar, T: AnAccountWritable>(sysvar: &S, account: &mut T) -> Option<()> {
    bincode::serialize_into(account.data_as_mut_slice(), sysvar).ok()
}

/// Return the information required to construct an `AccountInfo`.  Used by the
/// `AccountInfo` conversion implementations.
impl solana_program::account_info::Account for AccountNoData {
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
pub fn create_account_infos(accounts: &mut [(Pubkey, AccountNoData)]) -> Vec<AccountInfo> {
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
