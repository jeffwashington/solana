use crate::{
    contains::Contains,
    inline_spl_token_v2_0::{self, SPL_TOKEN_ACCOUNT_MINT_OFFSET, SPL_TOKEN_ACCOUNT_OWNER_OFFSET},
    secondary_index::*,
};
use log::*;
use bv::BitVec;
use dashmap::DashSet;
use ouroboros::self_referencing;
use solana_measure::measure::Measure;
use crate::message_processor::ExecuteDetailsTimings2;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::fmt::Debug;
use solana_sdk::{
    clock::Slot,
    pubkey::{Pubkey, PUBKEY_BYTES},
};
use std::{
    collections::{
        btree_map::{self, BTreeMap},
        HashMap, HashSet,
    },
    ops::{
        Bound,
        Bound::{Excluded, Included, Unbounded},
        Range, RangeBounds,
    },
    sync::{
        atomic::{AtomicU64},
        Arc, RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
};

impl< V> Default for AccountMap<V> {
    /// Creates an empty `BTreeMap`.
    fn default() -> AccountMap<V> {
        AccountMap::new()
    }
}

#[derive(Debug, Clone)]
pub enum AccountMapEntryBtree {
    /// A vacant entry.
    Vacant(VacantEntry),

    /// An occupied entry.
    Occupied(OccupiedEntry),
}

impl AccountMapEntryBtree {
    pub fn or_insert_with<V, F>(self, default: F, btree: &mut AccountMap<V>) -> &V
    where
        F: FnOnce() -> V, 
        {
            match self {
                AccountMapEntryBtree::Occupied(entry) => entry.into_mut(btree),
                AccountMapEntryBtree::Vacant(entry) => entry.insert(default(), btree),
            }
        }    
}

#[derive(Clone)]
pub struct VacantEntry {
    pub index: AccountMapIndex,
    pub key: Pubkey,
}

impl Debug for VacantEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("VacantEntry").field(self.key()).finish()
    }
}

impl VacantEntry {
    /// Creates an empty `BTreeMap`.
    pub fn key(&self) -> &Pubkey {
        &self.key
    }
    pub fn new(key: Pubkey, index: AccountMapIndex) -> Self {
        Self {
            key,
            index,
        }
    }
    pub fn insert<V>(self, value: V, btree: &mut AccountMap<V>) -> &V {    
        btree.insert_at_index(&self.index, self.key, value)
    }
}



/// A view into an occupied entry in a `BTreeMap`.
/// It is part of the [`Entry`] enum.
#[derive(Debug, Clone)]
pub struct OccupiedEntry {
    pub index: AccountMapIndex,
}

impl OccupiedEntry {
    pub fn new(index: AccountMapIndex) -> Self {
        Self {
            index,
        }
    }
    pub fn into_mut<V>(self, btree: &AccountMap<V>) -> &V {
        btree.get_at_index(&self.index).unwrap()
    }
    pub fn get<'a, 'b, V>(&'a self, btree: &'b AccountMap<V>) -> &'b V {
        btree.get_at_index(&self.index).unwrap()
    }
    pub fn remove<V>(self, btree: &mut AccountMap<V>) {
        btree.remove_at_index(&self.index);
    }
}

#[derive(Debug, Clone)]
pub struct AccountMapIndex {
    pub index: usize,
    pub insert: bool,
}

//pub type AccountMap<K, V> = BTreeMap<K, V>;
#[derive(Debug)]
pub struct AccountMap<V> {
    keys: Vec<Pubkey>,
    values: Vec<V>,
    count: usize,
}

impl<V> AccountMap<V> {
    pub fn new() -> Self {
        Self {
            keys: Vec::with_capacity(25_000_000),
            values: Vec::with_capacity(25_000_000),
            count: 0,
        }
    }
    pub fn get_index(&self, index: &AccountMapIndex) -> Option<(&Pubkey, &V)> {
        if index.insert {
            None
        }
        else {
            Some((&self.keys[index.index], &self.values[index.index]))
        }
    }

    pub fn find(&self, key: &Pubkey) -> AccountMapIndex {
        let mut l = 0;
        let mut index = l;
        let mut insert = true;
        if self.count > 0 {
            let count_minus_1 = self.count - 1;
            let mut r = self.count;
            let mut iteration = 0;
            loop {
                index = (l + r) / 2;
                let val = self.keys[index];
                let cmp = key.partial_cmp(&val).unwrap();
                //error!("left: {}, right: {}, count: {}, index: {}, cmp: {:?}, key: {:?} val: {:?}, iteration: {}", l, r, self.count, index, cmp, val, key, iteration);
                iteration += 1;
                match cmp {
                    Ordering::Equal => {insert = false;break;},
                    Ordering::Less => {r = index;
                    },
                    Ordering::Greater => {
                        if index == r - 1 {
                            index = r;
                            break;
                        }
                        l = index;
                        
                    },
                }
                if r == l {
                    break;
                }
            }
        }

        AccountMapIndex {
            index,
            insert,
        }
    }
    pub fn len(&self) -> usize {
        self.count
    }
    pub fn insert(&mut self, key: Pubkey, value: V) -> &V {
        let find = self.find(&key);
        self.insert_at_index(&find, key, value)
    }
    pub fn insert_at_index(&mut self, index: &AccountMapIndex, key: Pubkey, value: V) -> &V {
        if index.insert {
            self.keys.insert(index.index, key);
            self.values.insert(index.index, value);
            self.count += 1;
        }
        else {
            self.values[index.index] = value;
        }
        &self.values[index.index]
    }
    pub fn entry(&self, key: Pubkey) -> AccountMapEntryBtree {
        let find = self.find(&key);
        if find.insert {
            AccountMapEntryBtree::Vacant(VacantEntry::new(key, find))
        }
        else {
            AccountMapEntryBtree::Occupied(OccupiedEntry::new(find))
        }
    }
    pub fn contains_key(&self, key: &Pubkey) -> bool {
        let find = self.find(&key);
        !find.insert
    }
    pub fn get(&self, key: &Pubkey) -> Option<&V> {
        let find = self.find(&key);
        if find.insert {
            None
        }
        else {
            Some(&self.values[find.index])
        }
    }
    pub fn get_at_index(&self, index: &AccountMapIndex) -> Option<&V> {
        if index.insert {
            None
        }
        else {
            Some(&self.values[index.index])
        }
    }
    pub fn remove(&mut self, key: &Pubkey) {
        let find = self.find(&key);
        self.remove_at_index(&find)
    }
    pub fn remove_at_index(&mut self, index: &AccountMapIndex) {
        if index.insert {
        }
        else {
            self.count -= 1;
            self.keys.remove(index.index);
            self.values.remove(index.index);
        }
    }
    pub fn keys(&self) -> std::slice::Iter<'_, Pubkey> {
        self.keys.iter()
    }
    pub fn values(&self) -> std::slice::Iter<'_, V> {
        self.values.iter()
    }
    pub fn iter(&self) -> AccountMapIter<'_, V> {//std::slice::Iter<'_, (K, V)> {
        AccountMapIter::new(&self, AccountMapIndex {index: 0, insert: false,})
    }
    pub fn range<T, R>(&self, range: R) -> Option<(Pubkey, Pubkey)>
    where
        T: Ord + ?Sized,
        R: RangeBounds<T>,
        {
            panic!("not supported");
            None
        }
}

pub struct AccountMapIter<'a, V> {
    pub index: AccountMapIndex,
    pub btree: &'a AccountMap<V>,
    //_dummy: PhantomData<V>,
}
impl<'a, V> AccountMapIter<'a, V> {
    pub fn new(btree: &'a AccountMap<V>, index: AccountMapIndex) -> Self {
        Self {
            index,
            btree,
        }
    }
}
impl<'a, V> Iterator for AccountMapIter<'a, V> {
    type Item = (&'a Pubkey, &'a V);

    // next() is the only required method
    fn next(&mut self) -> Option<Self::Item> {
        let result =   self.btree.get_index(&self.index);
        if result.is_some() {
            self.index.index += 1;
        }

        result
    }    
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use log::*;
    use solana_sdk::signature::{Keypair, Signer};
    use std::collections::HashMap;

    #[test]
    fn test_account_map() {
        solana_logger::setup();
        let key1 = Pubkey::new(&[1u8; 32]);
        let key2 = Pubkey::new(&[2u8; 32]);
        let key3= Pubkey::new(&[3u8; 32]);

        let mut m = AccountMap::new();
        let val1 = 1;
        let val2 = 1;
        let val3 = 1;
        assert_eq!(0, m.len());
        m.insert(key1, val1);
        assert_eq!(m.get(&key1).unwrap(), &val1);
        assert_eq!(1, m.len());
        assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key1]);
        m.insert(key3, val3);
        assert_eq!(m.get(&key3).unwrap(), &val3);
        assert_eq!(2, m.len());
        assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key1, &key3]);
        m.insert(key2, val2);
        assert_eq!(m.get(&key2).unwrap(), &val2);
        assert_eq!(3, m.len());
        assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key1, &key2, &key3]);
        assert_eq!(m.values().collect::<Vec<_>>(), vec![&val1, &val2, &val3]);
    }

    #[test]
    fn test_account_map_entry() {
        solana_logger::setup();
        let key1 = Pubkey::new(&[1u8; 32]);
        let key2 = Pubkey::new(&[2u8; 32]);
        let key3= Pubkey::new(&[3u8; 32]);

        let mut m = AccountMap::new();
        let val1 = 1;
        let val2 = 1;
        let val3 = 1;
        assert_eq!(0, m.len());
        let entry = m.entry(key1).or_insert_with(|| val1, &mut m);
        assert_eq!(m.get(&key1).unwrap(), &val1);

        assert_eq!(1, m.len());
        assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key1]);
        m.insert(key3, val3);
        assert_eq!(m.get(&key3).unwrap(), &val3);
        assert_eq!(2, m.len());
        assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key1, &key3]);
        m.insert(key2, val2);
        assert_eq!(m.get(&key2).unwrap(), &val2);
        assert_eq!(3, m.len());
        assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key1, &key2, &key3]);
    }
}
