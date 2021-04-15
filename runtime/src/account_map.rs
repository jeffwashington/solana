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

impl< V: Clone> Default for AccountMap<V> {
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
    pub fn or_insert_with<V: Clone, F>(self, default: F, btree: &mut AccountMap<V>) -> &V
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
    pub fn insert<V: Clone>(self, value: V, btree: &mut AccountMap<V>) -> &V {    
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
    pub fn into_mut<V: Clone>(self, btree: &AccountMap<V>) -> &V {
        btree.get_at_index(&self.index).unwrap()
    }
    pub fn get<'a, 'b, V: Clone>(&'a self, btree: &'b AccountMap<V>) -> &'b V {
        btree.get_at_index(&self.index).unwrap()
    }
    pub fn remove<V: Clone>(self, btree: &mut AccountMap<V>) {
        btree.remove_at_index(&self.index);
    }
}

#[derive(Debug, Clone)]
pub struct AccountMapIndex {
    pub index: usize,
    pub insert: bool,
}

#[derive(Debug, Clone)]
pub struct InnerAccountMapIndex {
    pub outer_index: usize,
    pub inner_index: usize,
}

//pub type AccountMap<K, V> = BTreeMap<K, V>;
#[derive(Debug)]
pub struct AccountMap<V> {
    keys: Vec<Vec<Pubkey>>,
    values: Vec<Vec<V>>,
    count: usize,
    cumulative_lens: Vec<usize>,
    vec_size_max: usize,
}

impl<V: Clone> AccountMap<V> {
    pub fn new() -> Self {
        let vec_size_max = 1;
        Self {
            keys: vec![Self::new_vec(vec_size_max)],
            values: vec![Self::new_vec(vec_size_max)],
            count: 0,
            cumulative_lens: vec![0],
            vec_size_max,
        }
    }

    fn new_vec<T>(size: usize) -> Vec<T> {
        Vec::with_capacity(size)
    }

    pub fn get_index(&self, index: &AccountMapIndex) -> Option<(&Pubkey, &V)> {
        if index.insert || index.index >= self.count {
            None
        }
        else {
            let outer = self.get_outer_index(index.index);
            Some((&self.keys[outer.outer_index][outer.inner_index], &self.values[outer.outer_index][outer.inner_index]))
        }
    }

    fn mv<U: Clone>(vec: &mut Vec<Vec<U>>, outer: usize, inner: usize, to_move: usize, value: U) {
        let mut new_vec;
        if to_move == 0 {
            // could look forward to see if next outer can fit this new item without growing too big
            new_vec = vec![value];
        }
        else {
            new_vec = vec[outer][inner..].to_vec();
            vec[outer].resize(inner + 1, value.clone());
            vec[outer][inner] = value;
        }
        vec.insert(outer + 1, new_vec);
    }

    pub fn insert_at_index_alloc(&mut self, index: &AccountMapIndex, key: Pubkey, value: V, outer: &mut InnerAccountMapIndex ) {
        let max = self.vec_size_max;
        let max_move = max; // tune this later
        let size = self.keys[outer.outer_index].len();
        let to_move = size-outer.inner_index;
        //error!("insert_at_index_alloc, outer: {}, inner: {}, len: {}, to_move: {}, size: {}, values: {:?}", outer.outer_index, outer.inner_index, self.count, to_move, size, self.keys);
        if size > 0 && (to_move > max_move || size + 1 >= max) {
            // have to add a new vector
            Self::mv(&mut self.keys, outer.outer_index, outer.inner_index, to_move, key);
            Self::mv(&mut self.values, outer.outer_index, outer.inner_index, to_move, value);
            //error!("new keys: {:?}", self.keys);
            self.cumulative_lens.insert(outer.outer_index, self.cumulative_lens[outer.outer_index]); // we inserted here
            if to_move == 0 {
                //self.cumulative_lens[outer.outer_index + 1] -= outer.inner_index;
                outer.outer_index += 1;
                outer.inner_index = 0;
            }
            else {
                self.cumulative_lens[outer.outer_index] -= to_move;
            }
        }
        else {
            // no new vector - just insert
            self.keys[outer.outer_index].insert(outer.inner_index, key);
            self.values[outer.outer_index].insert(outer.inner_index, value);
        }
        // shift the rest of the cumulative offsets since we inserted
        for outer_index in &mut self.cumulative_lens[outer.outer_index..] {
            *outer_index += 1;
        }
        //error!("Inserted, lens: {:?}, keys: {:?}", self.cumulative_lens, self.keys);
        self.count += 1;
    }

    pub fn get_outer_index(&self, given_index: usize) -> InnerAccountMapIndex {
        let mut l = 0;
        let mut index = l;
        let mut inner_index = 0;
        if self.count > 0 {
            let mut r = self.cumulative_lens.len();
            let mut iteration = 0;
            loop {
                index = (l + r) / 2;
                let val = self.cumulative_lens[index];
                let cmp = given_index.partial_cmp(&val).unwrap();
                //error!("outer: left: {}, right: {}, count: {}, index: {}, cmp: {:?}, val: {:?}, iteration: {}", l, r, self.cumulative_lens.len(), index, cmp, val, iteration);
                iteration += 1;
                match cmp {
                    Ordering::Equal => {
                        if index + 1 < self.cumulative_lens.len() {
                        index += 1;
                        }
                        break;},
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
            //error!("returning: index: {}, inner_index: {}, given_index: {}, cumulative_lens: {:?}", index, inner_index, given_index, self.cumulative_lens);
            if index > 0 {
                inner_index = given_index - self.cumulative_lens[index - 1];
            }
            else {
                inner_index = given_index;
            }
            //error!("returned: outer: {}, inner_index: {}, given_index: {}, cumulative_lens: {:?}", index, inner_index, given_index, self.cumulative_lens);
        }

        InnerAccountMapIndex {
            outer_index: index,
            inner_index,
        }
    }
/*
    pub fn increment(&self, outer: &mut InnerAccountMapIndex) {
        while outer.outer_index < self.values.len() {
            let current = self.values[outer.outer_index];
            outer.inner_index += 1;
            if outer.inner_index < current.len() {
                return;
            }
            outer.inner_index = 0;
            outer.outer_index += 1;
            // loop because we could have an empty inner array and we may need to increment outer again
        }
    }
    */

    pub fn find(&self, key: &Pubkey) -> AccountMapIndex {
        let mut l = 0;
        let mut index = l;
        let mut insert = true;
        if self.count > 0 {
            let mut r = self.count;
            let mut iteration = 0;
            //error!("keys: {:?}", self.keys);
            loop {
                index = (l + r) / 2;
                let outer = self.get_outer_index(index);
                //error!("keys2: {:?}, outer: {:?}", self.keys, outer);
                let val = self.keys[outer.outer_index][outer.inner_index];
                let cmp = key.partial_cmp(&val).unwrap();
                //error!("left: {}, right: {}, count: {}, index: {}, cmp: {:?}, key: {:?} val: {:?}, iteration: {}, keys: {:?}", l, r, self.count, index, cmp, val, key, iteration, self.keys);
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
        let mut outer = self.get_outer_index(index.index);

        if index.insert {
            self.insert_at_index_alloc(index, key, value, &mut outer);
        }
        else {
            self.values[outer.outer_index][outer.inner_index] = value;
        }
        //error!("outer: {}, inner: {}, len: {}, insert: {}", outer.outer_index, outer.inner_index, self.values.len(), index.insert);
        if self.count % 20_000 == 0 {
            error!("count: {}, lens: {:?}", self.count, self.cumulative_lens);
        }
        &self.values[outer.outer_index][outer.inner_index]
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
        self.get_at_index(&find)
    }
    pub fn get_at_index(&self, index: &AccountMapIndex) -> Option<&V> {
        if index.insert {
            None
        }
        else {
            let mut outer = self.get_outer_index(index.index);
            Some(&self.values[outer.outer_index][outer.inner_index])
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
            let mut outer = self.get_outer_index(index.index);
            self.count -= 1;
            // TODO - could shrink to zero size here
            self.keys[outer.outer_index].remove(outer.inner_index);
            self.values[outer.outer_index].remove(outer.inner_index);
            for cumulative in &mut self.cumulative_lens[outer.outer_index..] {
                *cumulative -= 1;
            }
        }
    }
    pub fn keys(&self) -> AccountMapIterKeys<'_, V> {//std::slice::Iter<'_, (K, V)> {
        AccountMapIterKeys::new(&self, AccountMapIndex {index: 0, insert: false,})
    }
    pub fn values(&self) -> AccountMapIterValues<'_, V> {//std::slice::Iter<'_, (K, V)> {
        AccountMapIterValues::new(&self, AccountMapIndex {index: 0, insert: false,})
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
impl<'a, V:Clone> Iterator for AccountMapIter<'a, V> {
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
pub struct AccountMapIterValues<'a, V> {
    pub index: AccountMapIndex,
    pub btree: &'a AccountMap<V>,
    //_dummy: PhantomData<V>,
}
impl<'a, V> AccountMapIterValues<'a, V> {
    pub fn new(btree: &'a AccountMap<V>, index: AccountMapIndex) -> Self {
        Self {
            index,
            btree,
        }
    }
}
impl<'a, V: Clone> Iterator for AccountMapIterValues<'a, V> {
    type Item = &'a V;

    // next() is the only required method
    fn next(&mut self) -> Option<Self::Item> {
        let result =   self.btree.get_index(&self.index).map(|(_,v)| v);
        if result.is_some() {
            self.index.index += 1;
        }

        result
    }    
}

pub struct AccountMapIterKeys<'a, V: Clone> {
    pub index: AccountMapIndex,
    pub btree: &'a AccountMap<V>,
    //_dummy: PhantomData<V>,
}
impl<'a, V: Clone> AccountMapIterKeys<'a, V> {
    pub fn new(btree: &'a AccountMap<V>, index: AccountMapIndex) -> Self {
        Self {
            index,
            btree,
        }
    }
}
impl<'a, V:Clone> Iterator for AccountMapIterKeys<'a, V> {
    type Item = &'a Pubkey;

    // next() is the only required method
    fn next(&mut self) -> Option<Self::Item> {
        let result =   self.btree.get_index(&self.index).map(|(k,_)| k);
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
    fn test_account_map123() {
        solana_logger::setup();
        let key0 = Pubkey::new(&[0u8; 32]);
        let key1 = Pubkey::new(&[1u8; 32]);
        let key2 = Pubkey::new(&[2u8; 32]);
        let key3= Pubkey::new(&[3u8; 32]);

        let mut m = AccountMap::new();
        let val0 = 0;
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
        m.remove(&key2);
        assert_eq!(2, m.len());
        assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key1, &key3]);
        m.insert(key0, val0);
        assert_eq!(m.get(&key0).unwrap(), &val0);
        assert_eq!(3, m.len());
        assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key0, &key1, &key3]);
        assert_eq!(m.values().collect::<Vec<_>>(), vec![&val0, &val1, &val3]);
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
