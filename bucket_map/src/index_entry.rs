#![allow(dead_code)]

use {
    crate::{
        bucket_storage::{BucketOccupied, BucketStorage},
        RefCount,
    },
    bv::BitVec,
    modular_bitfield::prelude::*,
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    std::{fmt::Debug, marker::PhantomData},
};

/// in use/occupied
const OCCUPIED_OCCUPIED: u8 = 1;
/// free, ie. not occupied
const OCCUPIED_FREE: u8 = 0;

/// header for elements in a bucket
/// needs to be multiple of size_of::<u64>()
#[derive(Copy, Clone)]
#[repr(C)]
pub(crate) struct OccupiedHeader {
    /// OCCUPIED_OCCUPIED or OCCUPIED_FREE
    pub(crate) packed_ref_count: PackedRefCount,
}

pub struct BucketWithHeader {}

impl BucketOccupied for BucketWithHeader {
    fn occupy(&mut self, element: &mut [u8], ix: usize) {
        assert!(self.is_free(element, ix));
        let entry: &mut OccupiedHeader =
            BucketStorage::<BucketWithHeader>::get_mut_from_parts(element);
        entry.packed_ref_count.set_occupied(OCCUPIED_OCCUPIED);
    }
    fn free(&mut self, element: &mut [u8], ix: usize) {
        assert!(!self.is_free(element, ix));
        let entry: &mut OccupiedHeader =
            BucketStorage::<BucketWithHeader>::get_mut_from_parts(element);
        entry.packed_ref_count.set_occupied(OCCUPIED_FREE);
    }
    fn is_free(&self, element: &[u8], _ix: usize) -> bool {
        let entry: &OccupiedHeader = BucketStorage::<BucketWithHeader>::get_from_parts(element);
        entry.packed_ref_count.occupied() == OCCUPIED_FREE
    }
    fn offset_to_first_data() -> usize {
        std::mem::size_of::<OccupiedHeader>()
    }
    /// initialize this struct
    fn new(_num_elements: usize) -> Self {
        Self {}
    }
    fn growing_index_element(
        &mut self,
        element_new: &mut [u8],
        _ix_new: usize,
        _other: &Self,
        element_old: &[u8],
        _ix_old: usize,
    ) {
        let entry: &mut OccupiedHeader =
            BucketStorage::<BucketWithHeader>::get_mut_from_parts(element_new);
        let prev: &OccupiedHeader = BucketStorage::<BucketWithHeader>::get_from_parts(element_old);
        *entry = *prev;
    }
}

/// allocated in `contents` in a BucketStorage
#[derive(Debug, Default)]
pub struct IndexBucketUsingRefCountBits<T> {
    /// 2 bits per entry that represent a 4 state enum tag
    pub enum_tag: BitVec,
    _phantom: PhantomData<T>,
}

impl<T: Copy + 'static> BucketOccupied for IndexBucketUsingRefCountBits<T> {
    fn occupy(&mut self, element: &mut [u8], ix: usize) {
        assert!(self.is_free(element, ix));
        self.enum_tag.set(ix as u64 * 2, false);
        self.enum_tag.set(ix as u64 * 2 + 1, true);
    }
    fn free(&mut self, element: &mut [u8], ix: usize) {
        assert!(!self.is_free(element, ix));
        self.enum_tag.set(ix as u64 * 2, false);
        self.enum_tag.set(ix as u64 * 2 + 1, false);
    }
    fn is_free(&self, _element: &[u8], ix: usize) -> bool {
        !self.enum_tag.get(ix as u64 * 2) && !self.enum_tag.get(ix as u64 * 2 + 1)
    }
    fn offset_to_first_data() -> usize {
        0
    }
    fn new(num_elements: usize) -> Self {
        Self {
            enum_tag: BitVec::new_fill(false, num_elements as u64 * 2),
            _phantom: PhantomData,
        }
    }
    fn growing_index_element(
        &mut self,
        _element_new: &mut [u8],
        ix_new: usize,
        other: &Self,
        _element_old: &[u8],
        ix_old: usize,
    ) {
        self.enum_tag
            .set(ix_new as u64 * 2, other.enum_tag.get(ix_old as u64 * 2));
        self.enum_tag.set(
            ix_new as u64 * 2 + 1,
            other.enum_tag.get(ix_old as u64 * 2 + 1),
        );
    }
}

pub type DataBucket = BucketWithHeader;
pub type IndexBucket<T> = IndexBucketUsingRefCountBits<T>;

/// contains the index of an entry in the index bucket.
/// This type allows us to call methods to interact with the index entry on this type.
pub struct IndexEntryPlaceInBucket<T> {
    pub ix: u64,
    _phantom: PhantomData<T>,
}

#[repr(C)]
#[derive(Copy, Clone)]
// one instance of this per item in the index
// stored in the index bucket
pub struct IndexEntry<T: Clone + Copy> {
    pub(crate) key: Pubkey, // can this be smaller if we have reduced the keys into buckets already?
    /// depends on the contents of ref_count.slot_count_enum
    pub(crate) contents: SingleElementOrMultipleSlots<T>,
}

#[bitfield(bits = 64)]
#[repr(C)]
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub(crate) struct PackedRefCount {
    /// whether this entry in the data file is occupied or not
    pub(crate) occupied: B1,
    /// ref_count of this entry. We don't need any where near 63 bits for this value
    pub(crate) ref_count: B63,
}

/// required fields when an index element references the data file
#[repr(C)]
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub(crate) struct MultipleSlots {
    // if the bucket doubled, the index can be recomputed using storage_cap_and_offset.create_bucket_capacity_pow2
    pub(crate) storage_cap_and_offset: PackedStorage,
    /// num elements in the slot list
    pub num_slots: Slot,
}

impl MultipleSlots {
    pub(crate) fn set_storage_capacity_when_created_pow2(
        &mut self,
        storage_capacity_when_created_pow2: u8,
    ) {
        self.storage_cap_and_offset
            .set_capacity_when_created_pow2(storage_capacity_when_created_pow2)
    }

    pub(crate) fn set_storage_offset(&mut self, storage_offset: u64) {
        self.storage_cap_and_offset
            .set_offset_checked(storage_offset)
            .expect("New storage offset must fit into 7 bytes!")
    }

    pub(crate) fn storage_capacity_when_created_pow2(&self) -> u8 {
        self.storage_cap_and_offset.capacity_when_created_pow2()
    }

    fn storage_offset(&self) -> u64 {
        self.storage_cap_and_offset.offset()
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub(crate) union SingleElementOrMultipleSlots<T: Clone + Copy> {
    /// the slot list contains a single element. No need for an entry in the data file.
    pub(crate) single_element: T,
    /// the slot list ocntains more than one element. This contains the reference to the data file.
    pub(crate) multiple_slots: MultipleSlots,
}

#[repr(u8)]
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum SlotCountEnum<'a, T> {
    /// this spot is not allocated
    Free = 0,
    /// zero slots in the slot list
    ZeroSlots = 1,
    /// one slot in the slot list, it is stored in the index
    OneSlotInIndex(&'a T) = 2,
    /// if > 1 slots or ref_count != 1, ref_count and slots are stored in data file
    MultipleSlots(&'a MultipleSlots) = 3,
}

/// Pack the storage offset and capacity-when-crated-pow2 fields into a single u64
#[bitfield(bits = 64)]
#[repr(C)]
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub(crate) struct PackedStorage {
    capacity_when_created_pow2: B8,
    offset: B56,
}

impl<T: Copy> IndexEntry<T> {
    /// return closest bucket index fit for the slot slice.
    /// Since bucket size is 2^index, the return value is
    ///     min index, such that 2^index >= num_slots
    ///     index = ceiling(log2(num_slots))
    /// special case, when slot slice empty, return 0th index.
    pub fn data_bucket_from_num_slots(num_slots: Slot) -> u64 {
        // Compute the ceiling of log2 for integer
        if num_slots == 0 {
            0
        } else {
            (Slot::BITS - (num_slots - 1).leading_zeros()) as u64
        }
    }
}

impl<T: Copy + 'static> IndexEntryPlaceInBucket<T> {
    fn get_enum_tag(&self, index_bucket: &BucketStorage<IndexBucket<T>>) -> u8 {
        let bitvec = &index_bucket.contents.enum_tag;
        (bitvec.get(self.ix * 2) as u8 * 2) + bitvec.get(self.ix * 2 + 1) as u8
    }
    fn set_enum_tag(&self, index_bucket: &mut BucketStorage<IndexBucket<T>>, value: u8) {
        let bitvec = &mut index_bucket.contents.enum_tag;
        bitvec.set(self.ix * 2, (value & 2) == 2);
        bitvec.set(self.ix * 2 + 1, (value & 1) == 1);
    }
    pub(crate) fn get_slot_count_enum<'a>(
        &self,
        index_bucket: &'a BucketStorage<IndexBucket<T>>,
    ) -> SlotCountEnum<'a, T> {
        let enum_tag = self.get_enum_tag(index_bucket);
        let index_entry = index_bucket.get::<IndexEntry<T>>(self.ix);
        unsafe {
            match enum_tag {
                0 => SlotCountEnum::Free,
                1 => SlotCountEnum::ZeroSlots,
                2 => SlotCountEnum::OneSlotInIndex(&index_entry.contents.single_element),
                3 => SlotCountEnum::MultipleSlots(&index_entry.contents.multiple_slots),
                _ => {
                    panic!("unexpected value");
                }
            }
        }
    }

    pub(crate) fn get_multiple_slots_mut<'a>(
        &self,
        index_bucket: &'a mut BucketStorage<IndexBucket<T>>,
    ) -> Option<&'a mut MultipleSlots> {
        let enum_tag = self.get_enum_tag(index_bucket);
        unsafe {
            match enum_tag {
                3 => {
                    let index_entry = index_bucket.get_mut::<IndexEntry<T>>(self.ix);
                    Some(&mut index_entry.contents.multiple_slots)
                }
                _ => None,
            }
        }
    }

    pub(crate) fn set_slot_count_enum_value<'a>(
        &self,
        index_bucket: &'a mut BucketStorage<IndexBucket<T>>,
        value: SlotCountEnum<'a, T>,
    ) {
        let tag = match value {
            SlotCountEnum::Free => 0,
            SlotCountEnum::ZeroSlots => 1,
            SlotCountEnum::OneSlotInIndex(single_element) => {
                let index_entry = index_bucket.get_mut::<IndexEntry<T>>(self.ix);
                index_entry.contents.single_element = *single_element;
                2
            }
            SlotCountEnum::MultipleSlots(multiple_slots) => {
                let index_entry = index_bucket.get_mut::<IndexEntry<T>>(self.ix);
                index_entry.contents.multiple_slots = *multiple_slots;
                3
            }
        };
        self.set_enum_tag(index_bucket, tag);
    }

    pub fn init(&self, index_bucket: &mut BucketStorage<IndexBucket<T>>, pubkey: &Pubkey) {
        self.set_slot_count_enum_value(index_bucket, SlotCountEnum::ZeroSlots);
        let index_entry = index_bucket.get_mut::<IndexEntry<T>>(self.ix);
        index_entry.key = *pubkey;
    }

    pub fn ref_count(
        &self,
        index_bucket: &BucketStorage<IndexBucket<T>>,
        data_buckets: &[BucketStorage<DataBucket>],
    ) -> RefCount {
        match self.get_slot_count_enum(index_bucket) {
            SlotCountEnum::MultipleSlots(multiple_slots) => {
                let data_bucket_ix =
                    IndexEntry::<T>::data_bucket_from_num_slots(multiple_slots.num_slots);
                let data_bucket = &data_buckets[data_bucket_ix as usize];
                let loc = Self::data_loc(data_bucket, multiple_slots);
                assert!(!data_bucket.is_free(loc));
                let start = data_bucket.get_start_offset_with_header(loc);
                let header = BucketStorage::<DataBucket>::get_from_parts::<OccupiedHeader>(
                    &data_bucket.mmap[start..],
                );
                header.packed_ref_count.ref_count()
            }
            // unallocated slot list, so ref count 0 also
            SlotCountEnum::ZeroSlots => 0,
            // only entries in the data file are allowed to have a ref count other than 1
            _ => 1,
        }
    }

    /// This function maps the original data location into an index in the current bucket storage.
    /// This is coupled with how we resize bucket storages.
    pub(crate) fn data_loc(
        storage: &BucketStorage<DataBucket>,
        multiple_slots: &MultipleSlots,
    ) -> u64 {
        multiple_slots.storage_offset()
            << (storage.capacity_pow2 - multiple_slots.storage_capacity_when_created_pow2())
    }

    pub fn read_value<'a>(
        &self,
        index_bucket: &'a BucketStorage<IndexBucket<T>>,
        data_buckets: &'a [BucketStorage<DataBucket>],
    ) -> Option<(&'a [T], RefCount)> {
        let mut ref_count = 1;
        let data = match self.get_slot_count_enum(index_bucket) {
            SlotCountEnum::ZeroSlots => {
                // num_slots is 0. This means we don't have an actual allocation.
                &[]
            }
            SlotCountEnum::OneSlotInIndex(single_element) => {
                // only element is stored in the index entry
                std::slice::from_ref(single_element)
            }
            SlotCountEnum::MultipleSlots(multiple_slots) => {
                ref_count = self.ref_count(index_bucket, data_buckets);

                let data_bucket_ix =
                    IndexEntry::<T>::data_bucket_from_num_slots(multiple_slots.num_slots);
                let data_bucket = &data_buckets[data_bucket_ix as usize];
                let loc = Self::data_loc(data_bucket, multiple_slots);
                assert!(!data_bucket.is_free(loc));
                data_bucket.get_cell_slice::<T>(loc, multiple_slots.num_slots)
            }
            _ => {
                unimplemented!();
            }
        };
        Some((data, ref_count))
    }

    pub fn new(ix: u64) -> Self {
        Self {
            ix,
            _phantom: PhantomData,
        }
    }

    pub fn key<'a>(&self, index_bucket: &'a BucketStorage<IndexBucket<T>>) -> &'a Pubkey {
        let entry: &IndexEntry<T> = index_bucket.get(self.ix);
        &entry.key
    }

    pub fn set_ref_count2(
        &self,
        _index_bucket: &mut BucketStorage<IndexBucket<T>>,
        _ref_count: RefCount,
    ) {
        todo!(); /*
                 let index_entry = index_bucket.get_mut::<IndexEntry<T>>(self.ix);
                 index_entry
                     .packed_ref_count
                     .set_ref_count_checked(ref_count)
                     .expect("ref count must fit into 62 bits!")
                     */
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::{path::PathBuf, sync::Arc},
        tempfile::tempdir,
    };

    impl<T: Clone + Copy + Debug> IndexEntry<T> {
        pub fn new(key: Pubkey) -> Self {
            IndexEntry {
                key,
                contents: SingleElementOrMultipleSlots {
                    multiple_slots: MultipleSlots {
                        storage_cap_and_offset: PackedStorage::default(),
                        num_slots: 0,
                    },
                },
            }
        }
    }

    /// verify that accessors for storage_offset and capacity_when_created are
    /// correct and independent
    #[test]
    fn test_api() {
        for offset in [0, 1, u32::MAX as u64] {
            let mut multiple_slots = MultipleSlots::default();

            if offset != 0 {
                multiple_slots.set_storage_offset(offset);
            }
            assert_eq!(multiple_slots.storage_offset(), offset);
            assert_eq!(multiple_slots.storage_capacity_when_created_pow2(), 0);
            for pow in [1, 255, 0] {
                multiple_slots.set_storage_capacity_when_created_pow2(pow);
                assert_eq!(multiple_slots.storage_offset(), offset);
                assert_eq!(multiple_slots.storage_capacity_when_created_pow2(), pow);
            }
        }
    }

    #[test]
    fn test_size() {
        assert_eq!(std::mem::size_of::<PackedStorage>(), 1 + 7);
        assert_eq!(
            std::mem::size_of::<IndexEntry::<u64>>(),
            32 + (8 + 8).max(std::mem::size_of::<u64>())
        );
    }

    fn index_bucket_for_testing() -> BucketStorage<IndexBucket<u64>> {
        let tmpdir = tempdir().unwrap();
        let paths: Vec<PathBuf> = vec![tmpdir.path().to_path_buf()];
        assert!(!paths.is_empty());

        // `new` here creates a file in `tmpdir`. Once the file is created, `tmpdir` can be dropped without issue.
        BucketStorage::<IndexBucket<u64>>::new(
            Arc::new(paths),
            1,
            std::mem::size_of::<IndexEntry<u64>>() as u64,
            1,
            Arc::default(),
            Arc::default(),
        )
    }

    fn index_entry_for_testing() -> (
        BucketStorage<IndexBucket<u64>>,
        IndexEntryPlaceInBucket<u64>,
    ) {
        (index_bucket_for_testing(), IndexEntryPlaceInBucket::new(0))
    }

    #[test]
    #[should_panic(expected = "New storage offset must fit into 7 bytes!")]
    fn test_set_storage_offset_value_too_large() {
        let too_big = 1 << 56;
        let mut multiple_slots = MultipleSlots::default();
        multiple_slots.set_storage_offset(too_big);
    }

    /*
    #[test]
    #[should_panic(expected = "ref count must fit into 62 bits!")]
    fn test_set_ref_count_too_large() {
        let too_big = 1 << 63;
        let (mut index_bucket, index) = index_entry_for_testing();
        index.set_ref_count(&mut index_bucket, too_big);
    }
    */

    #[test]
    fn test_data_bucket_from_num_slots() {
        for n in 0..512 {
            assert_eq!(
                IndexEntry::<u64>::data_bucket_from_num_slots(n),
                (n as f64).log2().ceil() as u64
            );
        }
        assert_eq!(
            IndexEntry::<u64>::data_bucket_from_num_slots(u32::MAX as u64),
            32
        );
        assert_eq!(
            IndexEntry::<u64>::data_bucket_from_num_slots(u32::MAX as u64 + 1),
            32
        );
        assert_eq!(
            IndexEntry::<u64>::data_bucket_from_num_slots(u32::MAX as u64 + 2),
            33
        );
    }
}
