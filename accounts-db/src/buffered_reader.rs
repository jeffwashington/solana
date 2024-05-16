//! File I/O buffered reader for AppendVec
use {
    crate::{append_vec::ValidSlice, file_io::read_more_buffer},
    std::{fs::File, ops::Range},
};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum BufferedReaderResult {
    Eof,
    Success,
}

/// read a file a large buffer at a time and provide access to a slice in that buffer
pub struct BufferedReader<'a> {
    offset_of_next_read: usize,
    buf: Vec<u8>,
    valid_bytes: Range<usize>,
    last_offset: usize,
    read_requirements: Option<usize>,
    file_len_valid: usize,
    file: &'a File,
    default_min_read_requirement: usize,
}

impl<'a> BufferedReader<'a> {
    /// `buffer_size`: how much to try to read at a time
    /// `file_len_valid`: # bytes that are valid in the file, may be less then overall file len
    /// `default_min_read_requirement`: make sure we always have this much data available if we're asked to read
    pub fn new(
        buffer_size: usize,
        file_len_valid: usize,
        file: &'a File,
        default_min_read_requirement: usize,
    ) -> Self {
        let buffer_size = buffer_size.min(file_len_valid);
        Self {
            offset_of_next_read: 0,
            buf: (0..buffer_size).map(|_| 0).collect::<Vec<_>>(),
            valid_bytes: 0..0,
            last_offset: 0,
            read_requirements: None,
            file_len_valid,
            file,
            default_min_read_requirement,
        }
    }
    /// read to make sure we have the minimum amount of data
    pub fn read(&mut self) -> std::io::Result<BufferedReaderResult> {
        let must_read = self
            .read_requirements
            .unwrap_or(self.default_min_read_requirement);
        if self.valid_bytes.len() < must_read {
            // we haven't used all the bytes we read last time, so adjust the effective offset
            self.last_offset = self.offset_of_next_read - self.valid_bytes.len();
            read_more_buffer(
                self.file,
                &mut self.offset_of_next_read,
                &mut self.buf,
                &mut self.valid_bytes,
                self.file_len_valid,
            )?;
            if self.valid_bytes.len() < must_read {
                return Ok(BufferedReaderResult::Eof);
            }
        }
        // reset this once we have checked that we had this much data once
        self.read_requirements = None;
        Ok(BufferedReaderResult::Success)
    }
    /// return the biggest slice of valid data starting at the current offset
    fn get_data(&'a self) -> ValidSlice<'a> {
        ValidSlice::new(&self.buf[self.valid_bytes.clone()])
    }
    /// return offset within `file` of start of read at current offset
    pub fn get_data_and_offset(&'a self) -> (usize, ValidSlice<'a>) {
        (self.last_offset + self.valid_bytes.start, self.get_data())
    }
    /// advance the offset of where to read next by `delta`
    pub fn advance_offset(&mut self, delta: usize) {
        if self.valid_bytes.len() >= delta {
            self.valid_bytes.start += delta;
        } else {
            let additional_amount_to_skip = delta - self.valid_bytes.len();
            self.valid_bytes = 0..0;
            self.offset_of_next_read += additional_amount_to_skip;
        }
    }
    /// specify the amount of data required to read next time `read` is called
    pub fn set_required_data_len(&mut self, len: usize) {
        self.read_requirements = Some(len);
    }
}
