//! File I/O buffered reader for AppendVec
use {
    crate::{append_vec::ValidSlice, file_io::read_more_buffer},
    std::{fs::File, ops::Range},
};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum BufferedReaderStatus {
    Eof,
    Success,
}

/// read a file a large buffer at a time and provide access to a slice in that buffer
pub struct BufferedReader<'a> {
    /// when we are next asked to read from file, start at this offset
    file_offset_of_next_read: usize,
    /// the most recently read data. `buf_valid_bytes` specifies the range of `buf` that is valid.
    buf: Box<[u8]>,
    /// specifies the range of `buf` that contains valid data that has not been used by the caller
    buf_valid_bytes: Range<usize>,
    /// offset in the file of the `buf_valid_bytes`.`start`
    file_last_offset: usize,
    /// how many contiguous bytes caller needs
    read_requirements: Option<usize>,
    /// how many bytes are valid in the file. The file's len may be longer.
    file_len_valid: usize,
    /// reference to file handle
    file: &'a File,
    /// we always want at least this many contiguous bytes available or we must read more into the buffer.
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
            file_offset_of_next_read: 0,
            buf: vec![0; buffer_size].into_boxed_slice(),
            buf_valid_bytes: 0..0,
            file_last_offset: 0,
            read_requirements: None,
            file_len_valid,
            file,
            default_min_read_requirement,
        }
    }
    /// read to make sure we have the minimum amount of data
    pub fn read(&mut self) -> std::io::Result<BufferedReaderStatus> {
        let must_read = self
            .read_requirements
            .unwrap_or(self.default_min_read_requirement);
        if self.buf_valid_bytes.len() < must_read {
            // we haven't used all the bytes we read last time, so adjust the effective offset
            self.file_last_offset = self.file_offset_of_next_read - self.buf_valid_bytes.len();
            read_more_buffer(
                self.file,
                &mut self.file_offset_of_next_read,
                &mut self.buf,
                &mut self.buf_valid_bytes,
                self.file_len_valid,
            )?;
            if self.buf_valid_bytes.len() < must_read {
                return Ok(BufferedReaderStatus::Eof);
            }
        }
        // reset this once we have checked that we had this much data once
        self.read_requirements = None;
        Ok(BufferedReaderStatus::Success)
    }
    /// return the biggest slice of valid data starting at the current offset
    fn get_data(&'a self) -> ValidSlice<'a> {
        ValidSlice::new(&self.buf[self.buf_valid_bytes.clone()])
    }
    /// return offset within `file` of start of read at current offset
    pub fn get_data_and_offset(&'a self) -> (usize, ValidSlice<'a>) {
        (
            self.file_last_offset + self.buf_valid_bytes.start,
            self.get_data(),
        )
    }
    /// advance the offset of where to read next by `delta`
    pub fn advance_offset(&mut self, delta: usize) {
        if self.buf_valid_bytes.len() >= delta {
            self.buf_valid_bytes.start += delta;
        } else {
            let additional_amount_to_skip = delta - self.buf_valid_bytes.len();
            self.buf_valid_bytes = 0..0;
            self.file_offset_of_next_read += additional_amount_to_skip;
        }
    }
    /// specify the amount of data required to read next time `read` is called
    pub fn set_required_data_len(&mut self, len: usize) {
        self.read_requirements = Some(len);
    }
}
