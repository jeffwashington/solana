//! File i/o helper functions.
#[cfg(unix)]
use std::os::unix::prelude::FileExt;
use std::{fs::File, ops::Range};

/// `buffer` contains `valid_bytes` of data at its end.
/// Move those valid bytes to the beginning of `buffer`, then read from `offset` to fill the rest of `buffer`.
/// Update `offset` for the next read and update `valid_bytes` to specify valid portion of `buffer`.
/// `valid_len` is # of valid bytes in the file. This may be <= file length.
pub fn read_more_buffer(
    file: &File,
    offset: &mut usize,
    buffer: &mut [u8],
    valid_bytes: &mut Range<usize>,
    valid_len: usize,
) -> std::io::Result<()> {
    // copy remainder of `valid_bytes` into beginning of `buffer`.
    // These bytes were left over from the last read but weren't enough for the next desired contiguous read chunk.
    buffer.copy_within(valid_bytes.clone(), 0);

    // read the rest of `buffer`
    let bytes_read = read_buffer(file, *offset, &mut buffer[valid_bytes.len()..], valid_len)?;
    *offset += bytes_read;
    *valid_bytes = 0..(valid_bytes.len() + bytes_read);

    Ok(())
}

#[cfg(unix)]
/// Read, starting at `start_offset`, until `buffer` is full or we read past `valid_len`/eof.
/// `valid_len` is # of valid bytes in the file. This may be <= file length.
/// return # bytes read
pub fn read_buffer(
    file: &File,
    start_offset: usize,
    buffer: &mut [u8],
    valid_len: usize,
) -> std::io::Result<usize> {
    let mut offset = start_offset;
    let mut start_read = 0;
    let mut total_bytes_read = 0;
    if start_offset >= valid_len {
        return Ok(0);
    }

    while start_read < buffer.len() {
        let bytes_read_this_time = file.read_at(&mut buffer[start_read..], offset as u64)?;
        total_bytes_read += bytes_read_this_time;
        if total_bytes_read + start_offset >= valid_len {
            total_bytes_read -= (total_bytes_read + start_offset) - valid_len;
            // we've read all there is in the file
            break;
        }
        // There is possibly more to read. `read_at` may have returned partial results, so prepare to loop and read again.
        start_read += bytes_read_this_time;
        offset += bytes_read_this_time;
    }
    Ok(total_bytes_read)
}

#[cfg(not(unix))]
/// this cannot be supported if we're not on unix-os
pub fn read_buffer(
    _file: &File,
    _start_offset: usize,
    _buffer: &mut [u8],
    _valid_len: usize,
) -> std::io::Result<usize> {
    panic!("unimplemented");
}
