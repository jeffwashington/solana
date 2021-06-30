//! SharedBuffer is given a Reader and SharedBufferReader implements the Reader trait.
//! SharedBuffer reads ahead in the underlying file and saves the data.
//! SharedBufferReaders can be created for the buffer and independently keep track of each reader's read location.
//! The background reader keeps track of the progress of each client. After data has been read by all readers,
//!  the buffer is recycled and reading ahead continues.
//! A primary use case is the underlying reader being decompressing a file, which can be computationally expensive.
//! The clients of SharedBufferReaders could be parallel instances which need access to the decompressed data.
use {
    log::*,
    solana_measure::measure::Measure,
    std::{
        io::*,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Condvar, Mutex, RwLock,
        },
        thread::{Builder, JoinHandle},
        time::Duration,
    },
};

// encapsulate complications of 'unneeded' mutex and Condvar
#[derive(Default, Debug)]
struct WaitableCondvar {
    pub mutex: Mutex<u8>,
    pub event: Condvar,
}

impl WaitableCondvar {
    pub fn notify_all(&self) {
        self.event.notify_all();
    }
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let lock = self.mutex.lock().unwrap();
        let res = self.event.wait_timeout(lock, timeout).unwrap();
        if res.1.timed_out() {
            return true;
        }
        false
    }
}

// tunable parameters:
// # bytes allocated and populated by reading ahead
const TOTAL_BUFFER_BUDGET: usize = 2_000_000_000;
// data is read-ahead and saved in chunks of this many bytes
const CHUNK_SIZE: usize = 100_000_000;

type OneSharedBuffer = Arc<Vec<u8>>;

struct SharedBufferInternal {
    // error encountered during read
    error: RwLock<std::io::Result<usize>>,
    bg_reader: Mutex<Option<JoinHandle<()>>>,
    bg_eof_reached: AtomicBool,
    stop: AtomicBool,

    // Keep track of the next read location per outstanding client.
    // index is client's my_client_index.
    // Value at index is index into buffers where that client is currently reading.
    // Any buffer at index < min(clients) can be recycled or destroyed.
    clients: RwLock<Vec<usize>>,

    // bg thread reads to 'newly_read_data' and signals
    newly_read_data: RwLock<Vec<OneSharedBuffer>>,
    // set when newly_read_data gets new data written to it and can be transferred
    newly_read_data_signal: WaitableCondvar,
    // unpacking callers read from 'data'. newly_read_data is transferred to 'data when 'data' is exhausted.
    // This minimizes lock contention since bg file reader has to have almost constant write access.
    data: RwLock<Vec<OneSharedBuffer>>,

    // currently available set of buffers for bg to read into
    // during operation, this is exhausted as the bg reads ahead
    // As all clients are done with an earlier buffer, it is recycled by being put back into this vec for the bg thread to pull out.
    buffers: RwLock<Vec<OneSharedBuffer>>,
    // signaled when a new buffer is added to buffers. This throttles the bg reading.
    new_buffer_signal: WaitableCondvar,

    // it is convenient to have one of these around
    empty_buffer: OneSharedBuffer,
}

pub struct SharedBuffer {
    instance: Arc<SharedBufferInternal>,
}

impl SharedBuffer {
    pub fn new<T: 'static + Read + std::marker::Send>(reader: T) -> Self {
        let inner = SharedBufferInternal {
            newly_read_data: RwLock::new(vec![]),
            data: RwLock::new(vec![OneSharedBuffer::default()]), // initialize with 1 vector of empty data
            error: RwLock::new(Ok(0)),
            bg_reader: Mutex::new(None),
            bg_eof_reached: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            newly_read_data_signal: WaitableCondvar::default(),
            new_buffer_signal: WaitableCondvar::default(),
            clients: RwLock::new(vec![]),
            buffers: RwLock::new(Self::alloc_initial_vectors()),
            empty_buffer: OneSharedBuffer::default(),
        };
        let instance = Arc::new(inner);
        let instance_ = instance.clone();

        let handle = Builder::new()
            .name("solana-compressed_file_reader".to_string())
            .spawn(move || {
                instance_.read_entire_file_in_bg(reader);
            });
        *instance.bg_reader.lock().unwrap() = Some(handle.unwrap());
        Self { instance }
    }
    fn alloc_initial_vectors() -> Vec<OneSharedBuffer> {
        let initial_vector_count = TOTAL_BUFFER_BUDGET / CHUNK_SIZE;
        (0..initial_vector_count)
            .into_iter()
            .map(|_| Arc::new(vec![0u8; CHUNK_SIZE]))
            .collect()
    }
}

pub struct SharedBufferReader {
    instance: Arc<SharedBufferInternal>,
    my_client_index: usize,
    // index in 'instance' of the current buffer this reader is reading from.
    // The current buffer is referenced from 'current_data'.
    // Until we exhaust this buffer, we don't need to get a lock to read from this.
    last_buffer_index: usize,
    // the index within current_data where we will next read
    index_in_current_data: usize,
    current_data: OneSharedBuffer,

    // convenient to have access to
    empty_buffer: OneSharedBuffer,
}

impl Drop for SharedBufferInternal {
    fn drop(&mut self) {
        if let Some(handle) = self.bg_reader.lock().unwrap().take() {
            self.stop.store(true, Ordering::Relaxed);
            handle.join().unwrap();
        }
    }
}

impl Drop for SharedBufferReader {
    fn drop(&mut self) {
        self.client_done_reading();
    }
}

impl SharedBufferInternal {
    fn read_entire_file_in_bg<T: 'static + Read + std::marker::Send>(&self, mut reader: T) {
        let now = std::time::Instant::now();
        let mut read_us = 0;

        let mut max_bytes_read = 0;
        let mut wait_us = 0;
        let mut total_bytes = 0;
        'outer: loop {
            if self.stop.load(Ordering::Relaxed) {
                self.set_error(std::io::Error::from(std::io::ErrorKind::TimedOut));
                break;
            }

            let mut dest_data;
            let buffer_size;
            let mut buffers = self.buffers.write().unwrap();
            let buffer = buffers.pop();
            drop(buffers);
            match buffer {
                Some(buffer) => {
                    dest_data = buffer;
                    buffer_size = dest_data.len();
                    // assert that this should not result in a vector copy
                    // These are internal buffers and should not be held by anyone else.
                    assert_eq!(Arc::strong_count(&dest_data), 1);
                }
                None => {
                    let mut wait_for_new_buffer = Measure::start("wait_for_new_buffer");
                    // none available, so wait_us
                    self.wait_for_new_buffer();
                    wait_for_new_buffer.stop();
                    wait_us += wait_for_new_buffer.as_us();
                    continue; // check stop, try to get a buffer again
                }
            }

            let mut bytes_read = 0;
            let mut end = false;
            let target = Arc::make_mut(&mut dest_data);

            while bytes_read < buffer_size {
                let mut time_read = Measure::start("read");
                // Read from underlying reader into the next spot in the target buffer.
                // Note that this read takes less time (up to 2x) if we read into the same static buffer location each call.
                // But, we have to copy the data out later, so we choose to pay the price at read time to put the data where it is useful.
                let result = reader.read(&mut target[bytes_read..]);
                time_read.stop();
                read_us += time_read.as_us();
                match result {
                    Ok(size) => {
                        if size == 0 {
                            end = true;
                            break;
                        }
                        total_bytes += size;
                        max_bytes_read = std::cmp::max(max_bytes_read, size);
                        bytes_read += size;
                        // loop to read some more
                    }
                    Err(err) => {
                        self.set_error(err);
                        break 'outer;
                    }
                }
            }

            if bytes_read > 0 {
                // store this buffer in the bg buffer list
                let mut data = self.newly_read_data.write().unwrap();
                data.push(dest_data);
                drop(data);
                self.newly_read_data_signal.notify_all();
            }

            if end {
                self.bg_eof_reached.store(true, Ordering::Relaxed);
                self.newly_read_data_signal.notify_all();
                break 'outer;
            }
        }

        info!(
            "reading entire decompressed file took: {} us, bytes: {}, read_us: {}, waiting_for_buffer_us: {}, largest fetch: {}, error: {:?}",
            now.elapsed().as_micros(),
            total_bytes,
            read_us,
            wait_us,
            max_bytes_read,
            self.error.read().unwrap()
        );
    }
    fn set_error(&self, error: std::io::Error) {
        *self.error.write().unwrap() = Err(error);
        self.newly_read_data_signal.notify_all();
    }
    fn default_wait_timeout() -> Duration {
        Duration::from_millis(1000)
    }
    fn wait_for_new_buffer(&self) -> bool {
        self.new_buffer_signal
            .wait_timeout(Self::default_wait_timeout())
    }
    fn wait_for_newly_read_data(&self) -> bool {
        self.newly_read_data_signal
            .wait_timeout(Self::default_wait_timeout())
    }
    fn transfer_data_from_bg(&self) -> bool {
        let mut bg_lock = self.newly_read_data.write().unwrap();
        if bg_lock.is_empty() {
            // no data available
            return false;
        }
        let mut newly_read_data: Vec<OneSharedBuffer> = vec![];
        std::mem::swap(&mut *bg_lock, &mut newly_read_data);
        drop(bg_lock);
        let mut to_lock = self.data.write().unwrap();
        to_lock.append(&mut newly_read_data);
        true
    }
    fn has_reached_eof(&self) -> bool {
        self.bg_eof_reached.load(Ordering::Relaxed)
    }
}

impl SharedBufferReader {
    pub fn new(original_instance: &SharedBuffer) -> Self {
        let original_instance = &original_instance.instance;
        let last_buffer_index = 0;
        let mut list = original_instance.clients.write().unwrap();
        let my_client_index = list.len();
        if my_client_index > 0 {
            let current_min = list.iter().min().unwrap();
            if current_min > &0 {
                panic!("SharedBufferReaders must all be created before the first one reads");
            }
        }
        list.push(last_buffer_index);
        drop(list);

        Self {
            instance: Arc::clone(original_instance),
            my_client_index,
            last_buffer_index,
            index_in_current_data: 0,
            // startup condition for our local reference to the buffer we want to read from.
            // data[0] will always exist. It will be empty, But that is ok. Corresponds to last_buffer_index initial value of 0.
            current_data: original_instance.data.read().unwrap()[0].clone(),
            empty_buffer: original_instance.empty_buffer.clone(),
        }
    }
    fn client_done_reading(&mut self) {
        // has the effect of causing nobody to ever again wait on this reader's progress
        self.update_client_index(usize::MAX);
    }

    fn update_client_index(&mut self, last_buffer_index: usize) {
        let previous_last_buffer_index = self.last_buffer_index;
        self.last_buffer_index = last_buffer_index;
        let client_index = self.my_client_index;
        let mut indexes = self.instance.clients.write().unwrap();
        indexes[client_index] = last_buffer_index;
        drop(indexes);
        let mut new_min = *self.instance.clients.read().unwrap().iter().min().unwrap();
        // if new_min == usize::MAX, then every caller is done reading
        new_min = std::cmp::min(new_min, self.instance.data.read().unwrap().len());
        if new_min > previous_last_buffer_index {
            let eof = self.instance.has_reached_eof();

            for recycle in previous_last_buffer_index..new_min {
                //error!("recycling: {}", recycle);
                let mut remove = self.empty_buffer.clone();
                let mut data = self.instance.data.write().unwrap();
                std::mem::swap(&mut remove, &mut data[recycle]);
                drop(data);
                if remove.len() < CHUNK_SIZE {
                    // swapped out buffer will be destroyed
                    continue; // another thread beat us or we have initial small buffers, so ignore these
                }

                if !eof {
                    // if !eof, recycle this buffer and notify waiting reader(s)
                    // if eof, just drop buffer this buffer since it isn't needed for reading anymore
                    self.instance.buffers.write().unwrap().push(remove);
                    self.instance.new_buffer_signal.notify_all(); // new buffer available
                }
            }
        }
    }
}

impl Read for SharedBufferReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let dest_len = buf.len();
        let mut remaining_dest = dest_len;
        let mut offset_in_dest = 0;
        let mut source = &*self.current_data;
        let full_len = source.len();
        let remaining_len = full_len - self.index_in_current_data;
        let bytes_to_transfer = std::cmp::min(remaining_len, dest_len);

        // copy what we can
        buf[offset_in_dest..(offset_in_dest + bytes_to_transfer)].copy_from_slice(
            &source[self.index_in_current_data..(self.index_in_current_data + bytes_to_transfer)],
        );
        self.index_in_current_data += bytes_to_transfer;
        offset_in_dest += bytes_to_transfer;
        remaining_dest -= bytes_to_transfer;

        let mut eof_seen = false;
        while remaining_dest > 0 {
            let mut good = true;
            if source.is_empty() {
                let instance = &*self.instance;
                let lock = instance.data.read().unwrap();
                if self.last_buffer_index < lock.len() {
                    self.current_data = Arc::clone(&lock[self.last_buffer_index]);
                    source = &*self.current_data;
                } else {
                    good = false;
                }
            }
            if good {
                let full_len = source.len();
                let remaining_len = full_len - self.index_in_current_data;

                let bytes_to_transfer = std::cmp::min(remaining_dest, remaining_len);

                // copy what we can
                buf[offset_in_dest..(offset_in_dest + bytes_to_transfer)].copy_from_slice(
                    &source[self.index_in_current_data
                        ..(self.index_in_current_data + bytes_to_transfer)],
                );
                self.index_in_current_data += bytes_to_transfer;
                offset_in_dest += bytes_to_transfer;
                remaining_dest -= bytes_to_transfer;

                if remaining_dest == 0 {
                    break;
                }
                self.current_data = self.empty_buffer.clone(); // we have exhausted this buffer, unref it so it can be recycled without copy
                self.index_in_current_data = 0;
                self.update_client_index(self.last_buffer_index + 1);
                source = &*self.empty_buffer;
            }

            let instance = &*self.instance;
            let lock = instance.data.read().unwrap();
            if self.last_buffer_index >= lock.len() {
                drop(lock);

                if self.instance.transfer_data_from_bg() {
                    continue;
                }

                // another thread may have transferred the data
                if self.last_buffer_index >= instance.data.read().unwrap().len() {
                    continue;
                }

                if eof_seen {
                    break; // eof reached, and we have had a chance to read all data that was buffered
                }

                // no data, we could not transfer, and still no data, so check for eof.
                // If we got an eof, then we have to check again to make sure there isn't data now that we may have to transfer or not.
                if instance.has_reached_eof() {
                    eof_seen = true;
                    continue;
                }

                {
                    let error = instance.error.read().unwrap();
                    if error.is_err() {
                        drop(error);
                        let mut error = instance.error.write().unwrap();
                        let mut stored_error = Ok(0);
                        std::mem::swap(&mut *error, &mut stored_error);
                        drop(error);
                        return stored_error;
                    }
                }
                // no data to transfer, and file not finished, so wait
                instance.wait_for_newly_read_data();
                continue;
            }
        }
        Ok(offset_in_dest)
    }
}
