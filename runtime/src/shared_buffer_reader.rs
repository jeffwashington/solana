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
    },
};

type ABuffer = Arc<Vec<u8>>;

pub struct SharedBufferInternal {
    pub error: RwLock<std::io::Result<usize>>,
    pub bg_reader: Mutex<Option<JoinHandle<()>>>,
    pub bg_eof_reached: AtomicBool,
    pub stop: AtomicBool,

    pub clients: RwLock<Vec<usize>>, // keep track of the next read location per outstanding client

    // bg thread reads to 'newly_read_data'
    pub newly_read_data: RwLock<Vec<ABuffer>>,
    pub newly_read_data_signal: (Mutex<u8>, Condvar),

    // unpacking callers read from 'data'. newly_read_data is transferred to 'data when 'data' is exhausted.
    // This minimizes lock contention since bg file reader has to have almost constant write access.
    pub data: RwLock<Vec<ABuffer>>,

    pub buffers: RwLock<Vec<ABuffer>>,
    pub new_buffer_signal: (Mutex<u8>, Condvar),
    pub empty_buffer: ABuffer,
}

pub struct SharedBuffer {
    pub instance: Arc<SharedBufferInternal>,
}

impl SharedBuffer {
    pub fn new<T: 'static + Read + std::marker::Send>(reader: T) -> Self {
        let inner = SharedBufferInternal {
            newly_read_data: RwLock::new(vec![]),
            data: RwLock::new(vec![ABuffer::default()]), // initialize with 1 vector of empty data
            error: RwLock::new(Ok(0)),
            bg_reader: Mutex::new(None),
            bg_eof_reached: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            newly_read_data_signal: (Mutex::new(0), Condvar::new()),
            new_buffer_signal: (Mutex::new(0), Condvar::new()),
            clients: RwLock::new(vec![]),
            buffers: RwLock::new(Self::alloc_initial_vectors()),
            empty_buffer: ABuffer::default(),
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
    fn alloc_initial_vectors() -> Vec<ABuffer> {
        let initial_vector_count = TOTAL_BUFFER_BUDGET / CHUNK_SIZE;
        (0..initial_vector_count)
            .into_iter()
            .map(|_| Arc::new(vec![0u8; CHUNK_SIZE]))
            .collect()
    }
}

pub struct SharedBufferReader {
    pub instance: Arc<SharedBufferInternal>,
    pub last_buffer_index: usize,
    pub last_index_within_last_buffer: usize,
    pub my_client_index: usize,
    pub current_data: ABuffer,
    pub empty_buffer: ABuffer,
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

const TOTAL_BUFFER_BUDGET: usize = 2_000_000_000;
const CHUNK_SIZE: usize = 100_000_000;
const MAX_READ_SIZE: usize = 100_000_000;

impl SharedBufferInternal {
    fn read_entire_file_in_bg<T: 'static + Read + std::marker::Send>(&self, mut reader: T) {
        let now = std::time::Instant::now();
        let (_lock, cvar) = &self.newly_read_data_signal;
        let mut read = 0;

        let mut largest = 0;
        let mut wait = 0;
        let mut total_bytes = 0;
        'outer: loop {
            if self.stop.load(Ordering::Relaxed) {
                self.set_error(std::io::Error::from(std::io::ErrorKind::TimedOut));
                info!("stopped before file reading was finished");
                break;
            }

            let mut dest_data;
            let read_this_much;
            let mut buffers = self.buffers.write().unwrap();
            let buffer = buffers.pop();
            drop(buffers);
            match buffer {
                Some(buffer) => {
                    dest_data = buffer;
                    read_this_much = dest_data.len();
                    // assert that this should not result in a vector copy
                    assert_eq!(Arc::strong_count(&dest_data), 1);
                }
                None => {
                    let mut wait_for_new_buffer = Measure::start("wait_for_new_buffer");
                    // none available, so wait
                    self.wait_for_new_buffer();
                    wait_for_new_buffer.stop();
                    wait += wait_for_new_buffer.as_us();
                    continue; // check stop, try to get a buffer again
                }
            }

            let mut read_so_far = 0;
            let mut end = false;
            // call underlying reader to fill our buffer
            let target = Arc::make_mut(&mut dest_data);

            loop {
                let read_end = std::cmp::min(read_so_far + MAX_READ_SIZE, read_this_much);
                if read_end == read_so_far {
                    break;
                }
                let mut time_read = Measure::start("read");
                let result = reader.read(&mut target[read_so_far..]);
                time_read.stop();
                read += time_read.as_us();
                match result {
                    Ok(size) => {
                        if size == 0 {
                            end = true;
                            break;
                        }
                        total_bytes += size;

                        // keep track of the largest single read we got from the decompressor
                        largest = std::cmp::max(largest, size);

                        // loop to read some more
                        read_so_far += size;
                    }
                    Err(err) => {
                        self.set_error(err);
                        break 'outer;
                    }
                }
            }

            if read_so_far > 0 {
                let mut data = self.newly_read_data.write().unwrap();
                data.push(dest_data);
                drop(data);
                cvar.notify_all(); // notify after data added
            }

            if end {
                self.bg_eof_reached.store(true, Ordering::Relaxed);
                cvar.notify_all(); // notify after read complete is set
                break 'outer;
            }
        }

        info!(
            "reading entire decompressed file took: {} us, bytes: {}, read_us: {}, waiting_for_buffer_us: {}, largest fetch: {}, error: {:?}",
            now.elapsed().as_micros(),
            total_bytes,
            read,
            wait,
            largest,
            self.error.read().unwrap()
        );
    }
    fn set_error(&self, error: std::io::Error) {
        *self.error.write().unwrap() = Err(error);
        let (_lock, cvar) = &self.newly_read_data_signal;
        cvar.notify_all(); // notify after error is set
    }
    fn wait_for_new_buffer(&self) -> bool {
        let (lock, cvar) = &self.new_buffer_signal;
        let data = lock.lock().unwrap();
        let res = cvar
            .wait_timeout(data, std::time::Duration::from_millis(1000))
            .unwrap();
        if res.1.timed_out() {
            return true;
        }
        false
    }
    fn wait_for_newly_read_data(&self) -> bool {
        let (lock, cvar) = &self.newly_read_data_signal;
        let data = lock.lock().unwrap();
        let res = cvar
            .wait_timeout(data, std::time::Duration::from_millis(1000))
            .unwrap();
        if res.1.timed_out() {
            return true;
        }
        false
    }
    fn transfer_data_from_bg(&self) -> bool {
        let mut from_lock = self.newly_read_data.write().unwrap();
        if from_lock.is_empty() {
            return false;
        }
        let mut newly_read_data: Vec<ABuffer> = vec![];
        std::mem::swap(&mut *from_lock, &mut newly_read_data);
        drop(from_lock);
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
        let my_client_index;
        {
            let mut list = original_instance.clients.write().unwrap();
            my_client_index = list.len();
            list.push(last_buffer_index);
        }

        let instance = Arc::clone(original_instance);
        Self {
            instance,
            my_client_index,
            last_buffer_index,
            last_index_within_last_buffer: 0,
            // startup condition for our local reference to the buffer we want to read from.
            // data[0] will always exist. It will be empty, But that is ok. Corresponds to last_buffer_index initial value of 0.
            current_data: original_instance.data.read().unwrap()[0].clone(),
            empty_buffer: original_instance.empty_buffer.clone(),
        }
    }
    fn client_done_reading(&mut self) {
        self.update_client_index(usize::MAX); // has the effect of causing the buffer to never again wait on this reader's progress
    }

    fn update_client_index(&mut self, last_buffer_index: usize) {
        let previous_last_buffer_index = self.last_buffer_index;
        self.last_buffer_index = last_buffer_index;
        let client_index = self.my_client_index;
        let mut indices = self.instance.clients.write().unwrap();
        indices[client_index] = last_buffer_index;
        drop(indices);
        let indices = self.instance.clients.read().unwrap();
        let mut new_min = *indices.iter().min().unwrap();
        new_min = std::cmp::min(new_min, self.instance.data.read().unwrap().len());
        //error!("update_client_index: {}, {}, new min is: {}, sizes: {:?}", self.my_client_index, last_buffer_index, new_min, *self.instance.clients.read().unwrap());
        drop(indices);
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
                    let (_lock, cvar) = &self.instance.new_buffer_signal;
                    cvar.notify_all(); // new buffer available
                }
            }
        }
    }
}

impl Read for SharedBufferReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let request_len = buf.len();
        let mut remaining_request = request_len;
        let mut offset_in_dest = 0;
        let mut source = &*self.current_data;
        let full_len = source.len();
        let remaining_len = full_len - self.last_index_within_last_buffer;
        let bytes_to_transfer = std::cmp::min(remaining_len, request_len);

        // copy what we can
        buf[offset_in_dest..(offset_in_dest + bytes_to_transfer)].copy_from_slice(
            &source[self.last_index_within_last_buffer
                ..(self.last_index_within_last_buffer + bytes_to_transfer)],
        );
        self.last_index_within_last_buffer += bytes_to_transfer;
        offset_in_dest += bytes_to_transfer;
        remaining_request -= bytes_to_transfer;

        let mut eof_seen = false;
        while remaining_request > 0 {
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
                let remaining_len = full_len - self.last_index_within_last_buffer;

                let bytes_to_transfer = std::cmp::min(remaining_request, remaining_len);

                // copy what we can
                buf[offset_in_dest..(offset_in_dest + bytes_to_transfer)].copy_from_slice(
                    &source[self.last_index_within_last_buffer
                        ..(self.last_index_within_last_buffer + bytes_to_transfer)],
                );
                self.last_index_within_last_buffer += bytes_to_transfer;
                offset_in_dest += bytes_to_transfer;
                remaining_request -= bytes_to_transfer;

                if remaining_request == 0 {
                    break;
                }
                self.current_data = self.empty_buffer.clone(); // we have exhausted this buffer, unref it so it can be recycled without copy
                self.last_index_within_last_buffer = 0;
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
