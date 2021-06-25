use {
    crossbeam_channel::{
        unbounded, RecvTimeoutError,
    },
    log::*,
    solana_measure::measure::Measure,
    std::{
        io::*,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Condvar, Mutex, RwLock,
        },
        thread::{Builder, JoinHandle},
    },
};

pub struct SeekableBufferingReaderInner {
    // unpacking callers read from 'data'. Data is transferred when 'data' is exhausted.
    // This minimizes lock contention since bg file reader has to have almost constant write access.
    pub data: RwLock<Vec<Vec<u8>>>,
    // bg thread reads to 'new_data'
    pub new_data: RwLock<Vec<Vec<u8>>>,
    pub len: AtomicUsize,
    pub calls: AtomicUsize,
    pub error: RwLock<std::io::Result<usize>>,
    pub bg_reader: Mutex<Option<JoinHandle<()>>>,
    pub file_read_complete: AtomicBool,
    pub stop: AtomicBool,
    pub new_data_signal: (Mutex<bool>, Condvar),
}

pub struct SeekableBufferingReader {
    pub instance: Arc<SeekableBufferingReaderInner>,
    pub pos: usize,
    pub last_buffer_index: usize,
    pub next_index_within_last_buffer: usize,
}

impl Clone for SeekableBufferingReader {
    fn clone(&self) -> Self {
        Self {
            instance: Arc::clone(&self.instance),
            pos: 0,
            last_buffer_index: 0,
            next_index_within_last_buffer: 0,
        }
    }
}

impl Drop for SeekableBufferingReaderInner {
    fn drop(&mut self) {
        if let Some(handle) = self.bg_reader.lock().unwrap().take() {
            self.stop.store(true, Ordering::Relaxed);
            handle.join().unwrap();
        }
    }
}

impl SeekableBufferingReader {
    pub fn new<T: 'static + Read + std::marker::Send>(reader: T) -> Self {
        let inner = SeekableBufferingReaderInner {
            new_data: RwLock::new(vec![]),
            data: RwLock::new(vec![]),
            len: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
            error: RwLock::new(Ok(0)),
            bg_reader: Mutex::new(None),
            file_read_complete: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            new_data_signal: (Mutex::new(false), Condvar::new()),
        };
        let result = Self {
            instance: Arc::new(inner),
            pos: 0,
            last_buffer_index: 0,
            next_index_within_last_buffer: 0,
        };

        let result_ = result.clone();

        let handle = Builder::new()
            .name("solana-compressed_file_reader".to_string())
            .spawn(move || {
                result_.read_entire_file_in_bg(reader);
            });
        *result.instance.bg_reader.lock().unwrap() = Some(handle.unwrap()); // TODO - unwrap here - do we expect to fail creating a thread? If we do, probably a fatal error anyway.
        std::thread::sleep(std::time::Duration::from_millis(200)); // hack: give time for file to be read a little bit
        result
    }
    fn read_entire_file_in_bg<T: 'static + Read + std::marker::Send>(&self, mut reader: T) {
        let mut time = Measure::start("");
        const CHUNK_SIZE: usize = 65536 * 2;
        let (_lock, cvar) = &self.instance.new_data_signal;
        let mut notify = 0;
        let mut read = 0;
        let mut allocate = 0;
        let notify_all = || {
            let mut time_notify = Measure::start("notify");
            cvar.notify_all();
            time_notify.stop();
            time_notify.as_us()
        };

        let (sender, receiver) = unbounded();

        let request = Arc::new((Mutex::new(false), Condvar::new()));
        let request_ = request.clone();

        let handle = Builder::new()
            .name("solana-buf_allocator".to_string())
            .spawn(move || {
                'outer: loop {
                    let max_bins = 100;
                    for _b in 0..max_bins {
                        let mut v = Vec::with_capacity(CHUNK_SIZE);
                        unsafe {
                            v.set_len(CHUNK_SIZE);
                        }
                        let _ = sender.send(v);
                    }
                    error!("sent {} vecs", max_bins);

                    loop {
                        // wait for a request for more
                        let lock = request_.0.lock().unwrap();
                        let lock = request_.1.wait(lock).unwrap();
                        if *lock {
                            break 'outer;
                        }
                        break;
                    }
                }
                error!("done making vecs");
            });

        let mut chunk_index = 0;
        let mut total_len = 0;
        'outer: loop {
            if self.instance.stop.load(Ordering::Relaxed) {
                self.set_error(std::io::Error::from(std::io::ErrorKind::TimedOut));
                info!("stopped before file reading was finished");
                break;
            }

            let mut dest_data;
            let mut timeout_us = 0;
            let mut attempts = 0;
            let mut m = Measure::start("");
            loop {
                let data = receiver.recv_timeout(std::time::Duration::from_micros(timeout_us));
                match data {
                    Err(RecvTimeoutError::Disconnected) => break 'outer,
                    Err(RecvTimeoutError::Timeout) => {
                        request.1.notify_one();
                        attempts += 1;
                        timeout_us = 100; // we can wait longer now that we requested
                        if attempts % 1000 == 0 {
                            error!("attempts to get new vecs: {}", attempts);
                        }
                    }
                    Ok(new_buffer) => {
                        dest_data = new_buffer;
                        break;
                    }
                }
            }
            m.stop();
            allocate += m.as_us();

            let mut time_read = Measure::start("read");
            let result = reader.read(&mut dest_data[..]);
            time_read.stop();
            read += time_read.as_us();
            match result {
                Ok(size) => {
                    if size == 0 {
                        self.instance
                            .file_read_complete
                            .store(true, Ordering::Relaxed);
                        notify += notify_all(); // notify after read complete is set
                        break;
                    }
                    total_len += size;
                    self.instance.new_data.write().unwrap().push(dest_data);
                    chunk_index += 1;
                    //

                    notify += notify_all(); // notify after data added
                }
                Err(err) => {
                    self.set_error(err);
                    break;
                }
            }
        }
        time.stop();
        let _ = handle.unwrap().join();
        self.instance.len.fetch_add(total_len, Ordering::Relaxed);
        error!(
            "reading entire decompressed file took: {} us, bytes: {}, read_us: {}, notify_us: {}, allocate_us: {}, chunks: {}",
            time.as_us(),
            self.instance.len.load(Ordering::Relaxed),
            read,
            notify,
            allocate,
            chunk_index,
        );
    }
    fn set_error(&self, error: std::io::Error) {
        error!("error reading file");
        *self.instance.error.write().unwrap() = Err(error);
        let (_lock, cvar) = &self.instance.new_data_signal;
        cvar.notify_all(); // notify after error is set
    }
    fn transfer_data(&self) -> bool {
        let mut from_lock = self.instance.new_data.write().unwrap();
        if from_lock.is_empty() {
            return false;
        }
        let mut new_data: Vec<Vec<u8>> = vec![];
        std::mem::swap(&mut *from_lock, &mut new_data);
        drop(from_lock);
        let mut to_lock = self.instance.data.write().unwrap();
        to_lock.append(&mut new_data);
        true
    }
    pub fn calls(&self) -> usize {
        self.instance.calls.load(Ordering::Relaxed)
    }
    pub fn len(&self) -> usize {
        self.instance.len.load(Ordering::Relaxed)
    }
    fn wait_for_new_data(&self) -> bool {
        let (lock, cvar) = &self.instance.new_data_signal;
        let data = lock.lock().unwrap();
        let res = cvar
            .wait_timeout(data, std::time::Duration::from_millis(1000))
            .unwrap();
        if res.1.timed_out() {
            return true;
        }
        return false;
    }
}

impl Read for SeekableBufferingReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let request_len = buf.len();

        let mut remaining_request = request_len;
        let mut offset_in_dest = 0;
        while remaining_request > 0 {
            {
                let error = self.instance.error.read().unwrap();
                if error.is_err() {
                    drop(error);
                    let mut error = self.instance.error.write().unwrap();
                    let mut stored_error = Ok(0);
                    std::mem::swap(&mut *error, &mut stored_error);
                    drop(error);
                    return stored_error;
                }
            }
            let lock = self.instance.data.read().unwrap();
            if self.last_buffer_index >= lock.len() {
                drop(lock);
                if self.transfer_data() {
                    continue;
                }
                if self.instance.file_read_complete.load(Ordering::Relaxed) {
                    break; // eof reached
                }
                // no data to transfer, and file not finished, so wait:
                let timed_out = self.wait_for_new_data();
                std::thread::sleep(std::time::Duration::from_millis(1000));
                info!("Waiting on new data, timed out: {}", timed_out);
                continue;
            }
            let source = &lock[self.last_buffer_index];
            let full_len = source.len();
            let remaining_len = full_len - self.next_index_within_last_buffer;
            if remaining_len >= remaining_request {
                let bytes_to_transfer = remaining_request;
                buf[offset_in_dest..(offset_in_dest + bytes_to_transfer)].copy_from_slice(
                    &source[self.next_index_within_last_buffer
                        ..(self.next_index_within_last_buffer + bytes_to_transfer)],
                );
                self.next_index_within_last_buffer += bytes_to_transfer;
                offset_in_dest += bytes_to_transfer;
                remaining_request -= bytes_to_transfer;
            } else {
                let bytes_to_transfer = remaining_len;
                buf[offset_in_dest..(offset_in_dest + bytes_to_transfer)].copy_from_slice(
                    &source[self.next_index_within_last_buffer
                        ..(self.next_index_within_last_buffer + bytes_to_transfer)],
                );
                offset_in_dest += bytes_to_transfer;
                self.next_index_within_last_buffer = 0;
                self.last_buffer_index += 1;
                remaining_request -= bytes_to_transfer;
            }
        }

        self.instance.calls.fetch_add(1, Ordering::Relaxed);
        Ok(offset_in_dest)
    }
}
