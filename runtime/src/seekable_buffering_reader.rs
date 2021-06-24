use {
    log::*,
    solana_measure::measure::Measure,
    std::{
        io::*,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Mutex, RwLock,
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
    pub stop: AtomicBool,
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
            stop: AtomicBool::new(false),
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
        let mut data = [0u8; CHUNK_SIZE];
        loop {
            if self.instance.stop.load(Ordering::Relaxed) {
                info!("stopped before file reading was finished");
                break;
            }
            let result = reader.read(&mut data);
            match result {
                Ok(size) => {
                    self.instance
                        .new_data
                        .write()
                        .unwrap()
                        .push(data[0..size].to_vec());
                    self.instance.len.fetch_add(size, Ordering::Relaxed);
                    if size == 0 {
                        break;
                    }
                }
                Err(err) => {
                    error!("error reading file");
                    *self.instance.error.write().unwrap() = Err(err);
                    break;
                }
            }
        }
        time.stop();
        error!(
            "reading entire decompressed file took: {} us, bytes: {}",
            time.as_us(),
            self.instance.len.load(Ordering::Relaxed)
        );
    }
    fn transfer_data(&self) {
        let mut from_lock = self.instance.new_data.write().unwrap();
        if from_lock.is_empty() {
            return;
        }
        let mut new_data: Vec<Vec<u8>> = vec![];
        std::mem::swap(&mut *from_lock, &mut new_data);
        drop(from_lock);
        let mut to_lock = self.instance.data.write().unwrap();
        to_lock.append(&mut new_data);
    }
    pub fn calls(&self) -> usize {
        self.instance.calls.load(Ordering::Relaxed)
    }
    pub fn len(&self) -> usize {
        self.instance.len.load(Ordering::Relaxed)
    }
}

impl Read for SeekableBufferingReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let request_len = buf.len();

        let mut remaining_request = request_len;
        let mut offset_in_dest = 0;
        let mut transferred_data = false;
        while remaining_request > 0 {
            let lock = self.instance.data.read().unwrap();
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
            if self.last_buffer_index >= lock.len() {
                if !transferred_data {
                    transferred_data = true;
                    drop(lock);
                    self.transfer_data();
                    continue;
                }
                break; // no more to read right now
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
