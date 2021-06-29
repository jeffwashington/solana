use {
    crossbeam_channel::{unbounded, RecvTimeoutError},
    log::*,
    rayon::{prelude::*, ThreadPool},
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

type ABuffer = Arc<Vec<u8>>;

pub struct SeekableBufferingReaderInner {
    // unpacking callers read from 'data'. Data is transferred when 'data' is exhausted.
    // This minimizes lock contention since bg file reader has to have almost constant write access.
    pub data: RwLock<Vec<ABuffer>>,
    // bg thread reads to 'new_data'
    pub new_data: RwLock<Vec<ABuffer>>,
    pub len: AtomicUsize,
    pub error: RwLock<std::io::Result<usize>>,
    pub data_written: AtomicUsize,
    pub bg_reader: Mutex<Option<JoinHandle<()>>>,
    pub file_read_complete: AtomicBool,
    pub stop: AtomicBool,
    pub new_data_signal: (Mutex<bool>, Condvar),
    pub new_buffer_signal: (Mutex<bool>, Condvar),
    pub clients: RwLock<Vec<usize>>, // keep track of the next read location per outstanding client
    pub buffers: RwLock<Vec<ABuffer>>,
    pub empty_buffer: ABuffer,
}

pub struct SeekableBufferingReader {
    pub instance: Arc<SeekableBufferingReaderInner>,
    pub pos: usize,
    pub last_buffer_index: usize,
    pub next_index_within_last_buffer: usize,
    pub my_client_index: usize,
    pub time_spent_waiting: u64,
    pub in_read: u64,
    pub in_read_part1: u64,
    pub in_read_part2: u64,
    pub copy_data: u64,
    pub update_client_index: u64,
    pub transfer_data: u64,
    pub lock_data: u64,
    pub current_data: ABuffer,
    pub calls: usize,
    pub empty_buffer: ABuffer,
}
/*
impl Clone for SeekableBufferingReader {
    fn clone(&self) -> Self {
        let instance = Arc::clone(&self.instance);
        let mut list = instance.clients.write().unwrap();
        let my_client_index = list.len();
        error!("adding client: {}", my_client_index);
        list.push(0);
        drop(list);
        Self {
            instance,
            pos: 0,
            last_buffer_index: 0,
            next_index_within_last_buffer: 0,
            my_client_index,
        }
    }
}
*/
impl Drop for SeekableBufferingReaderInner {
    fn drop(&mut self) {
        if let Some(handle) = self.bg_reader.lock().unwrap().take() {
            self.stop.store(true, Ordering::Relaxed);
            handle.join().unwrap();
        }
    }
}

const TOTAL_BUFFER_BUDGET: usize = 4_000_000_000;
const CHUNK_SIZE: usize = 100_000_000;
const MAX_READ_SIZE: usize = 100_000_000; //65536*2;

impl SeekableBufferingReader {
    fn new_with_instance(instance: &Arc<SeekableBufferingReaderInner>) -> Self {
        Self {
            instance: Arc::clone(instance),
            pos: 0,
            last_buffer_index: 0,
            next_index_within_last_buffer: 0,
            my_client_index: usize::MAX,
            in_read: 0,
            in_read_part1: 0,
            in_read_part2: 0,
            time_spent_waiting: 0,
            copy_data: 0,
            update_client_index: 0,
            transfer_data: 0,
            lock_data: 0,
            // data[0] will always exist. may be empty, But that is ok. Corresponds to last_buffer_index initial value of 0
            current_data: instance.data.read().unwrap()[0].clone(),
            calls: 0,
            empty_buffer: instance.empty_buffer.clone(),
        }
    }
    pub fn clone_reader(&self) -> Self {
        let mut result = Self::new_with_instance(&self.instance);
        let mut list = self.instance.clients.write().unwrap();
        result.my_client_index = list.len();
        error!("adding client: {}", result.my_client_index);
        list.push(0);
        drop(list);
        result
    }
    pub fn clone_internal(&self) -> Self {
        Self::new_with_instance(&self.instance)
    }
    pub fn new<T: 'static + Read + std::marker::Send>(reader: Vec<T>) -> Self {
        let inner = SeekableBufferingReaderInner {
            new_data: RwLock::new(vec![]),
            data: RwLock::new(vec![ABuffer::default()]), // initialize with 1 vector of empty data
            len: AtomicUsize::new(0),
            error: RwLock::new(Ok(0)),
            data_written: AtomicUsize::new(0),
            bg_reader: Mutex::new(None),
            file_read_complete: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            new_data_signal: (Mutex::new(false), Condvar::new()),
            new_buffer_signal: (Mutex::new(false), Condvar::new()),
            clients: RwLock::new(vec![]),
            buffers: RwLock::new(Self::alloc_initial_vectors()),
            empty_buffer: ABuffer::default(),
        };
        let result = Self::new_with_instance(&Arc::new(inner));

        let divisions = reader.len();
        let par = reader
            .into_iter()
            .enumerate()
            .map(|(i, reader)| {
                let mut r = result.clone_internal();
                r.no_more_reading();
                (i, reader, r)
            })
            .collect::<Vec<_>>();
        let handle = Builder::new()
            .name("solana-compressed_file_reader".to_string())
            .spawn(move || {
                par.into_par_iter().for_each(|(i, reader, result)| {
                    error!("starting: {}", i);
                    result.read_entire_file_in_bg(reader, i, divisions);
                });
            });
        *result.instance.bg_reader.lock().unwrap() = Some(handle.unwrap()); // TODO - unwrap here - do we expect to fail creating a thread? If we do, probably a fatal error anyway.
        result
    }
    fn alloc_initial_vectors() -> Vec<ABuffer> {
        let buffer_count = TOTAL_BUFFER_BUDGET / CHUNK_SIZE;
        let initial_smaller_buffers_for_startup = 10;
        let initial_vector_count = buffer_count + initial_smaller_buffers_for_startup;
        error!("{} initial vectors", initial_vector_count);
        (0..initial_vector_count)
            .into_iter()
            .map(|i| {
                let size = if i >= buffer_count {
                    // a few smaller sizes to get us initial data quicker to prevent readers from waiting
                    CHUNK_SIZE / 10
                } else {
                    CHUNK_SIZE
                };
                Arc::new(vec![0u8; size])
            })
            .collect()
    }
    fn read_entire_file_in_bg<T: 'static + Read + std::marker::Send>(
        &self,
        mut reader: T,
        division: usize,
        divisions: usize,
    ) {
        let now = std::time::Instant::now();
        let mut time = Measure::start("");
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
        error!("{}, {}", file!(), line!());

        let request = Arc::new((Mutex::new(false), Condvar::new()));
        /*
        let (sender, receiver) = unbounded();
        let request_ = request.clone();
        let handle = Builder::new()
            .name("solana-buf_allocator".to_string())
            .spawn(move || {
                error!("{}, {}", file!(), line!());
                'outer: loop {
                    let max_bins = 100;
                    for _b in 0..max_bins {
                        let v = if true {
                            let mut v = Vec::with_capacity(CHUNK_SIZE);
                            unsafe {
                                v.set_len(CHUNK_SIZE);
                            }
                            v
                        } else {
                            vec![0u8; CHUNK_SIZE]
                        };
                        let _ = sender.send(v);
                    }
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
            */

        let mut largest = 0;
        let mut chunk_index = 0;
        let mut total_len = 0;
        error!("{}, {}", file!(), line!());
        let mut division_index = 0;
        let dummy = self.empty_buffer.clone();
        'outer: loop {
            if self.instance.stop.load(Ordering::Relaxed) {
                self.set_error(std::io::Error::from(std::io::ErrorKind::TimedOut));
                info!("stopped before file reading was finished");
                break;
            }

            let mut dest_data = dummy.clone();
            let mut timeout_us = 0;
            let mut attempts = 0;
            let mut m = Measure::start("");
            let use_this_division = division_index % divisions == division;
            let mut read_this_much = CHUNK_SIZE;
            if use_this_division {
                loop {
                    let mut buffers = self.instance.buffers.write().unwrap();
                    let remaining = buffers.len();
                    let buffer = buffers.pop();
                    drop(buffers);
                    match buffer {
                        Some(buffer) => {
                            //error!("got buffer: idx: {}, remainng: {}", division_index, remaining);
                            dest_data = buffer;
                            read_this_much = dest_data.len();
                            assert_eq!(Arc::strong_count(&dest_data), 1);
                            break;
                        }
                        None => {
                            //error!("wait for new buffer");
                            // none available, so wait
                            self.wait_for_new_buffer();
                        }
                    }
                }
            } else {
                dest_data = dummy.clone();
            }
            m.stop();
            allocate += m.as_us();

            let mut read_this_time = 0;
            let mut end = false;
            let mut static_data = vec![0; MAX_READ_SIZE];
            //let mut other_static_data = vec![0; MAX_READ_SIZE];
            loop {
                //std::mem::swap(&mut static_data, &mut other_static_data);
                let read_start = read_this_time;
                let read_end = std::cmp::min(read_start + MAX_READ_SIZE, read_this_much); // TODO this should be: dest_data.len());
                if read_end == read_start {
                    break;
                }
                let mut time_read = Measure::start("read");
                let result = reader.read(&mut static_data[0..(read_end - read_start)]);
                time_read.stop();
                read += time_read.as_us();
                match result {
                    Ok(size) => {
                        if size == 0 {
                            end = true;
                            break;
                        }

                        if use_this_division {
                            if division_index < 3 && read_start == 0 {
                                error!(
                                    "division_index: {} block read: {} bytes, {} us",
                                    division_index,
                                    size,
                                    now.elapsed().as_micros()
                                );
                            }
                            Arc::make_mut(&mut dest_data)[read_start..(read_start + size)]
                                .copy_from_slice(&static_data[0..size]);
                        }
                        // loop to read some more
                        largest = std::cmp::max(largest, size);
                        read_this_time += size;
                    }
                    Err(err) => {
                        self.set_error(err);
                        break 'outer;
                    }
                }
            }

            if read_this_time > 0 {
                if use_this_division {
                    self.instance
                        .len
                        .fetch_add(read_this_time, Ordering::Relaxed);
                    total_len += read_this_time;
                    if division_index < 3 {
                        error!(
                            "final: division_index: {} block read: {} bytes, {} us",
                            division_index,
                            read_this_time,
                            now.elapsed().as_micros()
                        );
                    }

                    loop {
                        let chunks_written = self.instance.data_written.load(Ordering::Relaxed);
                        if chunks_written == division_index {
                            let mut data = self.instance.new_data.write().unwrap();
                            data.push(dest_data);
                            drop(data);
                            notify += notify_all(); // notify after data added
                            self.instance.data_written.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        // we are ready with the next section, but the previous section hasn't written to the final output buffer yet, so we have to wait until it writes
                        self.wait_for_new_data();
                    }
                }
                division_index += 1;
                chunk_index += 1;
            }

            if end {
                self.instance
                    .file_read_complete
                    .store(true, Ordering::Relaxed);
                notify += notify_all(); // notify after read complete is set
                break 'outer;
            }
        }
        time.stop();
        {
            *request.0.lock().unwrap() = true;
            request.1.notify_one();
        }

        error!("waiting to join allocator");
        //let _ = handle.unwrap().join();
        //self.instance.len.fetch_add(total_len, Ordering::Relaxed);
        error!(
            "reading entire decompressed file took: {} us, bytes: {}, read_us: {}, notify_us: {}, allocate_us: {}, chunks: {}, largest fetch: {}",
            time.as_us(),
            self.instance.len.load(Ordering::Relaxed),
            read,
            notify,
            allocate,
            chunk_index,
            largest,
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
        let mut new_data: Vec<ABuffer> = vec![];
        std::mem::swap(&mut *from_lock, &mut new_data);
        drop(from_lock);
        let mut to_lock = self.instance.data.write().unwrap();
        to_lock.append(&mut new_data);
        true
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
    fn wait_for_new_buffer(&self) -> bool {
        let (lock, cvar) = &self.instance.new_buffer_signal;
        let data = lock.lock().unwrap();
        let res = cvar
            .wait_timeout(data, std::time::Duration::from_millis(1000))
            .unwrap();
        if res.1.timed_out() {
            error!("timed out waiting for buffer");
            return true;
        }
        return false;
    }
    pub fn no_more_reading(&mut self) {
        if self.my_client_index != usize::MAX {
            self.update_client_index(usize::MAX);
        }
    }
    fn update_client_index(&mut self, last_buffer_index: usize) {
        if self.my_client_index == usize::MAX {
            panic!("");
        }
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
            let eof = self.reached_eof();

            for recycle in (previous_last_buffer_index..new_min) {
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
    fn reached_eof(&self) -> bool {
        self.instance.file_read_complete.load(Ordering::Relaxed)
    }
}

impl Read for SeekableBufferingReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let request_len = buf.len();
        let mut remaining_request = request_len;
        let mut offset_in_dest = 0;
        let mut source = &*self.current_data;
        let full_len = source.len();
        let remaining_len = full_len - self.next_index_within_last_buffer;
        let bytes_to_transfer = std::cmp::min(remaining_len, request_len);

        // copy what we can
        buf[offset_in_dest..(offset_in_dest + bytes_to_transfer)].copy_from_slice(
            &source[self.next_index_within_last_buffer
                ..(self.next_index_within_last_buffer + bytes_to_transfer)],
        );
        self.next_index_within_last_buffer += bytes_to_transfer;
        offset_in_dest += bytes_to_transfer;
        remaining_request -= bytes_to_transfer;

        let mut eof_seen = false;
        while remaining_request > 0 {
            let mut good = true;
            if source.is_empty() {
                let mut m = Measure::start("");
                let mut instance = &*self.instance;
                let lock = instance.data.read().unwrap();
                m.stop();
                self.lock_data += m.as_us();
                if self.last_buffer_index < lock.len() {
                    self.current_data = Arc::clone(&lock[self.last_buffer_index]);
                    //error!("updating data to {}, len: {}", self.last_buffer_index, self.current_data.len());
                    source = &*self.current_data;
                }
                else {
                    good = false;
                    //error!("could not update data: {}", self.last_buffer_index);
                }
            }
            if good {
                let full_len = source.len();
                let remaining_len = full_len - self.next_index_within_last_buffer;

                let bytes_to_transfer = std::cmp::min(remaining_request, remaining_len);

                // copy what we can
                let mut m = Measure::start("");
                buf[offset_in_dest..(offset_in_dest + bytes_to_transfer)].copy_from_slice(
                    &source[self.next_index_within_last_buffer
                        ..(self.next_index_within_last_buffer + bytes_to_transfer)],
                );
                m.stop();
                self.copy_data += m.as_us();
                self.next_index_within_last_buffer += bytes_to_transfer;
                offset_in_dest += bytes_to_transfer;
                remaining_request -= bytes_to_transfer;

                if remaining_request == 0 {
                    break;
                }
                drop(source);
                self.current_data = self.empty_buffer.clone(); // we have exhausted this buffer, unref it so it can be recycled without copy
                self.next_index_within_last_buffer = 0;
                let mut m = Measure::start("");
                self.update_client_index(self.last_buffer_index + 1);
                m.stop();
                source = &*self.empty_buffer;
                self.update_client_index += m.as_us();
            }

            let mut m = Measure::start("");
            let mut instance = &*self.instance;
            let lock = instance.data.read().unwrap();
            m.stop();
            self.lock_data += m.as_us();

            if self.last_buffer_index >= lock.len() {
                drop(lock);

                let mut m = Measure::start("");
                let r = self.transfer_data();
                m.stop();
                self.transfer_data += m.as_us();
                if r {
                    continue;
                }
                if self.last_buffer_index >= instance.data.read().unwrap().len() {
                    continue;
                }

                if eof_seen {
                    error!("eof reached");
                    break; // eof reached
                }

                let mut m = Measure::start("");
                // no data, we could not transfer, and still no data, so check for eof.
                // If we got an eof, then we have to check again to make sure there isn't data now that we may have to transfer or not.
                if self.reached_eof() {
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
                        error!("got error");
                        return stored_error;
                    }
                }
                // no data to transfer, and file not finished, so wait:
                //std::thread::sleep(std::time::Duration::from_millis(1000));
                let timed_out = self.wait_for_new_data();
                m.stop();
                if self.last_buffer_index == 0 {
                    info!(
                        "Waited on new data, timed out: {}, us: {}",
                        timed_out,
                        m.as_us()
                    );
                }
                if timed_out {
                    error!("timed out waiting for new data");
                }
                self.time_spent_waiting += m.as_us();
                continue;
            }
        }
        Ok(offset_in_dest)
    }
}

impl Drop for SeekableBufferingReader {
    fn drop(&mut self) {
        if self.my_client_index != usize::MAX {
            error!("dropping client: {}, waiting: {} us, in_read: {} us, copy_data: {} us, recycler: {} us, transfer: {} us, lock: {} us, left over: {} us, calls: {}, parts 1/2: {}, {}", self.my_client_index, self.time_spent_waiting, self.in_read, self.copy_data, self.update_client_index, self.transfer_data, self.lock_data,
            self.in_read - (self.copy_data+ self.update_client_index + self.transfer_data + self.lock_data), self.calls, self.in_read_part1, self.in_read_part2);
            self.update_client_index(usize::MAX); // this one is done reading
        }
    }
}
