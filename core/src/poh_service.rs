//! The `poh_service` module implements a service that records the passing of
//! "ticks", a measure of time in the PoH stream
use crate::poh_recorder::{PohRecorder, PohRecorderError};
use solana_ledger::poh::PohEntry;
use solana_measure::measure::Measure;
use solana_sdk::{clock::Slot, hash::Hash, poh_config::PohConfig, transaction::Transaction};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{
    mpsc::{channel, Receiver, RecvError, Sender, TryRecvError},
    Arc, Mutex,
};
use std::thread::{self, sleep, Builder, JoinHandle};
use std::time::Instant;

pub struct PohService {
    tick_producer: JoinHandle<()>,
}

// Number of hashes to batch together.
// * If this number is too small, PoH hash rate will suffer.
// * The larger this number is from 1, the speed of recording transactions will suffer due to lock
//   contention with the PoH hashing within `tick_producer()`.
//
// Can use test_poh_service to calibrate this
pub const DEFAULT_HASHES_PER_BATCH: u64 = 1;

pub const DEFAULT_PINNED_CPU_CORE: usize = 0;

const TARGET_SLOT_ADJUSTMENT_NS: u64 = 50_000_000;

impl PohService {
    pub fn new(
        poh_recorder: Arc<Mutex<PohRecorder>>,
        poh_config: &Arc<PohConfig>,
        poh_exit: &Arc<AtomicBool>,
        ticks_per_slot: u64,
        pinned_cpu_core: usize,
        hashes_per_batch: u64,
        receiver_mixin: Receiver<(
            Hash,
            Vec<Transaction>,
            Slot,
            Sender<std::result::Result<(), PohRecorderError>>,
        )>,
        sender_mixin_result: Sender<Option<PohEntry>>,
    ) -> Self {
        let poh_exit_ = poh_exit.clone();
        let poh_config = poh_config.clone();
        let tick_producer = Builder::new()
            .name("solana-poh-service-tick_producer".to_string())
            .spawn(move || {
                solana_sys_tuner::request_realtime_poh();
                if poh_config.hashes_per_tick.is_none() {
                    if poh_config.target_tick_count.is_none() {
                        Self::sleepy_tick_producer(poh_recorder, &poh_config, &poh_exit_);
                    } else {
                        Self::short_lived_sleepy_tick_producer(
                            poh_recorder,
                            &poh_config,
                            &poh_exit_,
                        );
                    }
                } else {
                    // PoH service runs in a tight loop, generating hashes as fast as possible.
                    // Let's dedicate one of the CPU cores to this thread so that it can gain
                    // from cache performance.
                    if let Some(cores) = core_affinity::get_core_ids() {
                        core_affinity::set_for_current(cores[pinned_cpu_core]);
                    }
                    // Account for some extra time outside of PoH generation to account
                    // for processing time outside PoH.
                    let adjustment_per_tick = if ticks_per_slot > 0 {
                        TARGET_SLOT_ADJUSTMENT_NS / ticks_per_slot
                    } else {
                        0
                    };
                    Self::tick_producer(
                        poh_recorder,
                        &poh_exit_,
                        poh_config.target_tick_duration.as_nanos() as u64 - adjustment_per_tick,
                        ticks_per_slot,
                        hashes_per_batch,
                        receiver_mixin,
                        sender_mixin_result,
                    );
                }
                poh_exit_.store(true, Ordering::Relaxed);
            })
            .unwrap();

        Self { tick_producer }
    }

    fn sleepy_tick_producer(
        poh_recorder: Arc<Mutex<PohRecorder>>,
        poh_config: &PohConfig,
        poh_exit: &AtomicBool,
    ) {
        error!("sleepy_tick_producer");
        while !poh_exit.load(Ordering::Relaxed) {
            sleep(poh_config.target_tick_duration);
            poh_recorder.lock().unwrap().tick();
        }
    }

    fn short_lived_sleepy_tick_producer(
        poh_recorder: Arc<Mutex<PohRecorder>>,
        poh_config: &PohConfig,
        poh_exit: &AtomicBool,
    ) {
        error!("short_lived_sleepy_tick_producer");

        let mut warned = false;
        for _ in 0..poh_config.target_tick_count.unwrap() {
            sleep(poh_config.target_tick_duration);
            poh_recorder.lock().unwrap().tick();
            if poh_exit.load(Ordering::Relaxed) && !warned {
                warned = true;
                warn!("exit signal is ignored because PohService is scheduled to exit soon");
            }
        }
    }

    fn tick_producer(
        poh_recorder: Arc<Mutex<PohRecorder>>,
        poh_exit: &AtomicBool,
        target_tick_ns: u64,
        ticks_per_slot: u64,
        hashes_per_batch: u64,
        receiver_mixin: Receiver<(
            Hash,
            Vec<Transaction>,
            Slot,
            Sender<std::result::Result<(), PohRecorderError>>,
        )>,
        sender_mixin_result: Sender<Option<PohEntry>>,
    ) {
        //error!("tick_producer");
        let poh;
        let mut tick_height;
        {
            let recorder = poh_recorder.lock().unwrap();
            poh = recorder.poh.clone();
            tick_height = recorder.tick_height();
        }
        let mut now = Instant::now();
        let mut last_metric = Instant::now();
        let mut num_ticks = 0;
        let mut num_hashes = 0;
        let mut total_sleep_us = 0;
        let mut total_lock_time_ns = 0;
        let mut total_lock_time_record_ns = 0;
        let mut total_lock_time_poh_ns = 0;
        let mut total_hash_time_ns = 0;
        let mut total_tick_time_ns = 0;
        let mut sleep_deficit_ns = 0;
        let mut ct = 0;
        let mut try_again_mixin = None;
        loop {
            num_hashes += hashes_per_batch;
            let should_tick = {
                let mixin = if let Some((mixin, transactions, bank_slot, sender)) = try_again_mixin
                {
                    try_again_mixin = None;
                    Ok((mixin, transactions, bank_slot, sender))
                } else {
                    receiver_mixin.try_recv()
                };
                if let Ok((mut mixin, mut transactions, mut bank_slot, mut sender)) = mixin {
                    //error!("jwash:Received mixin");
                    let mut lock_time = Measure::start("lock");
                    let mut poh_recorder_l = poh_recorder.lock().unwrap();
                    lock_time.stop();
                    total_lock_time_record_ns += lock_time.as_ns();
                    loop {
                        let res = poh_recorder_l.record(bank_slot, mixin, transactions);
                        sender.send(res);
                        if let Ok(mut mixin, mut transactions, mut bank_slot, mut sender) =
                            receiver_mixin.try_recv()
                        {
                            continue;
                        }
                        break;
                    }
                    false // record will tick if it needs to
                } else {
                    let mut lock_time = Measure::start("lock");
                    let mut poh_l = poh.lock().unwrap(); // keep locked?
                    lock_time.stop();
                    total_lock_time_poh_ns += lock_time.as_ns();
                    let mut hash_time = Measure::start("hash");
                    let r = poh_l.hash(hashes_per_batch);
                    hash_time.stop();
                    total_hash_time_ns += hash_time.as_ns();
                    r
                }
            };
            ct += 1;
            if ct % 1000000 == 0 {
                //error!("count: {}", ct);
            }
            if should_tick {
                //error!("tick count: {}", ct);
                // Lock PohRecorder only for the final hash...
                {
                    let mut lock_time = Measure::start("lock");
                    let mut poh_recorder_l = poh_recorder.lock().unwrap();
                    lock_time.stop();
                    total_lock_time_ns += lock_time.as_ns();
                    let mut tick_time = Measure::start("tick");
                    poh_recorder_l.tick();
                    tick_height = poh_recorder_l.tick_height();
                    tick_time.stop();
                    total_tick_time_ns += tick_time.as_ns();
                }
                //error!("after tick count: {}", ct);
                num_ticks += 1;
                let elapsed_ns = now.elapsed().as_nanos() as u64;

                /*if elapsed_ns > target_tick_ns {
                    sleep_deficit_ns += elapsed_ns - target_tick_ns;
                }
                let sleep_time_ns = target_tick_ns.saturating_sub(sleep_deficit);*/

                // sleep is not accurate enough to get a predictable time.
                // Kernel can not schedule the thread for a while.
                /*
                while (now.elapsed().as_nanos() as u64) < target_tick_ns {
                    std::hint::spin_loop();
                }*/
                total_sleep_us += (now.elapsed().as_nanos() as u64 - elapsed_ns) / 1000;
                now = Instant::now();

                if last_metric.elapsed().as_millis() > 1000 {
                    let elapsed_us = last_metric.elapsed().as_micros() as u64;
                    let us_per_slot = (elapsed_us * ticks_per_slot) / num_ticks;
                    datapoint_info!(
                        "poh-service",
                        ("ticks", num_ticks as i64, i64),
                        ("hashes", num_hashes as i64, i64),
                        ("elapsed_us", us_per_slot, i64),
                        ("total_sleep_us", total_sleep_us, i64),
                        ("total_tick_time_us", total_tick_time_ns / 1000, i64),
                        ("total_lock_time_tick_us", total_lock_time_ns / 1000, i64),
                        (
                            "total_lock_time_record_us",
                            total_lock_time_record_ns / 1000,
                            i64
                        ),
                        ("total_lock_time_poh_us", total_lock_time_poh_ns / 1000, i64),
                        ("total_hash_time_us", total_hash_time_ns / 1000, i64),
                    );
                    total_sleep_us = 0;
                    num_ticks = 0;
                    num_hashes = 0;
                    total_tick_time_ns = 0;
                    total_lock_time_ns = 0;
                    total_lock_time_record_ns = 0;
                    total_lock_time_ns = 0;
                    total_lock_time_poh_ns = 0;
                    last_metric = Instant::now();
                }
                if poh_exit.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
        let mut dct = 0;
        while receiver_mixin.try_recv().is_ok() {
            dct += 1;
        }

        drop(receiver_mixin);
        drop(sender_mixin_result);
        error!("tick producer break, count: {}, dropped: {}", ct, dct);
    }

    pub fn join(self) -> thread::Result<()> {
        self.tick_producer.join()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::poh_recorder::WorkingBank;
    use num_format::{Buffer, Locale};
    use rand::{thread_rng, Rng};
    use rayon::iter::{IntoParallelIterator, ParallelIterator};
    use rayon::ThreadPoolBuilder;
    use solana_ledger::genesis_utils::{create_genesis_config, GenesisConfigInfo};
    use solana_ledger::leader_schedule_cache::LeaderScheduleCache;
    use solana_ledger::{blockstore::Blockstore, get_tmp_ledger_path, poh::compute_hash_time_ns};
    use solana_measure::measure::Measure;
    use solana_perf::test_tx::test_tx;
    use solana_runtime::bank::Bank;
    use solana_sdk::clock;
    use solana_sdk::hash::hash;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::timing;
    use std::time::Duration;

    #[test]
    fn test_poh_service() {
        solana_logger::setup();

        let hash_samples = std::env::var("HASH_SAMPLES")
            .map(|x| x.parse().unwrap())
            .unwrap_or(1_000_000);

        let hash_ns = compute_hash_time_ns(hash_samples);
        let default_target_tick_duration =
            timing::duration_as_us(&PohConfig::default().target_tick_duration);
        let target_tick_duration = Duration::from_micros(default_target_tick_duration);

        let ticks_per_second = 1_000_000 / default_target_tick_duration;
        let hashes_per_second = hash_samples * 1_000_000_000 / hash_ns;
        let hashes_per_tick = hashes_per_second / ticks_per_second;
        info!(
            "{} hashes/s {} ns/hash: {} hashes/tick: {}",
            ticks_per_second,
            hashes_per_second,
            hash_ns / hash_samples,
            hashes_per_tick,
        );

        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new(&genesis_config));
        let prev_hash = bank.last_blockhash();
        let ledger_path = get_tmp_ledger_path!();

        let blockstore =
            Blockstore::open(&ledger_path).expect("Expected to be able to open database ledger");
        let blockstore = Arc::new(blockstore);

        // specify RUN_TIME to run in a benchmark-like mode
        // to calibrate batch size
        let run_time = std::env::var("RUN_TIME")
            .map(|x| x.parse().unwrap())
            .unwrap_or(0);
        let is_test_run = run_time == 0;

        let par_batch_size = std::env::var("PAR_BATCH_SIZE")
            .map(|x| x.parse().unwrap())
            .unwrap_or(1);

        let rayon_threads = std::env::var("RAYON_THREADS")
            .map(|x| x.parse().unwrap())
            .unwrap_or(1);

        let use_rayon = std::env::var("USE_RAYON").is_ok();
        let set_affinity = std::env::var("SET_AFFINITY").is_ok();

        info!(
            "batch_size: {} rayon: {} rayon_threads: {} set_affinity: {}",
            par_batch_size, use_rayon, rayon_threads, set_affinity
        );

        for i in 0..10 {
            let mut waiting = Arc::new(Mutex::new(0));
            let poh_config = Arc::new(PohConfig {
                hashes_per_tick: Some((hashes_per_tick / 10) * (i + 1)),
                target_tick_duration,
                target_tick_count: None,
            });
            let (poh_recorder, entry_receiver, receiver_mixin, sender_mixin_result) =
                PohRecorder::new(
                    bank.tick_height(),
                    prev_hash,
                    bank.slot(),
                    Some((4, 4)),
                    bank.ticks_per_slot(),
                    &Pubkey::default(),
                    &blockstore.clone(),
                    &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                    &poh_config,
                );
            let sender_mixin = poh_recorder.sender_mixin();
            let poh_recorder = Arc::new(Mutex::new(poh_recorder));
            let exit = Arc::new(AtomicBool::new(false));
            let working_bank = WorkingBank {
                bank: bank.clone(),
                min_tick_height: bank.tick_height(),
                max_tick_height: std::u64::MAX,
            };
            let ticks_per_slot = bank.ticks_per_slot();
            let waiting2 = waiting.clone();
            let entry_producer = {
                let poh_recorder = poh_recorder.clone();
                let exit = exit.clone();
                let bank = bank.clone();

                Builder::new()
                    .name("solana-poh-service-entry_producer".to_string())
                    .spawn(move || {
                        let thread_pool = ThreadPoolBuilder::new()
                            .num_threads(rayon_threads)
                            .start_handler(move |_i| {
                                if set_affinity {
                                    let cores: Vec<_> =
                                        (2..affinity::get_core_num()).into_iter().collect();
                                    affinity::set_thread_affinity(&cores).unwrap();
                                }
                            })
                            .build()
                            .unwrap();
                        let now = Instant::now();
                        let mut total_us = 0;
                        let mut total_times = 0;
                        //error!("ln: {}", line!());
                        let h1 = hash(b"hello world!");
                        let tx = test_tx();
                        //error!("ln: {}", line!());
                        loop {
                            //error!("ln: {}", line!());
                            // send some data
                            let mut time = Measure::start("record");
                            let record_lock = |sender_result: Sender<
                                std::result::Result<(), PohRecorderError>,
                            >,
                                               receiver_result: &Receiver<
                                std::result::Result<(), PohRecorderError>,
                            >,
                                               sender_mixin: &Sender<(
                                Hash,
                                Vec<Transaction>,
                                Slot,
                                Sender<std::result::Result<(), PohRecorderError>>,
                            )>| {
                                //error!("Sending mixin");
                                let res = sender_mixin.send((
                                    h1,
                                    vec![tx.clone()],
                                    bank.slot(),
                                    sender_result,
                                ));
                                if res.is_err() {
                                    error!(
                                        "Failed to send record, current waits: {}",
                                        *waiting.lock().unwrap()
                                    );
                                    return ();
                                }
                                *waiting.lock().unwrap() += 1;
                                let res = receiver_result.recv(); //_timeout(Duration::from_millis(4000));
                                *waiting.lock().unwrap() -= 1;
                                if res.is_err() {
                                    match res {
                                        Err(err) => {
                                            error!("Error in recv_timeout");
                                            return (); /* Err(
                                                           PohRecorderError::InvalidCallingObject,
                                                       );*/
                                        }
                                        Ok(_) => {}
                                    }
                                }
                                ()
                            };
                            //error!("ln: {}", line!());
                            if use_rayon {
                                let chunks = rayon_threads;
                                let chunk_size = par_batch_size / chunks;
                                let sender_mixins = (0..chunks)
                                    .into_iter()
                                    .map(|chunk| (sender_mixin.clone(), chunk))
                                    .collect::<Vec<_>>();
                                //error!("this many parallel: {}", sender_mixins.len());
                                thread_pool.install(|| {
                                    sender_mixins.into_par_iter().for_each(
                                        |(sender_mixin, chunk)| {
                                            //let sender_mixin = sender_mixin.clone();
                                            let (sender_result, receiver_result) = channel();
                                            let mut chunk_size = chunk_size;
                                            if chunk == chunks - 1 {
                                                let chunk_size_new =
                                                    par_batch_size - chunk_size * (chunks - 1);
                                                assert!(chunk_size_new < chunk_size + chunks);
                                                chunk_size = chunk_size_new;
                                            }
                                            for _i in 0..chunk_size {
                                                record_lock(
                                                    sender_result.clone(),
                                                    &receiver_result,
                                                    &sender_mixin,
                                                );
                                            }
                                        },
                                    )
                                    /*
                                    (0..par_batch_size).into_par_iter().for_each(record_lock);
                                    */
                                });
                            } else {
                                panic!("unsupported"); //(0..par_batch_size).into_iter().for_each(record_lock);
                            }
                            time.stop();
                            total_us += time.as_us();
                            total_times += 1;
                            if is_test_run && thread_rng().gen_ratio(1, 4) {
                                sleep(Duration::from_millis(200));
                            }

                            if exit.load(Ordering::Relaxed) {
                                let mut buf = Buffer::default();
                                buf.write_formatted(&(total_times * par_batch_size), &Locale::en);
                                info!(
                                    "spent:{}ms record: {}ms entries recorded: {}",
                                    now.elapsed().as_millis(),
                                    total_us / 1000,
                                    buf.as_str(),
                                );
                                break;
                            }
                        }
                    })
                    .unwrap()
            };

            let hashes_per_batch = std::env::var("HASHES_PER_BATCH")
                .map(|x| x.parse().unwrap())
                .unwrap_or(DEFAULT_HASHES_PER_BATCH);
            let poh_service = PohService::new(
                poh_recorder.clone(),
                &poh_config,
                &exit,
                0,
                DEFAULT_PINNED_CPU_CORE,
                hashes_per_batch,
                receiver_mixin,
                sender_mixin_result,
            );
            poh_recorder.lock().unwrap().set_working_bank(working_bank);

            // get some events
            let mut hashes = 0;
            let mut need_tick = true;
            let mut need_entry = true;
            let mut need_partial = true;
            let mut num_ticks = 0;

            let time = Instant::now();
            while run_time != 0 || need_tick || need_entry || need_partial {
                let (_bank, (entry, _tick_height)) = entry_receiver.recv().unwrap();

                if entry.is_tick() {
                    num_ticks += 1;
                    assert!(
                        entry.num_hashes <= poh_config.hashes_per_tick.unwrap(),
                        "{} <= {}",
                        entry.num_hashes,
                        poh_config.hashes_per_tick.unwrap()
                    );

                    if entry.num_hashes == poh_config.hashes_per_tick.unwrap() {
                        need_tick = false;
                    } else {
                        need_partial = false;
                    }

                    hashes += entry.num_hashes;

                    //error!("hashes: {}, hashes_per_tick expected: {}, num hashes: {}, num_ticks: {}, iteration: {}", hashes, poh_config.hashes_per_tick.unwrap(), entry.num_hashes, num_ticks, i);
                    assert_eq!(
                        hashes,
                        poh_config.hashes_per_tick.unwrap(),
                        "num hashes: {}, num_ticks: {}, iteration: {}",
                        entry.num_hashes,
                        num_ticks,
                        i
                    );

                    hashes = 0;
                } else {
                    assert!(entry.num_hashes >= 1);
                    need_entry = false;
                    hashes += entry.num_hashes;
                }

                if run_time != 0 {
                    if time.elapsed().as_millis() > run_time {
                        break;
                    }
                } else {
                    assert!(
                        time.elapsed().as_secs() < 60,
                        "Test should not run for this long! {}s tick {} entry {} partial {}",
                        time.elapsed().as_secs(),
                        need_tick,
                        need_entry,
                        need_partial,
                    );
                }
            }
            info!(
                "hashes_per_tick: {:?} target_tick_duration: {} ns ticks_per_slot: {}",
                poh_config.hashes_per_tick,
                poh_config.target_tick_duration.as_nanos(),
                ticks_per_slot
            );
            let elapsed = time.elapsed();
            info!(
                "{} ticks in {}ms {}us/tick",
                num_ticks,
                elapsed.as_millis(),
                elapsed.as_micros() / num_ticks
            );

            error!(
                "Trying to exit, active waits: {}",
                *waiting2.lock().unwrap()
            );
            exit.store(true, Ordering::Relaxed);
            error!("poh_service.join");
            poh_service.join().unwrap();
            drop(poh_recorder);
            //drop(poh_service);
            error!("entry_producer.join: {}", *waiting2.lock().unwrap());
            entry_producer.join().unwrap();
        }
        drop(blockstore);
        Blockstore::destroy(&ledger_path).unwrap();
    }
}
