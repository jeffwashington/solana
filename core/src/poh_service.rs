//! The `poh_service` module implements a service that records the passing of
//! "ticks", a measure of time in the PoH stream
use crate::poh_recorder::{PohRecorder, Record};
use solana_measure::measure::Measure;
use solana_sdk::poh_config::PohConfig;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc::Receiver, Arc, Mutex};
use std::thread::{self, sleep, Builder, JoinHandle};
use std::time::{Duration, Instant};

pub struct PohService {
    tick_producer: JoinHandle<()>,
}

// Number of hashes to batch together.
// * If this number is too small, PoH hash rate will suffer.
// * The larger this number is from 1, the speed of recording transactions will suffer due to lock
//   contention with the PoH hashing within `tick_producer()`.
//
// Can use test_poh_service to calibrate this
pub const DEFAULT_HASHES_PER_BATCH: u64 = 64;

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
        record_receiver: Receiver<Record>,
    ) -> Self {
        let poh_exit_ = poh_exit.clone();
        let poh_config = poh_config.clone();
        let tick_producer = Builder::new()
            .name("solana-poh-service-tick_producer".to_string())
            .spawn(move || {
                solana_sys_tuner::request_realtime_poh();
                if poh_config.hashes_per_tick.is_none() {
                    if poh_config.target_tick_count.is_none() {
                        Self::sleepy_tick_producer(
                            poh_recorder,
                            &poh_config,
                            &poh_exit_,
                            record_receiver,
                        );
                    } else {
                        Self::short_lived_sleepy_tick_producer(
                            poh_recorder,
                            &poh_config,
                            &poh_exit_,
                            record_receiver,
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
                        record_receiver,
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
        record_receiver: Receiver<Record>,
    ) {
        panic!("");

        while !poh_exit.load(Ordering::Relaxed) {
            Self::read_record_receiver_and_process(
                &poh_recorder,
                &record_receiver,
                Duration::from_millis(0),
            );
            sleep(poh_config.target_tick_duration);
            poh_recorder.lock().unwrap().tick(1);
        }
    }

    pub fn read_record_receiver_and_process(
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        record_receiver: &Receiver<Record>,
        timeout: Duration,
    ) {
        panic!("");
        let record = record_receiver.recv_timeout(timeout);
        if let Ok(record) = record {
            if record
                .sender
                .send(poh_recorder.lock().unwrap().record(
                    record.slot,
                    record.mixin,
                    record.transactions,
                    false,
                ))
                .is_err()
            {
                panic!("Error returning mixin hash");
            }
        }
    }

    fn short_lived_sleepy_tick_producer(
        poh_recorder: Arc<Mutex<PohRecorder>>,
        poh_config: &PohConfig,
        poh_exit: &AtomicBool,
        record_receiver: Receiver<Record>,
    ) {
        panic!("");
        let mut warned = false;
        for _ in 0..poh_config.target_tick_count.unwrap() {
            Self::read_record_receiver_and_process(
                &poh_recorder,
                &record_receiver,
                Duration::from_millis(0),
            );
            sleep(poh_config.target_tick_duration);
            poh_recorder.lock().unwrap().tick(1);
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
        record_receiver: Receiver<Record>,
    ) {
        let poh = poh_recorder.lock().unwrap().poh.clone();
        let mut now = Instant::now();
        let mut last_metric = Instant::now();
        let mut num_ticks = 0;
        let mut num_hashes = 0;
        let mut num_just_hashes = 0;
        let mut total_sleep_us = 0;
        let mut total_lock_time_ns = 0;
        let mut total_hash_time_ns = 0;
        let mut total_tick_time_ns = 0;
        let mut try_again_mixin = None;
        let mut failure_count = 10;
        let mut last_tick_height = poh_recorder.lock().unwrap().tick_height();
        let mut last_tick_time = Instant::now();
        let mut num_just_hashes_this_tick = 0;
        loop {
            let should_tick = {
                let mixin = if let Some(record) = try_again_mixin {
                    try_again_mixin = None;
                    Ok(record)
                } else {
                    record_receiver.try_recv()
                };
                if let Ok(mut record) = mixin {
                    let mut lock_time = Measure::start("lock");
                    let mut poh_recorder_l = poh_recorder.lock().unwrap();
                    lock_time.stop();
                    total_lock_time_ns += lock_time.as_ns();
                    loop {
                        let mut temp = Vec::new();
                        std::mem::swap(&mut temp, &mut record.transactions);
                        let res = poh_recorder_l.record(record.slot, record.mixin, temp, true);
                        let _ = record.sender.send(res); // TODO what do we do on failure here?
                        num_hashes += 1; // may have also ticked inside record

                        let get_again = record_receiver.try_recv();
                        match get_again {
                            Ok(mut record2) => {
                                std::mem::swap(&mut record2, &mut record);
                            }
                            Err(_) => {
                                break;
                            }
                        }
                    }
                    false // record will tick if it needs to
                } else {
                    let mut lock_time = Measure::start("lock");
                    let mut poh_l = poh.lock().unwrap();
                    lock_time.stop();
                    total_lock_time_ns += lock_time.as_ns();
                    let mut r;
                    loop {
                        num_hashes += hashes_per_batch;
                        num_just_hashes_this_tick += hashes_per_batch;
                        num_just_hashes += hashes_per_batch;
                        let mut hash_time = Measure::start("hash");
                        r = poh_l.hash(hashes_per_batch);
                        hash_time.stop();
                        total_hash_time_ns += hash_time.as_ns();
                        if r {
                            let elapsed_us = last_tick_time.elapsed().as_micros() as u64;
                            if elapsed_us > 7000 {
                                info!("should_tick is late: {:?}, rate: {} kH/s", elapsed_us, num_just_hashes_this_tick * 1_000/elapsed_us );
                            }

                            //error!("should tick in poh: {}", last_tick_height + 1);
                            break;
                        }
                        let get_again = record_receiver.try_recv();
                        match get_again {
                            Ok(inside) => {
                                try_again_mixin = Some(inside);
                                break;
                            }
                            Err(_) => {
                                continue;
                            }
                        }
                    }
                    r
                }
            };
            if should_tick {
                // Lock PohRecorder only for the final hash...
                let tick_height;
                let elapsed_us;
                let mut lock_time;                
                {
                    lock_time = Measure::start("lock");
                    let mut poh_recorder_l = poh_recorder.lock().unwrap();
                    lock_time.stop();
                    total_lock_time_ns += lock_time.as_ns();
                    elapsed_us = last_tick_time.elapsed().as_micros() as u64;
                    let mut tick_time = Measure::start("tick");
                    //error!("ticking: {}", last_tick_height + 1);
                    let res = poh_recorder_l.tick((last_tick_height + 1) as usize);
                    if !res {
                        if failure_count > 0 {
                            failure_count -= 1;
                            error!("error: failed ticking: {}", last_tick_height + 1);
                        }
                        else {
                            error!("error: failed ticking: {}", last_tick_height + 1);
                            //panic!("failed: {}", last_tick_height + 1);
                        }
                    }
                    tick_height = poh_recorder_l.tick_height();
                    tick_time.stop();
                    total_tick_time_ns += tick_time.as_ns();
                    last_tick_time = Instant::now();
                }
                num_just_hashes_this_tick = 0;
                num_ticks += 1;
                let real_tick_elapsed = tick_height - last_tick_height;
                if elapsed_us > 7000 {
                    info!("should_tick is late: {:?}, rate: {} kH/s, htis lock time: {}ns, our ticks: {}, rael ticks: {}", elapsed_us, num_just_hashes_this_tick * 1_000/elapsed_us, lock_time.as_ns(),
                num_ticks, real_tick_elapsed );
                }

                /* why are we sleeping?
                let elapsed_ns = now.elapsed().as_nanos() as u64;
                // sleep is not accurate enough to get a predictable time.
                // Kernel can not schedule the thread for a while.
                while (now.elapsed().as_nanos() as u64) < target_tick_ns {
                    std::hint::spin_loop();
                }
                total_sleep_us += (now.elapsed().as_nanos() as u64 - elapsed_ns) / 1000;
                */
                now = Instant::now();

                let elapsed_us = last_metric.elapsed().as_micros() as u64;
                if elapsed_us > 1_000_000 {
                    let us_per_slot = (elapsed_us * ticks_per_slot) / real_tick_elapsed;
                    datapoint_info!(
                        "poh-service",
                        ("ticks", num_ticks as i64, i64),
                        ("hashes", num_hashes as i64, i64),
                        ("elapsed_us", us_per_slot, i64),
                        ("total_sleep_us", total_sleep_us, i64),
                        ("total_tick_time_us", total_tick_time_ns / 1000, i64),
                        ("total_lock_time_us", total_lock_time_ns / 1000, i64),
                        ("total_hash_time_us", total_hash_time_ns / 1000, i64),
                        ("total_elapsed_us", elapsed_us, i64),
                        ("missed_ticks", real_tick_elapsed, i64),
                        ("effective kHashes/sec", num_hashes * 1_000_000 / (std::cmp::max(1,total_hash_time_ns)), i64),
                    );
                    total_sleep_us = 0;
                    num_ticks = 0;
                    num_hashes = 0;
                    total_tick_time_ns = 0;
                    total_lock_time_ns = 0;
                    total_hash_time_ns = 0;
                    last_metric = Instant::now();
                    last_tick_height = tick_height;
                }
                if poh_exit.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.tick_producer.join()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::poh_recorder::WorkingBank;
    use rand::{thread_rng, Rng};
    use solana_ledger::genesis_utils::{create_genesis_config, GenesisConfigInfo};
    use solana_ledger::leader_schedule_cache::LeaderScheduleCache;
    use solana_ledger::{blockstore::Blockstore, get_tmp_ledger_path};
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
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new(&genesis_config));
        let prev_hash = bank.last_blockhash();
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");

            let default_target_tick_duration =
                timing::duration_as_us(&PohConfig::default().target_tick_duration);
            let target_tick_duration = Duration::from_micros(default_target_tick_duration);
            let poh_config = Arc::new(PohConfig {
                hashes_per_tick: Some(clock::DEFAULT_HASHES_PER_TICK),
                target_tick_duration,
                target_tick_count: None,
            });
            let (poh_recorder, entry_receiver, record_receiver) = PohRecorder::new(
                bank.tick_height(),
                prev_hash,
                bank.slot(),
                Some((4, 4)),
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &poh_config,
            );
            let poh_recorder = Arc::new(Mutex::new(poh_recorder));
            let exit = Arc::new(AtomicBool::new(false));
            let start = Arc::new(Instant::now());
            let working_bank = WorkingBank {
                bank: bank.clone(),
                start,
                min_tick_height: bank.tick_height(),
                max_tick_height: std::u64::MAX,
            };
            let ticks_per_slot = bank.ticks_per_slot();

            // specify RUN_TIME to run in a benchmark-like mode
            // to calibrate batch size
            let run_time = std::env::var("RUN_TIME")
                .map(|x| x.parse().unwrap())
                .unwrap_or(0);
            let is_test_run = run_time == 0;

            let entry_producer = {
                let poh_recorder = poh_recorder.clone();
                let exit = exit.clone();

                Builder::new()
                    .name("solana-poh-service-entry_producer".to_string())
                    .spawn(move || {
                        let now = Instant::now();
                        let mut total_us = 0;
                        let mut total_times = 0;
                        let h1 = hash(b"hello world!");
                        let tx = test_tx();
                        loop {
                            // send some data
                            let mut time = Measure::start("record");
                            let _ = poh_recorder.lock().unwrap().record(
                                bank.slot(),
                                h1,
                                vec![tx.clone()],
                            );
                            time.stop();
                            total_us += time.as_us();
                            total_times += 1;
                            if is_test_run && thread_rng().gen_ratio(1, 4) {
                                sleep(Duration::from_millis(200));
                            }

                            if exit.load(Ordering::Relaxed) {
                                info!(
                                    "spent:{}ms record: {}ms entries recorded: {}",
                                    now.elapsed().as_millis(),
                                    total_us / 1000,
                                    total_times,
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
                record_receiver,
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

                    assert_eq!(hashes, poh_config.hashes_per_tick.unwrap());

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
                "target_tick_duration: {} ticks_per_slot: {}",
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

            exit.store(true, Ordering::Relaxed);
            poh_service.join().unwrap();
            entry_producer.join().unwrap();
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }
}
