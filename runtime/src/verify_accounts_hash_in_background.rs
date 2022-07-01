//! at startup, verify accounts hash in the background
use {
    crate::waitable_condvar::WaitableCondvar,
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        thread::JoinHandle,
        time::Duration,
    },
};

#[derive(Debug)]
pub(crate) struct VerifyAccountsHashInBackground {
    pub(crate) verified: Arc<AtomicBool>,
    complete: Arc<WaitableCondvar>,
    thread: Mutex<Option<JoinHandle<bool>>>,
}

impl Default for VerifyAccountsHashInBackground {
    fn default() -> Self {
        Self {
            complete: Arc::default(),
            verified: Arc::default(),
            // no thread to start with
            thread: Mutex::new(None::<JoinHandle<bool>>),
        }
    }
}

impl VerifyAccountsHashInBackground {
    /// notify that the bg process has completed
    pub(crate) fn finished(&self) {
        self.complete.notify_all();
    }
    /// notify that the bg process has started
    pub(crate) fn started(&self, thread: JoinHandle<bool>) {
        *self.thread.lock().unwrap() = Some(thread);
    }
    /// block until bg process is complete
    pub fn wait_for_complete(&self) {
        // just now completing
        let mut lock = self.thread.lock().unwrap();
        if lock.is_none() {
            return; // nothing to do
        }
        let result = lock.take().unwrap().join().unwrap();
        if !result {
            // initial verification failed
            panic!("initial hash verification failed");
        }
        // we never have to check again
        self.verified.store(true, Ordering::Relaxed);
    }
    /// return true if bg hash verification is complete
    /// return false if bg hash verification has not completed yet
    /// if hash verification failed, a panic will occur
    pub(crate) fn check_complete(&self) -> bool {
        if self.verified.load(Ordering::Relaxed) {
            // already completed
            return true;
        }
        if self.complete.wait_timeout(Duration::default()) {
            self.wait_for_complete();
            true
        } else {
            false
        }
    }
}
