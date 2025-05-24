use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::cmp::min;

#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct ProgressData {
    pub total_units_to_process: u64,
    pub units_processed_so_far: u64,
    pub seeds_found_count: u64,
    pub is_cracking_active: bool,
}

impl ProgressData {
    pub(crate) fn reset(&mut self) {
        self.total_units_to_process = 0;
        self.units_processed_so_far = 0;
        self.seeds_found_count = 0;
        self.is_cracking_active = false;
    }

    pub(crate) fn start_cracking(&mut self, total_units: u64) {
        self.reset();
        self.total_units_to_process = total_units;
    }
}

lazy_static! {
    pub(crate) static ref CRACK_PROGRESS_REPORTER: Arc<Mutex<ProgressData>> = Arc::new(Mutex::new(ProgressData::default()));
}

pub static IS_CRACKING_ACTIVE_ATOMIC: AtomicBool = AtomicBool::new(false);
pub static STOP_REQUESTED_ATOMIC: AtomicBool = AtomicBool::new(false);

pub(crate) fn initialize_cracking_session(total_units: u64) {
    STOP_REQUESTED_ATOMIC.store(false, Ordering::Relaxed);
    IS_CRACKING_ACTIVE_ATOMIC.store(true, Ordering::Relaxed);
    let mut progress_guard = CRACK_PROGRESS_REPORTER.lock().expect("Mutex poisoned during initialize");
    progress_guard.start_cracking(total_units);
}

pub(crate) fn finalize_cracking_session() {
    IS_CRACKING_ACTIVE_ATOMIC.store(false, Ordering::Relaxed);
    let cracking_was_stopped_early = STOP_REQUESTED_ATOMIC.load(Ordering::Relaxed);
    {
        let mut progress_guard = CRACK_PROGRESS_REPORTER.lock().expect("Mutex poisoned during finalize");
        if !cracking_was_stopped_early && progress_guard.total_units_to_process > 0 {
            progress_guard.units_processed_so_far = progress_guard.total_units_to_process;
        }
    }
}

pub(crate) fn increment_processed_units(units: u64) {
    if IS_CRACKING_ACTIVE_ATOMIC.load(Ordering::Relaxed) {
        let mut progress_guard = CRACK_PROGRESS_REPORTER.lock().expect("Mutex poisoned during increment_processed");
        let current_processed = progress_guard.units_processed_so_far;
        let total_units = progress_guard.total_units_to_process;
        progress_guard.units_processed_so_far = min(current_processed.saturating_add(units), total_units);
    }
}

pub(crate) fn increment_seeds_found() {
    if IS_CRACKING_ACTIVE_ATOMIC.load(Ordering::Relaxed) {
        let mut progress_guard = CRACK_PROGRESS_REPORTER.lock().expect("Mutex poisoned during increment_found");
        progress_guard.seeds_found_count = progress_guard.seeds_found_count.saturating_add(1);
    }
}

pub(crate) fn should_stop_processing() -> bool {
    STOP_REQUESTED_ATOMIC.load(Ordering::Relaxed)
}