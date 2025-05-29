use std::sync::mpsc::Sender as StdSender;

pub type SeedFoundCallback = Option<extern "C" fn(seed: i64)>;
pub type ProgressCallback = Option<extern "C" fn(processed_delta: u64)>;
pub type InitCallback = Option<extern "C" fn(total_units: u64, initial_seeds_found: u64)>;

#[derive(Clone)]
pub struct CallbackAndCollectorSender {
    java_seed_callback: SeedFoundCallback,
    java_progress_callback: ProgressCallback,
    rust_seed_collector_tx: StdSender<i64>,
}

impl CallbackAndCollectorSender {
    pub fn new(
        java_seed_callback: SeedFoundCallback,
        java_progress_callback: ProgressCallback,
        rust_seed_collector_tx: StdSender<i64>,
    ) -> Self {
        Self {
            java_seed_callback,
            java_progress_callback,
            rust_seed_collector_tx,
        }
    }
}

impl crate::raw_data::sender::Sender for CallbackAndCollectorSender {
    fn send(&self, progress_event: crate::CrackProgress) -> bool {
        match progress_event {
            crate::CrackProgress::Seed(seed_u64) => {
                let seed_i64 = seed_u64 as i64;
                if let Some(cb) = self.java_seed_callback {
                    cb(seed_i64);
                }
                self.rust_seed_collector_tx.send(seed_i64).is_ok()
            }
            crate::CrackProgress::Progress(units_processed_delta) => {
                if let Some(cb) = self.java_progress_callback {
                    cb(units_processed_delta);
                }
                true
            }
        }
    }
}