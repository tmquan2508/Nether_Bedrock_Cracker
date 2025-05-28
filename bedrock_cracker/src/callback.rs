use std::sync::mpsc::Sender as StdSender;
// remove: use crate::CrackProgress; // Unused as progress_event is already typed with crate::CrackProgress

pub type SeedFoundCallback = Option<extern "C" fn(seed: i64)>;

#[derive(Clone)]
pub struct CallbackAndCollectorSender {
    java_callback: SeedFoundCallback,
    rust_collector_tx: StdSender<i64>,
}

impl CallbackAndCollectorSender {
    pub fn new(java_callback: SeedFoundCallback, rust_collector_tx: StdSender<i64>) -> Self {
        Self {
            java_callback,
            rust_collector_tx,
        }
    }
}

impl crate::raw_data::sender::Sender for CallbackAndCollectorSender {
    fn send(&self, progress_event: crate::CrackProgress) -> bool {
        match progress_event {
            crate::CrackProgress::Seed(seed_u64) => {
                let seed_i64 = seed_u64 as i64;
                if let Some(cb) = self.java_callback {
                    cb(seed_i64);
                }
                let send_successful = self.rust_collector_tx.send(seed_i64).is_ok();
                if send_successful {
                    crate::progress_tracker::increment_seeds_found();
                }
                send_successful
            }
            crate::CrackProgress::Progress(_units) => {
                true
            }
        }
    }
}