mod block_data;
mod layer;
pub mod raw_data;
mod callback;

use std::cmp::min;
use std::sync::mpsc::{channel};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crate::block_data::{BlockFilter, get_filter_power};
use crate::layer::create_filter_tree;
use crate::raw_data::block::Block;
use crate::raw_data::modes::{BedrockGeneration, OutputMode};
use crate::callback::{SeedFoundCallback, ProgressCallback, InitCallback, CallbackAndCollectorSender};

const MASK48: u64 = 0xFFFF_FFFF_FFFF;
const ROOF_HASH: u64 = 343340730;
const FLOOR_HASH: u64 = 2042456806;
const CHUNK_SIZE_FOR_LOGIC_INTERNAL: u64 = (1 << 12) * (1 << 25);

static CRACKING_SHOULD_STOP_ATOMIC: AtomicBool = AtomicBool::new(false);

#[repr(C)]
pub struct VecI64 {
    ptr: *const i64,
    len: usize,
}

#[no_mangle]
pub extern "C" fn estimate_result_amount_ffi(blocks_ptr: *const Block, len: usize) -> u64 {
    if blocks_ptr.is_null() || len == 0 {
        return 0;
    }
    let blocks: &[Block] = unsafe { std::slice::from_raw_parts(blocks_ptr, len) };
    let filters: Vec<_> = blocks.iter()
        .map(|block| BlockFilter::from(block, BedrockGeneration::Normal))
        .collect();
    get_filter_power(&filters)
}

#[no_mangle]
pub extern "C" fn crack_ffi(
    blocks_ptr: *const Block,
    len: usize,
    threads: u64,
    mode: BedrockGeneration,
    output_mode: OutputMode,
    java_seed_cb: SeedFoundCallback,
    java_progress_cb: ProgressCallback,
    java_init_cb: InitCallback,
) -> VecI64 {
    if blocks_ptr.is_null() || len == 0 || threads == 0 {
        return VecI64 { ptr: std::ptr::null(), len: 0 };
    }
    let blocks: &[Block] = unsafe { std::slice::from_raw_parts(blocks_ptr, len) };
    let blocks_owned = blocks.to_vec();

    let total_search_space_units: u64 = (1u64 << 36) << 12;

    if let Some(init_cb) = java_init_cb {
        init_cb(total_search_space_units, 0);
    }

    let (rust_seed_collector_tx, rust_seed_collector_rx) = channel::<i64>();

    let callback_sender = CallbackAndCollectorSender::new(
        java_seed_cb,
        java_progress_cb,
        rust_seed_collector_tx,
    );

    execute_cracking(
        &blocks_owned,
        threads,
        mode,
        output_mode,
        callback_sender,
    );

    let mut seeds_vec = vec![];
    while let Ok(seed) = rust_seed_collector_rx.recv() {
        seeds_vec.push(seed);
    }
    seeds_vec.sort_unstable();
    seeds_vec.dedup();
    
    seeds_vec.into()
}

#[no_mangle]
pub extern "C" fn reset_cracker_state_ffi() {
    CRACKING_SHOULD_STOP_ATOMIC.store(false, Ordering::Relaxed);
}

#[no_mangle]
pub extern "C" fn request_stop_crack_ffi() {
    CRACKING_SHOULD_STOP_ATOMIC.store(true, Ordering::Relaxed);
}

#[no_mangle]
pub extern "C" fn free_seed_vector_ffi(vec_i64: VecI64) {
    if !vec_i64.ptr.is_null() && vec_i64.len > 0 {
        unsafe {
            let _ = Vec::from_raw_parts(vec_i64.ptr as *mut i64, vec_i64.len, vec_i64.len);
        }
    }
}

fn execute_cracking<S: crate::raw_data::sender::Sender + Clone + Send + Sync + 'static>(
    blocks: &[Block],
    thread_count: u64,
    mode: BedrockGeneration,
    output: OutputMode,
    sender_instance: S,
) {
    let layers = create_filter_tree(blocks, mode, output, sender_instance.clone());

    let total_tasks_for_threading = 1u64 << 36;
    let mut thread_handles = Vec::new();

    for thread_idx in 0..thread_count {
        let start_task_idx = (thread_idx * total_tasks_for_threading) / thread_count;
        let end_task_idx = ((thread_idx + 1) * total_tasks_for_threading) / thread_count;

        let start_bits = start_task_idx << 12;
        let end_bits = end_task_idx << 12;

        let layers_clone = layers.clone();
        let sender_clone_for_thread_progress = sender_instance.clone();

        let handle = thread::spawn(move || {
            let mut current_pos_in_thread_segment = start_bits;
            while current_pos_in_thread_segment < end_bits {
                if CRACKING_SHOULD_STOP_ATOMIC.load(Ordering::Relaxed) {
                    return;
                }

                let chunk_process_end = min(current_pos_in_thread_segment + CHUNK_SIZE_FOR_LOGIC_INTERNAL, end_bits);
                let units_in_this_chunk = chunk_process_end - current_pos_in_thread_segment;

                for upper_bits_base in (current_pos_in_thread_segment..chunk_process_end).step_by(1 << 12) {
                     if CRACKING_SHOULD_STOP_ATOMIC.load(Ordering::Relaxed) {
                        return;
                    }
                    layers_clone.run_checks(upper_bits_base);
                }

                sender_clone_for_thread_progress.send(CrackProgress::Progress(units_in_this_chunk));
                current_pos_in_thread_segment = chunk_process_end;
            }
        });
        thread_handles.push(handle);
    }

    for handle in thread_handles {
        if let Err(e) = handle.join() {
            eprintln!("A cracking thread panicked: {:?}", e);
        }
    }
}

#[derive(Clone, Debug)]
pub enum CrackProgress {
    Seed(u64),
    Progress(u64),
}

impl From<Vec<i64>> for VecI64 {
    fn from(mut vec: Vec<i64>) -> Self {
        vec.shrink_to_fit();
        let ptr = vec.as_ptr();
        let len = vec.len();
        std::mem::forget(vec);
        VecI64 { ptr, len }
    }
}