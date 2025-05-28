#[macro_use]
extern crate lazy_static;

mod block_data;
mod layer;
pub mod raw_data;
mod progress_tracker;
mod callback;

use std::cmp::min;
use std::sync::mpsc::{channel, Receiver as StdReceiver};
use std::thread;

use crate::block_data::{BlockFilter, get_filter_power};
use crate::layer::create_filter_tree;
use crate::raw_data::block::Block;
use crate::raw_data::modes::{BedrockGeneration, OutputMode};
use crate::progress_tracker::{
    ProgressData,
    initialize_cracking_session,
    finalize_cracking_session,
    increment_processed_units,
    should_stop_processing,
    IS_CRACKING_ACTIVE_ATOMIC,
    STOP_REQUESTED_ATOMIC,
};
use crate::callback::{SeedFoundCallback, CallbackAndCollectorSender};

const MASK48: u64 = 0xFFFF_FFFF_FFFF;
const ROOF_HASH: u64 = 343340730;
const FLOOR_HASH: u64 = 2042456806;
const CHUNK_SIZE_FOR_LOGIC_INTERNAL: u64 = (1 << 12) * (1 << 25);

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
    internal_estimate_result_amount(blocks)
}

#[no_mangle]
pub extern "C" fn crack_ffi(
    blocks_ptr: *const Block,
    len: usize,
    threads: u64,
    mode: BedrockGeneration,
    output_mode: OutputMode,
    java_seed_cb: SeedFoundCallback,
) -> VecI64 {
    if blocks_ptr.is_null() || len == 0 || threads == 0 {
        return VecI64 { ptr: std::ptr::null(), len: 0 };
    }
    let blocks: &[Block] = unsafe { std::slice::from_raw_parts(blocks_ptr, len) };
    let blocks_owned = blocks.to_vec();

    let total_search_space: u64 = (1u64 << 36) << 12;
    initialize_cracking_session(total_search_space);

    let (rust_seed_collector_tx, rust_seed_collector_rx) = channel::<i64>();

    let callback_sender = CallbackAndCollectorSender::new(
        java_seed_cb,
        rust_seed_collector_tx,
    );

    let collected_seeds_vec = internal_crack(
        blocks_owned,
        threads,
        mode,
        output_mode,
        callback_sender,
        rust_seed_collector_rx,
    );

    finalize_cracking_session();
    collected_seeds_vec.into()
}

#[no_mangle]
pub extern "C" fn get_crack_progress_ffi() -> ProgressData {
    let mut data = progress_tracker::CRACK_PROGRESS_REPORTER.lock().unwrap().clone();
    data.is_cracking_active = IS_CRACKING_ACTIVE_ATOMIC.load(std::sync::atomic::Ordering::Relaxed);
    data
}

#[no_mangle]
pub extern "C" fn reset_crack_progress_externally_ffi() {
    let mut progress_guard = progress_tracker::CRACK_PROGRESS_REPORTER.lock().unwrap();
    progress_guard.reset();
    IS_CRACKING_ACTIVE_ATOMIC.store(false, std::sync::atomic::Ordering::Relaxed);
    STOP_REQUESTED_ATOMIC.store(false, std::sync::atomic::Ordering::Relaxed);
}

#[no_mangle]
pub extern "C" fn request_stop_crack_ffi() {
    STOP_REQUESTED_ATOMIC.store(true, std::sync::atomic::Ordering::Relaxed);
}

#[no_mangle]
pub extern "C" fn free_seed_vector_ffi(vec_i64: VecI64) {
    if !vec_i64.ptr.is_null() && vec_i64.len > 0 {
        unsafe {
            let _ = Vec::from_raw_parts(vec_i64.ptr as *mut i64, vec_i64.len, vec_i64.len);
        }
    }
}

fn internal_estimate_result_amount(blocks: &[Block]) -> u64 {
    let filters: Vec<_> = blocks.iter()
        .map(|block| BlockFilter::from(block, BedrockGeneration::Normal))
        .collect();
    get_filter_power(&filters)
}

fn internal_crack<S: crate::raw_data::sender::Sender + Clone + Send + Sync + 'static>(
    blocks: Vec<Block>,
    threads: u64,
    mode: BedrockGeneration,
    output_mode: OutputMode,
    sender_for_layers: S,
    final_seed_collector_rx: StdReceiver<i64>,
) -> Vec<i64> {
    search_bedrock_pattern_internal(
        &blocks,
        threads,
        mode,
        output_mode,
        sender_for_layers,
    );

    let mut seeds_vec = vec![];
    while let Ok(seed) = final_seed_collector_rx.recv() {
        seeds_vec.push(seed);
    }
    seeds_vec.sort_unstable();
    seeds_vec.dedup();
    seeds_vec
}

fn search_bedrock_pattern_internal<S: crate::raw_data::sender::Sender + Clone + Send + Sync + 'static>(
    blocks: &[Block],
    thread_count: u64,
    mode: BedrockGeneration,
    output: OutputMode,
    sender_instance: S,
) {
    let layers = create_filter_tree(blocks, mode, output, sender_instance.clone());

    let total_tasks_overall = 1u64 << 36;
    let mut thread_handles = Vec::new();

    for thread_idx in 0..thread_count {
        let start_task_idx = (thread_idx * total_tasks_overall) / thread_count;
        let end_task_idx = ((thread_idx + 1) * total_tasks_overall) / thread_count;

        let start_bits = start_task_idx << 12;
        let end_bits = end_task_idx << 12;

        let layers_clone = layers.clone();

        let handle = thread::spawn(move || {
            let mut current_pos_in_thread_segment = start_bits;
            while current_pos_in_thread_segment < end_bits {
                if should_stop_processing() {
                    return;
                }

                let chunk_process_end = min(current_pos_in_thread_segment + CHUNK_SIZE_FOR_LOGIC_INTERNAL, end_bits);
                let units_in_this_chunk = chunk_process_end - current_pos_in_thread_segment;

                for upper_bits_base in (current_pos_in_thread_segment..chunk_process_end).step_by(1 << 12) {
                    layers_clone.run_checks(upper_bits_base);
                }

                increment_processed_units(units_in_this_chunk);
                current_pos_in_thread_segment = chunk_process_end;
            }
        });
        thread_handles.push(handle);
    }

    for handle in thread_handles {
        handle.join().expect("A cracking thread panicked");
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