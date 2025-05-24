#[macro_use]
extern crate lazy_static;

mod block_data;
mod layer;
pub mod raw_data;
mod progress_tracker;

use std::cmp::min;
use std::sync::mpsc::channel;
use std::thread;

use crate::block_data::{BlockFilter, get_filter_power};
use crate::layer::create_filter_tree;
use crate::raw_data::block::Block;
use crate::raw_data::modes::{BedrockGeneration, OutputMode};
use crate::raw_data::sender::Sender;

use crate::progress_tracker::{
    ProgressData,
    initialize_cracking_session,
    finalize_cracking_session,
    increment_processed_units,
    increment_seeds_found,
    should_stop_processing
};

const MASK48: u64 = 0xFFFF_FFFF_FFFF;
const ROOF_HASH: u64 = 343340730;
const FLOOR_HASH: u64 = 2042456806;

const CHUNK_SIZE_FOR_LOGIC_INTERNAL: u64 = (1 << 12) * (1 << 25);

#[no_mangle]
pub extern "C" fn estimate_result_amount_ffi(blocks_ptr: *const Block, len: usize) -> u64 {
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
) -> VecI64 {
    let blocks: &[Block] = unsafe { std::slice::from_raw_parts(blocks_ptr, len) };
    let blocks_owned = blocks.to_vec();

    let total_search_space: u64 = (1u64 << 36) << 12;
    initialize_cracking_session(total_search_space);

    let result_seeds = internal_crack(blocks_owned, threads, mode, output_mode);

    finalize_cracking_session();
    result_seeds.into()
}

#[no_mangle]
pub extern "C" fn get_crack_progress_ffi() -> ProgressData {
    let mut data = progress_tracker::CRACK_PROGRESS_REPORTER.lock().unwrap().clone();
    data.is_cracking_active = progress_tracker::IS_CRACKING_ACTIVE_ATOMIC.load(std::sync::atomic::Ordering::Relaxed);
    data
}

#[no_mangle]
pub extern "C" fn reset_crack_progress_externally_ffi() {
    let mut progress_guard = progress_tracker::CRACK_PROGRESS_REPORTER.lock().unwrap();
    progress_guard.reset();
    progress_tracker::IS_CRACKING_ACTIVE_ATOMIC.store(false, std::sync::atomic::Ordering::Relaxed);
    progress_tracker::STOP_REQUESTED_ATOMIC.store(false, std::sync::atomic::Ordering::Relaxed);
}

#[no_mangle]
pub extern "C" fn request_stop_crack_ffi() {
    progress_tracker::STOP_REQUESTED_ATOMIC.store(true, std::sync::atomic::Ordering::Relaxed);
}

#[repr(C)]
pub struct VecI64 {
    ptr: *const i64,
    len: usize,
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

fn search_bedrock_pattern_internal<S: Sender + 'static>(
    blocks: &[Block],
    thread_count: u64,
    mode: BedrockGeneration,
    output: OutputMode,
    sender: S,
) {
    let checks = create_filter_tree(blocks, mode, output, sender.clone());

    let total_tasks_overall = 1u64 << 36;
    
    for thread_idx in 0..thread_count {
        let start_task_idx = (thread_idx * total_tasks_overall) / thread_count;
        let end_task_idx = ((thread_idx + 1) * total_tasks_overall) / thread_count;

        let start_bits = start_task_idx << 12;
        let end_bits = end_task_idx << 12;

        let checks_clone = checks.clone();
        // sender_clone không cần thiết ở đây nếu checks_clone.run_checks sử dụng sender từ create_filter_tree

        thread::spawn(move || {
            let mut current_pos_in_thread_segment = start_bits;
            while current_pos_in_thread_segment < end_bits {
                if should_stop_processing() {
                    return;
                }

                let chunk_process_end = min(current_pos_in_thread_segment + CHUNK_SIZE_FOR_LOGIC_INTERNAL, end_bits);
                let units_in_this_chunk = chunk_process_end - current_pos_in_thread_segment;

                for upper_bits_base in (current_pos_in_thread_segment..chunk_process_end).step_by(1 << 12) {
                    checks_clone.run_checks(upper_bits_base);
                }
                
                increment_processed_units(units_in_this_chunk);
                
                current_pos_in_thread_segment = chunk_process_end;
            }
        });
    }
}

fn internal_crack(
    blocks: Vec<Block>,
    threads: u64,
    mode: BedrockGeneration,
    output_mode: OutputMode,
) -> Vec<i64> {
    let (sender, receiver) = channel::<CrackProgress>();

    search_bedrock_pattern_internal(
        &*blocks,
        threads,
        mode,
        output_mode,
        sender,
    );

    let mut seeds_vec = vec![];
    while let Ok(event) = receiver.recv() {
        match event {
            CrackProgress::Seed(num) => {
                seeds_vec.push(num as i64);
                increment_seeds_found();
            }
            CrackProgress::Progress(_units_processed_in_chunk) => {
            }
        }
    }
    seeds_vec
}

#[derive(Clone, Debug)]
pub enum CrackProgress {
    Seed(u64),
    Progress(u64),
}

impl Into<VecI64> for Vec<i64> {
    fn into(mut self) -> VecI64 {
        self.shrink_to_fit();
        let ptr = self.as_ptr();
        let len = self.len();
        std::mem::forget(self);
        VecI64 { ptr, len }
    }
}