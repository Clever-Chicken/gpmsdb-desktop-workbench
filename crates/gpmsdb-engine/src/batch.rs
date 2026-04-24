use gpmsdb_format::MappedDatabase;
use rayon::prelude::*;
use std::{
    cell::RefCell,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
};

use crate::{identify_with_buffer, QueryPeak, RankedResult, SearchBuffer};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchProgressEvent {
    pub processed: usize,
    pub total: usize,
}

pub fn run_batch(
    db: &MappedDatabase,
    queries: &[Vec<QueryPeak>],
    coarse_limit: usize,
    ppm: u32,
    cancel: &AtomicBool,
) -> (Vec<Vec<RankedResult>>, Vec<BatchProgressEvent>) {
    let events = Mutex::new(Vec::new());
    let results = run_batch_with_progress(db, queries, coarse_limit, ppm, cancel, 1, |event| {
        events
            .lock()
            .expect("batch progress lock poisoned")
            .push(event);
    });
    let mut events = events.into_inner().expect("batch progress lock poisoned");
    events.sort_by_key(|event| event.processed);

    (results, events)
}

pub fn run_batch_with_progress<F>(
    db: &MappedDatabase,
    queries: &[Vec<QueryPeak>],
    coarse_limit: usize,
    ppm: u32,
    cancel: &AtomicBool,
    progress_every: usize,
    on_progress: F,
) -> Vec<Vec<RankedResult>>
where
    F: Fn(BatchProgressEvent) + Sync + Send,
{
    let total = queries.len();
    let processed = AtomicUsize::new(0);
    let progress_every = progress_every.max(1);
    let genome_count = db.header().genome_count as usize;

    // Each rayon worker thread gets its own SearchBuffer — zero contention,
    // zero per-query allocation after the first query on each thread.
    thread_local! {
        static THREAD_BUF: RefCell<Option<SearchBuffer>> = const { RefCell::new(None) };
    }

    let mut rows = queries
        .par_iter()
        .enumerate()
        .map(|(index, query)| {
            if cancel.load(Ordering::Relaxed) {
                return (index, Vec::new());
            }

            let result = THREAD_BUF.with(|cell| {
                let mut opt = cell.borrow_mut();
                let buf = opt.get_or_insert_with(|| SearchBuffer::new(genome_count));
                identify_with_buffer(db, query, coarse_limit, ppm, buf);
                buf.ranked.clone()
            });

            if cancel.load(Ordering::Relaxed) {
                return (index, result);
            }

            let done = processed.fetch_add(1, Ordering::Relaxed) + 1;
            if done == total || done % progress_every == 0 {
                on_progress(BatchProgressEvent {
                    processed: done,
                    total,
                });
            }

            (index, result)
        })
        .collect::<Vec<_>>();

    rows.sort_by_key(|(index, _)| *index);
    rows.into_iter().map(|(_, result)| result).collect()
}

pub fn run_batch_for_test(
    db: &MappedDatabase,
    queries: &[Vec<QueryPeak>],
    coarse_limit: usize,
    ppm: u32,
) -> (Vec<Vec<RankedResult>>, Vec<BatchProgressEvent>) {
    let cancel = AtomicBool::new(false);
    run_batch(db, queries, coarse_limit, ppm, &cancel)
}

pub fn run_batch_with_progress_for_test(
    db: &MappedDatabase,
    queries: &[Vec<QueryPeak>],
    coarse_limit: usize,
    ppm: u32,
    progress_every: usize,
) -> Vec<BatchProgressEvent> {
    let cancel = AtomicBool::new(false);
    let events = Mutex::new(Vec::new());
    let _results = run_batch_with_progress(
        db,
        queries,
        coarse_limit,
        ppm,
        &cancel,
        progress_every,
        |event| {
            events
                .lock()
                .expect("batch progress lock poisoned")
                .push(event);
        },
    );
    let mut events = events.into_inner().expect("batch progress lock poisoned");
    events.sort_by_key(|event| event.processed);
    events
}
