//! Isolated allocator test: only this test runs in this executable, so the
//! libtest runner cannot overlap unrelated allocation-heavy test fixtures.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, UInt32Array};
use arrow::record_batch::RecordBatch;
use nutmeg_graph::{ColumnMapping, SessionRegistry, StageOrder};

struct Meter;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Meter = Meter;

#[test]
fn rejected_expanding_batch_never_allocates_its_normalized_buffers() {
    let batch = RecordBatch::try_from_iter([(
        "id",
        Arc::new(UInt32Array::from((0..1_000_000).collect::<Vec<u32>>())) as ArrayRef,
    )])
    .unwrap();
    let registry = SessionRegistry::new(5 << 20);
    let mapping = ColumnMapping::default();
    let mut tx = registry.replacing("g", &mapping, &mapping, StageOrder::Canonical);
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let result = tx.push_nodes(&batch);
    let allocated = PEAK.load(Ordering::Relaxed) - before;
    assert!(result.is_err());
    // Even one normalized offsets buffer needs >4 MB. Leave ample room for
    // schema/error metadata while proving no such buffer was allocated.
    assert!(
        allocated < 256 * 1024,
        "rejected normalization allocated {allocated} bytes"
    );
    assert_eq!(registry.memory().unwrap().used_bytes, 0);

    // The same valid batch succeeds with enough room. The allocator observes
    // transient growth as well as retained arrays; reservation counters alone
    // would not detect the original allocate-first defect.
    let accepted = SessionRegistry::new(256 << 20);
    let mut tx = accepted.replacing("g", &mapping, &mapping, StageOrder::Canonical);
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    tx.push_nodes(&batch).unwrap();
    let normalization_peak = PEAK.load(Ordering::Relaxed) - before;
    assert!(normalization_peak <= accepted.memory().unwrap().peak_bytes);
    tx.finish().unwrap();
    let whole_stage_peak = PEAK.load(Ordering::Relaxed) - before;
    assert!(whole_stage_peak <= accepted.memory().unwrap().peak_bytes);
}
