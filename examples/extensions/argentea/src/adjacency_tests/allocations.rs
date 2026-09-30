//! Thread-local requested-allocation counters, excluding fixture construction.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Counts {
    pub calls: usize,
    pub bytes: usize,
    pub peak: usize,
    live: isize,
}
thread_local! { static ACTIVE: Cell<Option<Counts>> = const { Cell::new(None) }; }
struct Measured;
#[global_allocator]
static ALLOCATOR: Measured = Measured;
fn allocated(bytes: usize, old: usize) {
    ACTIVE.with(|active| {
        if let Some(mut counts) = active.get() {
            counts.calls += 1;
            counts.bytes += bytes;
            counts.live += bytes as isize - old as isize;
            counts.peak = counts.peak.max(counts.live.max(0) as usize);
            active.set(Some(counts));
        }
    });
}
// SAFETY: Every allocation and deallocation uses System with its original
// pointer and layout. Recording uses allocation-free thread-local Cells.
unsafe impl GlobalAlloc for Measured {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            allocated(layout.size(), 0);
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            allocated(layout.size(), 0);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        ACTIVE.with(|active| {
            if let Some(mut counts) = active.get() {
                counts.live -= layout.size() as isize;
                active.set(Some(counts));
            }
        });
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, size) };
        if !result.is_null() {
            allocated(size, layout.size());
        }
        result
    }
}
pub(super) fn measure<T>(f: impl FnOnce() -> T) -> (T, Counts) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ACTIVE.with(|active| active.set(None));
        }
    }
    ACTIVE.with(|active| {
        assert!(active.get().is_none());
        active.set(Some(Counts::default()));
    });
    let reset = Reset;
    let result = f();
    let counts = ACTIVE.with(|active| active.get().unwrap());
    drop(reset);
    (result, counts)
}
