//! Build-time source lookup over sorted, unique IDs already checked for ownership.
pub(crate) struct SourceIndex<'a> {
    ids: &'a [i64],
    stride: usize,
    dense: bool,
}
impl<'a> SourceIndex<'a> {
    /// Every ID must already have the same owner modulo the nonzero stride.
    pub(crate) fn new(ids: &'a [i64], stride: usize) -> Self {
        // Unique IDs with one residue occupy a contiguous owner-strided interval
        // exactly when the endpoint span has room for only those IDs. Use i128
        // here so signed IDs spanning i64::MIN through i64::MAX cannot overflow.
        let dense = ids.last().is_some_and(|last| {
            (ids.len() - 1)
                .checked_mul(stride)
                .is_some_and(|span| *last as i128 - ids[0] as i128 == span as i128)
        });
        Self { ids, stride, dense }
    }
    pub(crate) fn lookup(&self, source: i64) -> Option<usize> {
        if !self.dense {
            return self.ids.binary_search(&source).ok();
        }
        let first = self.ids[0];
        if source < first {
            return None;
        }
        // The nonnegative distance fits u64 even across the signed boundary.
        let distance = source.wrapping_sub(first) as u64;
        let index = usize::try_from(distance / self.stride as u64).ok()?;
        // Check equality as well as bounds: a foreign owner's ID can fall inside
        // the interval without belonging to this local arithmetic sequence.
        (self.ids.get(index) == Some(&source)).then_some(index)
    }
    pub(crate) fn lookup_work(&self) -> usize {
        if self.dense {
            1
        } else {
            1 + self.ids.len().max(1).ilog2() as usize
        }
    }
}
