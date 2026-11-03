//! Conservative reservations taken before Arrow normalization/sort allocation.
//! These bound Arrow buffers/builders, not process RSS or all Rust metadata.
use super::*;

fn add(a: usize, b: usize) -> usize {
    a.saturating_add(b)
}
fn mul(a: usize, b: usize) -> usize {
    a.saturating_mul(b)
}

/// Include geometric builder growth and the old buffer during reallocation.
/// The retained result is measured and the excess returned afterwards.
fn building(bytes: usize) -> usize {
    add(mul(bytes, 4), 256)
}

fn primitive_string_width(kind: &DataType) -> Option<usize> {
    Some(match kind {
        DataType::Boolean => 5,
        DataType::Int8 => 4,
        DataType::UInt8 => 3,
        DataType::Int16 => 6,
        DataType::UInt16 => 5,
        DataType::Int32 => 11,
        DataType::UInt32 => 10,
        DataType::Int64 | DataType::UInt64 => 20,
        // Even fixed-point rendering of a subnormal f64 fits in 768 bytes.
        DataType::Float16 | DataType::Float32 | DataType::Float64 => 768,
        // At most 78 integer digits plus 128 scale zeros, sign and decimal point.
        DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => 256,
        // Chrono formats a numeric offset, not the arbitrary timezone name.
        // Intervals have at most three integer components plus fixed units.
        DataType::Timestamp(..)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => 128,
        _ => return None,
    })
}

/// Upper bound for ArrayFormatter's recursive textual representation. Ordinary
/// lists visit each child once; views/dictionaries/unions may repeat children.
fn formatted_bytes(array: &ArrayRef) -> Result<usize> {
    let rows = array.len();
    if let Some(width) = primitive_string_width(array.data_type()) {
        return Ok(mul(rows, width));
    }
    let data = array.to_data();
    let children = || {
        data.child_data()
            .iter()
            .map(|child| formatted_bytes(&arrow::array::make_array(child.clone())))
            .try_fold(0, |sum, bytes| bytes.map(|bytes| add(sum, bytes)))
    };
    Ok(match array.data_type() {
        DataType::Null => mul(rows, 4),
        DataType::Utf8 => array
            .as_string::<i32>()
            .iter()
            .flatten()
            .map(str::len)
            .fold(0, add),
        DataType::LargeUtf8 => array
            .as_string::<i64>()
            .iter()
            .flatten()
            .map(str::len)
            .fold(0, add),
        DataType::Utf8View => array
            .as_string_view()
            .iter()
            .flatten()
            .map(str::len)
            .fold(0, add),
        // Binary display is hexadecimal; this also bounds the no-expansion
        // Binary->Utf8 cast used for structural IDs.
        DataType::Binary => mul(
            2,
            array
                .as_binary::<i32>()
                .iter()
                .flatten()
                .map(<[u8]>::len)
                .fold(0, add),
        ),
        DataType::LargeBinary => mul(
            2,
            array
                .as_binary::<i64>()
                .iter()
                .flatten()
                .map(<[u8]>::len)
                .fold(0, add),
        ),
        DataType::BinaryView => mul(
            2,
            array
                .as_binary_view()
                .iter()
                .flatten()
                .map(<[u8]>::len)
                .fold(0, add),
        ),
        DataType::FixedSizeBinary(width) => mul(mul(rows, (*width).max(0) as usize), 2),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(..)
        | DataType::Map(..) => add(
            mul(rows, 4),
            add(
                children()?,
                data.child_data()
                    .iter()
                    .map(|child| mul(child.len(), 4))
                    .fold(0, add),
            ),
        ),
        DataType::ListView(_) | DataType::LargeListView(_) => mul(
            rows,
            add(
                4,
                add(
                    children()?,
                    data.child_data()
                        .iter()
                        .map(|child| mul(child.len(), 4))
                        .fold(0, add),
                ),
            ),
        ),
        DataType::Struct(fields) => add(
            children()?,
            mul(
                rows,
                fields
                    .iter()
                    .map(|field| add(field.name().len(), 8))
                    .fold(4, add),
            ),
        ),
        DataType::Dictionary(..) | DataType::RunEndEncoded(..) => mul(rows, add(4, children()?)),
        DataType::Union(fields, _) => mul(
            rows,
            add(
                children()?,
                fields
                    .iter()
                    .map(|(_, field)| add(field.name().len(), 8))
                    .fold(0, add),
            ),
        ),
        other => return plan_err!("nutmeg: no preallocation bound for structural type {other}"),
    })
}

fn utf8_conversion(array: &ArrayRef) -> Result<usize> {
    if array.data_type() == &DataType::Utf8 {
        return Ok(0);
    }
    // These direct casts are rejected by Arrow 59.3. Reject before invoking any
    // allocation path; nested formatter support is handled separately above.
    if matches!(
        array.data_type(),
        DataType::Struct(_) | DataType::Map(..) | DataType::FixedSizeBinary(_)
    ) || matches!(array.data_type(), DataType::FixedSizeList(_, width) if *width != 1)
    {
        return plan_err!(
            "nutmeg: structural type {} cannot be cast to Utf8",
            array.data_type()
        );
    }
    let rows = array.len();
    let payload = formatted_bytes(array)?;
    let values = if matches!(
        array.data_type(),
        DataType::Dictionary(..) | DataType::RunEndEncoded(..) | DataType::FixedSizeList(_, 1)
    ) {
        let data = array.to_data();
        utf8_conversion(&arrow::array::make_array(
            data.child_data()
                .last()
                .expect("encoded or list values")
                .clone(),
        ))?
    } else {
        0
    };
    // Arrow 59.3 value_to_string starts GenericStringBuilder::new() with 1024
    // offsets and 1024 payload bytes, even for one row. Bound that floor plus
    // geometric growth/reallocation, including simultaneously live output.
    let buffers = add(add(payload, mul(add(rows, 1), 4)), rows.div_ceil(8)).max(8192);
    Ok(add(values, building(buffers)))
}

pub(super) fn normalization_bound(
    batch: &RecordBatch,
    part: Part,
    mapping: &ColumnMapping,
) -> Result<usize> {
    let rows = batch.num_rows();
    // Unchanged columns may share the caller's full allocation, including the
    // parent of a slice. Admission covers that retained allocation too.
    let mut bytes = held_bytes(std::slice::from_ref(batch));
    let mut used = HashSet::new();
    let structural = match part {
        Part::Nodes => vec![
            (&mapping.id, &["node_id", "id"][..], "node id", true),
            (&mapping.label, &["label"][..], "node label", false),
        ],
        Part::Edges => vec![
            (
                &mapping.source,
                &["source", "src_id", "src"][..],
                "edge source",
                true,
            ),
            (
                &mapping.target,
                &["target", "dst_id", "dst"][..],
                "edge target",
                true,
            ),
            (
                &mapping.edge_type,
                &["label", "edge_type", "type"][..],
                "edge type",
                false,
            ),
            (&mapping.edge_id, &["edge_id", "id"][..], "edge id", false),
        ],
    };
    for (mapping, candidates, what, required) in structural {
        if let Some((column, name)) = pick(batch, mapping, candidates, what, required)? {
            bytes = add(bytes, utf8_conversion(column)?);
            used.insert(name);
        } else {
            bytes = add(bytes, building(null_bound(&DataType::Utf8, rows)));
        }
    }
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if used.contains(field.name())
            || field.name().starts_with("property.")
            || field.name().starts_with("present.")
        {
            continue;
        }
        let kind = column.data_type();
        if kind.is_numeric() {
            bytes = add(bytes, building(add(mul(rows, 8), mul(rows.div_ceil(8), 2))));
        } else if matches!(part, Part::Nodes) {
            if matches!(kind, DataType::Utf8 | DataType::LargeUtf8) {
                bytes = add(bytes, utf8_conversion(column)?);
                bytes = add(bytes, building(rows.div_ceil(8)));
            } else if matches!(kind, DataType::FixedSizeList(..)) {
                bytes = add(bytes, building(rows.div_ceil(8)));
            }
        }
    }
    Ok(bytes)
}

/// All-null buffers for a field missing from one staged batch. Saturation means
/// admission fails before an unrepresentable allocation is attempted.
fn null_bound(kind: &DataType, rows: usize) -> usize {
    let validity = add(rows.div_ceil(8), 64);
    let data = match kind {
        DataType::Null => 0,
        DataType::Boolean => validity,
        DataType::Utf8 | DataType::Binary => mul(add(rows, 1), 4),
        DataType::LargeUtf8 | DataType::LargeBinary => mul(add(rows, 1), 8),
        DataType::Utf8View | DataType::BinaryView => mul(rows, 16),
        DataType::FixedSizeBinary(width) => mul(rows, (*width).max(0) as usize),
        DataType::List(field) | DataType::Map(field, _) => {
            add(mul(add(rows, 1), 4), null_bound(field.data_type(), 0))
        }
        DataType::LargeList(field) => add(mul(add(rows, 1), 8), null_bound(field.data_type(), 0)),
        DataType::ListView(field) => add(mul(rows, 8), null_bound(field.data_type(), 0)),
        DataType::LargeListView(field) => add(mul(rows, 16), null_bound(field.data_type(), 0)),
        DataType::FixedSizeList(field, width) => {
            null_bound(field.data_type(), mul(rows, (*width).max(0) as usize))
        }
        DataType::Struct(fields) => fields
            .iter()
            .map(|f| null_bound(f.data_type(), rows))
            .fold(0, add),
        DataType::Union(fields, _) => add(
            mul(rows, 5),
            fields
                .iter()
                .map(|(_, f)| null_bound(f.data_type(), rows))
                .fold(0, add),
        ),
        DataType::Dictionary(key, value) => add(null_bound(key, rows), null_bound(value, 0)),
        DataType::RunEndEncoded(ends, values) => add(
            null_bound(ends.data_type(), rows),
            null_bound(values.data_type(), rows),
        ),
        other => mul(rows, other.primitive_width().unwrap_or(32)),
    };
    add(add(data, validity), 256)
}

pub(super) fn unify_bound(batches: &[RecordBatch]) -> usize {
    let mut fields = BTreeMap::new();
    for batch in batches {
        for field in batch.schema().fields() {
            fields
                .entry(field.name().clone())
                .or_insert_with(|| field.data_type().clone());
        }
    }
    batches
        .iter()
        .flat_map(|batch| {
            fields
                .iter()
                .filter(move |(name, _)| batch.column_by_name(name).is_none())
                .map(move |(_, kind)| building(null_bound(kind, batch.num_rows())))
        })
        .fold(0, add)
}

pub(super) fn sort_keys_bound(batches: &[RecordBatch], keys: &[usize]) -> usize {
    fn nested(data: &ArrayData) -> usize {
        let own = add(mul(data.get_buffer_memory_size(), 16), mul(data.len(), 128));
        let children = data.child_data().iter().map(nested).fold(0, add);
        if matches!(
            data.data_type(),
            DataType::Dictionary(..)
                | DataType::RunEndEncoded(..)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
        ) {
            add(own, mul(data.len(), children))
        } else {
            add(add(own, children), mul(view_payload(data), 16))
        }
    }
    batches
        .iter()
        .map(|batch| {
            key_bytes_bound(batch, keys).unwrap_or_else(|| {
                keys.iter()
                    .map(|&key| nested(&batch.column(key).to_data()))
                    .fold(0, add)
            })
        })
        .map(building)
        .fold(0, add)
}

/// Views can repeat the same payload without allocating another backing buffer.
/// Row encoding materializes every occurrence, so physical size is insufficient.
fn view_payload(data: &ArrayData) -> usize {
    match data.data_type() {
        DataType::Utf8View => arrow::array::make_array(data.clone())
            .as_string_view()
            .iter()
            .flatten()
            .map(str::len)
            .fold(0, add),
        DataType::BinaryView => arrow::array::make_array(data.clone())
            .as_binary_view()
            .iter()
            .flatten()
            .map(<[u8]>::len)
            .fold(0, add),
        _ => 0,
    }
}

fn copy_expansion(data: &ArrayData) -> usize {
    let children = data.child_data().iter().map(copy_expansion).fold(0, add);
    if matches!(
        data.data_type(),
        DataType::Dictionary(..)
            | DataType::RunEndEncoded(..)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
    ) {
        // Interleave may materialize repeated values, or copy backing children
        // separately into each output chunk. One complete backing copy per row
        // bounds both strategies, including nested repeating layouts.
        let backing = data
            .child_data()
            .iter()
            .map(ArrayData::get_buffer_memory_size)
            .fold(0, add);
        mul(data.len(), add(backing, children))
    } else {
        add(children, view_payload(data))
    }
}

pub(super) fn copy_bound(batches: &[RecordBatch]) -> usize {
    // Interleave allocates output buffers, including nulls/offsets absent from
    // the inputs. Account for nested null layouts and builder capacity growth.
    let nulls = batches
        .iter()
        .map(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| null_bound(field.data_type(), batch.num_rows()))
                .fold(0, add)
        })
        .fold(0, add);
    let expansion = batches
        .iter()
        .flat_map(RecordBatch::columns)
        .map(|column| copy_expansion(&column.to_data()))
        .fold(0, add);
    building(add(add(sorted_copy_bound(batches), nulls), expansion))
}

#[cfg(test)]
mod tests;
