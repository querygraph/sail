use super::*;

#[test]
fn empty_casts_also_require_admission_before_their_builder_allocations() {
    let registry = SessionRegistry::new(1);
    let mapping = ColumnMapping::default();
    let mut tx = registry.replacing("g", &mapping, &mapping, StageOrder::Canonical);
    let empty = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::UInt32,
        false,
    )])));
    assert!(matches!(
        tx.push_nodes(&empty),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(registry.memory().unwrap().used_bytes, 0);
}

#[test]
fn expanding_integer_ids_are_refused_before_normalization_and_leave_no_admission() {
    let registry = SessionRegistry::new(5 << 20);
    let input = RecordBatch::try_from_iter([(
        "id",
        Arc::new(arrow::array::UInt32Array::from(
            (0..1_000_000).collect::<Vec<u32>>(),
        )) as ArrayRef,
    )])
    .unwrap();
    assert!(input.column(0).to_data().get_slice_memory_size().unwrap() < 5 << 20);
    let mapping = ColumnMapping::default();
    let mut tx = registry.replacing("g", &mapping, &mapping, StageOrder::Canonical);
    let error = tx.push_nodes(&input).unwrap_err();
    assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
    assert!(error.to_string().contains("before allocating"));
    assert_eq!(registry.memory().unwrap().used_bytes, 0);
    assert!(registry.list().unwrap().is_empty());
}

#[test]
fn normalized_output_is_within_preallocation_bounds_for_expanding_types() {
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(arrow::array::UInt32Array::from(vec![0, 123, u32::MAX])),
        Arc::new(arrow::array::Float64Array::from(vec![
            f64::MIN_POSITIVE,
            -f64::MAX,
            0.0,
        ])),
        Arc::new(arrow::array::LargeStringArray::from(vec![
            "", "abc", "longer",
        ])),
        Arc::new(arrow::array::StringViewArray::from(vec![
            "",
            "abc",
            "longer than an inline view",
        ])),
        Arc::new(arrow::array::Int8Array::from(vec![i8::MIN, 0, i8::MAX])),
        Arc::new(arrow::array::ListArray::from_iter_primitive::<
            arrow::datatypes::Int32Type,
            _,
            _,
        >([
            Some(vec![Some(i32::MIN), Some(0)]),
            Some(vec![Some(i32::MAX)]),
            Some(vec![]),
        ])),
        Arc::new(
            arrow::array::DictionaryArray::<arrow::datatypes::Int8Type>::try_new(
                arrow::array::Int8Array::from(vec![0, 0, 1]),
                Arc::new(arrow::array::LargeStringArray::from(vec![
                    "a long dictionary value",
                    "second",
                ])),
            )
            .unwrap(),
        ),
    ];
    let dictionary = arrays.last().unwrap().clone();
    arrays.push(Arc::new(
        arrow::array::FixedSizeListArray::try_new(
            Arc::new(Field::new("item", dictionary.data_type().clone(), true)),
            1,
            dictionary,
            None,
        )
        .unwrap(),
    ));
    for array in arrays {
        let input = RecordBatch::try_from_iter([("id", array)]).unwrap();
        let mapping = ColumnMapping::default();
        let bound = normalization_bound(&input, Part::Nodes, &mapping).unwrap();
        let normalized = normalize_nodes(&input, &mapping).unwrap();
        assert!(held_bytes(&[normalized]) <= bound, "{:?}", input.schema());
    }
}

#[test]
fn repeated_view_and_encoded_values_are_admitted_at_their_expanded_size() {
    use arrow::array::{Int32Array, ListViewArray, RunArray, StringViewArray};
    let text = "x".repeat(8192);
    let one = StringViewArray::from(vec![text.as_str()]);
    let views = StringViewArray::try_new(
        vec![one.views()[0]; 2048].into(),
        one.data_buffers().to_vec(),
        None,
    )
    .unwrap();
    let strings = Arc::new(StringArray::from(vec![text.as_str()])) as ArrayRef;
    let lists = ListViewArray::new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        vec![0_i32; 256].into(),
        vec![1_i32; 256].into(),
        strings.clone(),
        None,
    );
    let runs = RunArray::<arrow::datatypes::Int32Type>::try_new(
        &Int32Array::from(vec![256]),
        strings.as_ref(),
    )
    .unwrap();
    for column in [
        Arc::new(views) as ArrayRef,
        Arc::new(lists) as ArrayRef,
        Arc::new(runs) as ArrayRef,
    ] {
        let batch = RecordBatch::try_from_iter([("property.repeated", column.clone())]).unwrap();
        let batches = [batch];
        let converter =
            RowConverter::new(vec![SortField::new(column.data_type().clone())]).unwrap();
        let encoded = converter
            .convert_columns(std::slice::from_ref(&column))
            .unwrap();
        assert!(
            encoded.size() > 8 * column.get_buffer_memory_size(),
            "fixture must exercise logical expansion: {}",
            column.data_type()
        );
        assert!(
            encoded.size() <= sort_keys_bound(&batches, &[0]),
            "sort bound for {}",
            column.data_type()
        );
        let indices: Vec<_> = (0..column.len()).rev().map(|row| (0, row)).collect();
        let copied = interleave(&[column.as_ref()], &indices).unwrap();
        assert!(
            copied.get_buffer_memory_size() <= copy_bound(&batches),
            "copy bound for {}",
            column.data_type()
        );
    }
}
