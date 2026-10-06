use std::sync::Arc;

use arrow_schema::Field;
use datafusion_common::arrow::array::{
    Array, Float64Array, LargeStringArray, StringArray, StringViewArray,
};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
};
use datafusion_ffi::udf::{FFI_ScalarUDF, ForeignScalarUDF};
use serde::Deserialize;

use crate::udf;

fn foreign(name: &str) -> ScalarUDF {
    let original = udf::functions()
        .into_iter()
        .find(|function| function.name() == name)
        .unwrap();
    extern "C" fn foreign_marker() -> usize {
        0
    }
    let mut exported = FFI_ScalarUDF::from(original);
    exported.library_marker_id = foreign_marker;
    let imported: Arc<dyn ScalarUDFImpl> = exported.into();
    assert!(imported.is::<ForeignScalarUDF>());
    ScalarUDF::new_from_shared_impl(imported)
}

fn invoke(function: &ScalarUDF, args: Vec<ColumnarValue>) -> Result<ColumnarValue> {
    let fields: Vec<_> = args
        .iter()
        .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
        .collect();
    let scalars: Vec<_> = args
        .iter()
        .map(|arg| match arg {
            ColumnarValue::Scalar(value) => Some(value),
            _ => None,
        })
        .collect();
    let rows = args
        .iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            _ => None,
        })
        .unwrap_or(1);
    let return_field = function.return_field_from_args(ReturnFieldArgs {
        arg_fields: &fields,
        scalar_arguments: &scalars,
    })?;
    function.invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields: fields,
        number_rows: rows,
        return_field,
        config_options: Arc::new(ConfigOptions::new()),
    })
}

fn text(value: Option<&str>) -> ColumnarValue {
    ColumnarValue::Scalar(ScalarValue::Utf8(value.map(str::to_owned)))
}

fn scalar(value: ColumnarValue) -> ScalarValue {
    ScalarValue::try_from_array(&value.into_array(1).unwrap(), 0).unwrap()
}

#[test]
fn all_names_and_default_arities_are_preserved() {
    assert_eq!(udf::functions().len(), 26);
    assert!(invoke(&foreign("ss_jaro"), vec![text(Some("x"))]).is_err());
    assert!(invoke(
        &foreign("ss_jaccard"),
        vec![text(Some("x")), text(Some("x")), text(Some("x"))]
    )
    .is_err());
    assert!(invoke(
        &foreign("ss_soundex"),
        vec![text(Some("x")), text(Some("x"))]
    )
    .is_err());
}

#[test]
fn foreign_batches_preserve_nulls_slices_and_scalar_broadcast() {
    let values = StringArray::from(vec![
        Some("padding"),
        Some("MARTHA"),
        None,
        Some("DWAYNE"),
        Some("padding"),
    ])
    .slice(1, 3);
    let output = invoke(
        &foreign("ss_jaro_winkler"),
        vec![ColumnarValue::Array(Arc::new(values)), text(Some("MARHTA"))],
    )
    .unwrap()
    .into_array(3)
    .unwrap();
    let scores = output.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((scores.value(0) - 0.9611111111111111).abs() < 1e-12);
    assert!(scores.is_null(1));
    assert!(!scores.is_null(2));
    assert_eq!(
        scalar(
            invoke(
                &foreign("ss_levenshtein"),
                vec![text(None), text(Some("x"))]
            )
            .unwrap()
        ),
        ScalarValue::Float64(None)
    );
    assert_eq!(
        scalar(invoke(&foreign("ss_soundex"), vec![text(None)]).unwrap()),
        ScalarValue::Utf8(None)
    );
}

#[test]
fn foreign_large_and_view_strings_use_utf16_reference_semantics() {
    for input in [
        ColumnarValue::Array(Arc::new(LargeStringArray::from(vec![
            Some("😀"),
            None,
            Some(""),
        ]))),
        ColumnarValue::Array(Arc::new(StringViewArray::from(vec![
            Some("😀"),
            None,
            Some(""),
        ]))),
    ] {
        let result = invoke(&foreign("ss_levenshtein"), vec![input, text(Some("😁"))])
            .unwrap()
            .into_array(3)
            .unwrap();
        let scores = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(scores.value(0), 0.5);
        assert!(scores.is_null(1));
        assert_eq!(scores.value(2), 0.0);
    }
    let input = ColumnarValue::Array(Arc::new(StringArray::from(Vec::<Option<&str>>::new())));
    assert_eq!(
        invoke(&foreign("ss_jaccard"), vec![input.clone(), input])
            .unwrap()
            .into_array(0)
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn literal_options_survive_foreign_transport_and_invalid_domains_are_refused() {
    let options = ColumnarValue::Scalar(ScalarValue::Int64(Some(2)));
    assert_eq!(
        scalar(
            invoke(
                &foreign("ss_jaccard_with_options"),
                vec![text(Some("abcd")), text(Some("abce")), options]
            )
            .unwrap()
        ),
        ScalarValue::Float64(Some(0.5))
    );
    assert!(invoke(
        &foreign("ss_cosine_with_options"),
        vec![
            text(Some("x")),
            text(Some("y")),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(-1)))
        ]
    )
    .is_err());
    assert!(invoke(
        &foreign("ss_monge_elkan_with_options"),
        vec![
            text(Some("x")),
            text(Some("y")),
            text(Some("unknown")),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(0)))
        ]
    )
    .is_err());
    assert!(invoke(
        &foreign("ss_jaro_winkler_with_options"),
        vec![
            text(Some("x")),
            text(Some("y")),
            ColumnarValue::Scalar(ScalarValue::Float64(Some(0.3))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(4)))
        ]
    )
    .is_err());
    let nonliteral = ColumnarValue::Array(Arc::new(
        datafusion_common::arrow::array::Int64Array::from(vec![2]),
    ));
    assert!(invoke(
        &foreign("ss_jaccard_with_options"),
        vec![text(Some("x")), text(Some("y")), nonliteral]
    )
    .is_err());
}

#[derive(Deserialize)]
struct Oracle {
    pairs: Vec<Pair>,
    cases: Vec<Case>,
}
#[derive(Deserialize)]
struct Pair {
    left: String,
    right: Option<String>,
}
#[derive(Deserialize)]
struct Case {
    id: serde_json::Value,
    function: String,
    pair: usize,
    parameters: Vec<serde_json::Value>,
    expected: serde_json::Value,
}

#[test]
fn compiled_scala_oracle_matches_all_exported_functions_through_arrow_ffi() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/oracle.json");
    let oracle: Oracle = serde_json::from_str(
        &std::fs::read_to_string(path)
            .expect("generate the pinned Scala oracle before qualification"),
    )
    .unwrap();
    assert!(
        oracle.cases.len() >= 1000,
        "qualification must include the complete corpus"
    );
    let functions: std::collections::HashMap<_, _> = udf::functions()
        .iter()
        .map(|function| {
            let name = function.name().to_owned();
            (name.clone(), foreign(&name))
        })
        .collect();
    for case in &oracle.cases {
        let pair = &oracle.pairs[case.pair];
        let name = if case.parameters.is_empty() {
            case.function.clone()
        } else {
            format!("{}_with_options", case.function)
        };
        let mut args = vec![text(Some(&pair.left))];
        if !matches!(
            case.function.as_str(),
            "ss_soundex" | "ss_refined_soundex" | "ss_double_metaphone"
        ) {
            args.push(text(Some(pair.right.as_ref().expect("binary oracle pair"))));
        }
        for (index, value) in case.parameters.iter().enumerate() {
            let scalar = if let Some(value) = value.as_str() {
                ScalarValue::Utf8(Some(value.to_owned()))
            } else if case.function == "ss_jaro_winkler" && index == 0 {
                ScalarValue::Float64(value.as_f64())
            } else {
                ScalarValue::Int64(value.as_i64())
            };
            args.push(ColumnarValue::Scalar(scalar));
        }
        let actual = scalar(
            invoke(&functions[&name], args)
                .unwrap_or_else(|error| panic!("case {} {name}: {error}", case.id)),
        );
        if let Some(expected) = case.expected.as_str() {
            assert_eq!(
                actual,
                ScalarValue::Utf8(Some(expected.to_owned())),
                "case {} {name}",
                case.id
            );
        } else {
            let ScalarValue::Float64(Some(actual)) = actual else {
                panic!("case {} did not return Float64", case.id)
            };
            let expected = case.expected.as_f64().unwrap();
            assert!(
                (actual - expected).abs() <= 1e-12,
                "case {} {name}: actual={actual} reference={expected}",
                case.id
            );
        }
    }
}
