use datafusion_expr::expr::HigherOrderFunction;
use datafusion_expr::{Expr, HigherOrderUDF, ScalarUDF, lambda, lit};
use sail_common_datafusion::literal::LiteralEvaluator;

use super::*;
use crate::scalar::math::div_mod_lazy::SparkDivModLazy;

#[test]
fn java_bmp_digit_mapping_and_surrogate_pair_rejection() -> Result<()> {
    #[derive(serde::Deserialize)]
    struct Mapping {
        starts: Vec<u32>,
        java_version: String,
        bmp_mapping_verified: bool,
    }
    let mapping: Mapping = serde_json::from_str(include_str!("digit-mapping.json"))
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    assert_eq!(mapping.java_version, "20.0.2");
    assert!(mapping.bmp_mapping_verified);
    assert_eq!(mapping.starts.len(), 37);
    for code in 0..=65535 {
        let Some(c) = char::from_u32(code) else {
            continue;
        };
        let expected = mapping
            .starts
            .iter()
            .find_map(|&start| code.checked_sub(start).filter(|&d| d < 10));
        match super::super::parse::cast(&c.to_string(), 1, 0, false) {
            Ok(value) => assert_eq!(Some(value), expected.map(i128::from), "U+{code:04X}"),
            Err(super::super::parse::CastError::Invalid) => {
                assert!(expected.is_none(), "U+{code:04X}")
            }
            other => {
                return exec_err!("unexpected single-character result U+{code:04X}: {other:?}");
            }
        }
    }
    for s in ["𝟙", "𐒡", "𞥐", "①", "²"] {
        assert!(matches!(
            super::super::parse::cast(s, 5, 2, false),
            Err(super::super::parse::CastError::Invalid)
        ));
    }
    Ok(())
}

#[test]
fn exponent_scale_limits_and_rounding_are_bounded() -> Result<()> {
    let f = SparkStringToDecimal::try_new(38, 2, true, false)?;
    for s in [
        "0e2147483648",
        "0.0e2147483649",
        "0.00e2147483650",
        "0e-2147483647",
        "1e-2147483647",
        "0e000000000000000000000000001",
    ] {
        assert_eq!(f.cast_value(Some(s))?, Some(0), "{s}");
    }
    for s in [
        "0e2147483649",
        "0e-2147483648",
        "1e99999999999",
        "1e-99999999999",
        "1e",
        "1e+",
        "1e-",
        "1.2.3",
        "+",
        "-",
        ".",
        "++1",
    ] {
        let error = f.cast_value(Some(s)).err().ok_or_else(|| {
            datafusion_common::exec_datafusion_err!("accepted malformed exponent {s}")
        })?;
        let CommonErrorCause::Structured(error) = CommonErrorCause::new::<NoPython>(&error) else {
            return exec_err!("unstructured {s}: {error}");
        };
        assert_eq!(error.condition, "CAST_INVALID_INPUT", "{s}");
    }
    for (s, expected) in [
        ("000.005", 1),
        ("-000.005", -1),
        ("1.234", 123),
        ("-1.235", -124),
    ] {
        assert_eq!(f.cast_value(Some(s))?, Some(expected));
    }
    let tiny = format!("0.{}5", "0".repeat(1000));
    assert_eq!(f.cast_value(Some(&tiny))?, Some(0));
    let f = SparkStringToDecimal::try_new(38, 0, true, false)?;
    let maximum = "9".repeat(38);
    assert_eq!(f.cast_value(Some(&maximum))?, Some(10_i128.pow(38) - 1));
    let f = SparkStringToDecimal::try_new(38, 38, true, false)?;
    assert_eq!(
        f.cast_value(Some(&format!("0.{maximum}")))?,
        Some(10_i128.pow(38) - 1)
    );
    assert!(f.cast_value(Some(&format!("0.{maximum}5"))).is_err());
    Ok(())
}

#[test]
fn negative_scale_policy_changes_range_errors_and_overflow_display() -> Result<()> {
    for (allow, condition) in [
        (false, "NUMERIC_OUT_OF_SUPPORTED_RANGE"),
        (true, "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION"),
    ] {
        let f = SparkStringToDecimal::try_new(5, 2, true, allow)?;
        let error = f
            .cast_value(Some("1e39"))
            .err()
            .ok_or_else(|| datafusion_common::exec_datafusion_err!("expected range error"))?;
        let CommonErrorCause::Structured(error) = CommonErrorCause::new::<NoPython>(&error) else {
            return exec_err!("unstructured: {error}");
        };
        assert_eq!(error.condition, condition);
        assert_eq!(
            error.parameters.get("value"),
            Some(&if allow {
                format!("1{}", "0".repeat(39))
            } else {
                "1e39".into()
            })
        );
    }
    let f = SparkStringToDecimal::try_new(5, 2, true, true)?;
    for (source, expected) in [
        ("0001000.000", "1000.000"),
        ("-1e1001", "-1E+1001"),
        ("1.230e2147483648", "1.230E+2147483648"),
    ] {
        let error = f
            .cast_value(Some(source))
            .err()
            .ok_or_else(|| datafusion_common::exec_datafusion_err!("expected overflow"))?;
        let CommonErrorCause::Structured(error) = CommonErrorCause::new::<NoPython>(&error) else {
            return exec_err!("unstructured: {error}");
        };
        assert_eq!(
            error.parameters.get("value").map(String::as_str),
            Some(expected)
        );
    }
    // Negative target-scale acceptance is a resolver/config decision. The pure
    // UDF handles the Arrow type; its flag governs parsed input range policy.
    let f = SparkStringToDecimal::try_new(5, -2, true, false)?;
    assert_eq!(f.cast_value(Some("149"))?, Some(1));
    assert_eq!(f.cast_value(Some("150"))?, Some(2));
    assert_eq!(f.cast_value(Some("-150"))?, Some(-2));
    Ok(())
}

#[test]
fn fields_are_nullable_type_only_and_preserve_nonextension_metadata() -> Result<()> {
    for fail in [true, false] {
        for nullable in [true, false] {
            let f = SparkStringToDecimal::try_new(5, 2, fail, false)?;
            assert_eq!(f.signature().volatility, Volatility::Immutable);
            let input = Arc::new(
                Field::new("source", DataType::Utf8, nullable).with_metadata(HashMap::from([
                    ("comment".into(), "retained".into()),
                    (EXTENSION_TYPE_NAME_KEY.into(), "arrow.json".into()),
                    (EXTENSION_TYPE_METADATA_KEY.into(), "{}".into()),
                ])),
            );
            let bad = ScalarValue::Utf8(Some("bad".into()));
            let field = f.return_field_from_args(ReturnFieldArgs {
                arg_fields: &[input],
                scalar_arguments: &[Some(&bad)],
            })?;
            assert_eq!(field.data_type(), &DataType::Decimal128(5, 2));
            assert!(field.is_nullable());
            assert_eq!(
                field.metadata(),
                &HashMap::from([("comment".into(), "retained".into())])
            );
        }
    }
    for (p, s) in [(0, 0), (39, 0), (5, 6)] {
        assert!(SparkStringToDecimal::try_new(p, s, true, false).is_err());
    }
    let f = SparkStringToDecimal::try_new(5, 2, true, false)?;
    assert!(invoke(&f, ColumnarValue::Scalar(ScalarValue::Int32(Some(1)))).is_err());
    Ok(())
}

#[test]
fn typed_failing_decimal_stays_skipped_in_the_ordered_literal_evaluator() -> Result<()> {
    let cast =
        ScalarUDF::from(SparkStringToDecimal::try_new(5, 2, true, false)?).call(vec![lit("bad")]);
    let expression = Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::new(HigherOrderUDF::new_from_impl(SparkDivModLazy::new(true))),
        vec![
            lambda(["unused_r"], lit(ScalarValue::Int32(None))),
            lambda(["unused_l"], cast),
            lambda(["l", "r"], lit(7)),
        ],
    ));
    assert_eq!(
        LiteralEvaluator::new().evaluate(&expression)?,
        ScalarValue::Int32(None)
    );
    Ok(())
}
