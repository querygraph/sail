use super::*;

const NANOS: i64 = 45_296_123_456_789;
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

fn time_type(precision: Option<i32>) -> DataType {
    DataType {
        kind: Some(sdt::Kind::Time(sdt::Time {
            precision,
            type_variation_reference: 0,
        })),
    }
}

fn time_literal(nano: i64, precision: Option<i32>, data_type: Option<DataType>) -> Literal {
    Literal {
        data_type,
        literal_type: Some(LiteralType::Time(Time { nano, precision })),
    }
}

fn assert_time(literal: Literal, nano: i64, precision: i32) -> SparkResult<()> {
    let literal = spec::Literal::try_from(literal)?;
    assert_eq!(
        literal,
        spec::Literal::Time {
            nanoseconds: Some(nano),
            precision,
        }
    );
    assert_eq!(
        literal.spark_time_metadata()?,
        Some(spec::SparkTimeMetadata::try_new(precision)?)
    );
    Ok(())
}

#[test]
fn time_literal_preserves_nanoseconds_at_every_declared_precision() -> SparkResult<()> {
    for precision in 0..=6 {
        for nano in [0, 1, 999, NANOS, NANOS_PER_DAY - 1] {
            assert_time(time_literal(nano, Some(precision), None), nano, precision)?;
        }
    }
    assert_time(time_literal(NANOS, None, None), NANOS, 6)
}

#[test]
fn time_literal_root_type_overrides_payload_precision() -> SparkResult<()> {
    for precision in 0..=6 {
        for payload_precision in [None, Some(0), Some(6), Some(-1), Some(9)] {
            assert_time(
                time_literal(NANOS, payload_precision, Some(time_type(Some(precision)))),
                NANOS,
                precision,
            )?;
        }
    }
    // An explicit root TIME with no precision uses its own default, not the payload's.
    assert_time(
        time_literal(NANOS, Some(0), Some(time_type(None))),
        NANOS,
        6,
    )
}

#[test]
fn time_literal_rejects_invalid_effective_precision_and_root_kind() {
    for precision in [i32::MIN, -1, 7, 9, i32::MAX] {
        assert!(spec::Literal::try_from(time_literal(NANOS, Some(precision), None)).is_err());
        assert!(
            spec::Literal::try_from(time_literal(
                NANOS,
                Some(6),
                Some(time_type(Some(precision))),
            ))
            .is_err()
        );
    }
    for data_type in [
        DataType { kind: None },
        DataType {
            kind: Some(sdt::Kind::Integer(sdt::Integer {
                type_variation_reference: 0,
            })),
        },
    ] {
        let result = spec::Literal::try_from(time_literal(NANOS, Some(6), Some(data_type)));
        assert!(matches!(result, Err(SparkError::InvalidArgument(message))
            if message.contains("expected TIME data type")));
    }
}

#[test]
fn time_literal_rejects_nanoseconds_outside_the_day() {
    for nano in [i64::MIN, -1, NANOS_PER_DAY, i64::MAX] {
        for root_type in [None, Some(time_type(Some(3)))] {
            let result = spec::Literal::try_from(time_literal(nano, Some(6), root_type));
            assert!(matches!(result, Err(SparkError::InvalidArgument(message))
                if message.contains("nanoseconds must be within a day")));
        }
    }
}

#[test]
fn time_typed_null_uses_root_type_before_legacy_type() -> SparkResult<()> {
    for precision in 0..=6 {
        for legacy_type in [DataType { kind: None }, time_type(Some(9))] {
            let literal = Literal {
                data_type: Some(time_type(Some(precision))),
                literal_type: Some(LiteralType::Null(legacy_type)),
            };
            assert_eq!(
                spec::Literal::try_from(literal)?,
                spec::Literal::Time {
                    nanoseconds: None,
                    precision,
                }
            );
        }
    }
    let integer_type = DataType {
        kind: Some(sdt::Kind::Integer(sdt::Integer {
            type_variation_reference: 0,
        })),
    };
    assert_eq!(
        spec::Literal::try_from(Literal {
            data_type: Some(integer_type),
            literal_type: Some(LiteralType::Null(time_type(Some(3)))),
        })?,
        spec::Literal::Int32 { value: None }
    );
    assert!(
        spec::Literal::try_from(Literal {
            data_type: Some(time_type(Some(9))),
            literal_type: Some(LiteralType::Null(time_type(Some(3)))),
        })
        .is_err()
    );
    assert_eq!(
        spec::Literal::try_from(Literal {
            data_type: Some(time_type(None)),
            literal_type: Some(LiteralType::Null(time_type(Some(0)))),
        })?,
        spec::Literal::Time {
            nanoseconds: None,
            precision: 6,
        }
    );
    assert!(
        spec::Literal::try_from(Literal {
            data_type: Some(DataType { kind: None }),
            literal_type: Some(LiteralType::Null(time_type(Some(3)))),
        })
        .is_err()
    );
    Ok(())
}
