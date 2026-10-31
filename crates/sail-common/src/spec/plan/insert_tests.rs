use super::*;

#[test]
fn replace_mode_serialization_preserves_legacy_position_default() -> Result<(), serde_json::Error> {
    let condition = Box::new(ExprWithSource {
        expr: Expr::Literal(Literal::Boolean { value: Some(true) }),
        source: None,
    });
    for by_name in [false, true] {
        let mode = InsertMode::Replace {
            condition: condition.clone(),
            by_name,
        };
        let encoded = serde_json::to_value(&mode)?;
        if by_name {
            assert_eq!(encoded["replace"]["byName"], true);
        } else {
            assert!(encoded["replace"].get("byName").is_none());
        }
        assert_eq!(serde_json::from_value::<InsertMode>(encoded)?, mode);
    }
    Ok(())
}
