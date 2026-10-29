use datafusion_expr::{cast, col, lit};

use super::*;

#[test]
fn ansi_string_cast_preserves_nested_cast_rename_decisions() -> PlanResult<()> {
    let function = ScalarUDF::from(SparkAnsiStringCast::try_new(DataType::Int32, "INT".into())?);
    for input in [
        col("value"),
        lit("12"),
        col("value").alias("renamed"),
        cast(col("value"), DataType::Utf8),
    ] {
        let legacy = cast(input.clone(), DataType::Int32);
        let ansi = function.call(vec![input]);
        assert_eq!(need_rename_cast(&ansi), need_rename_cast(&legacy));
        assert_eq!(
            need_rename_cast(&cast(ansi, DataType::Utf8)),
            need_rename_cast(&cast(legacy, DataType::Utf8)),
        );
    }
    Ok(())
}
