//! `DataFrameSuite.scala` "SPARK-7551: support backticks for DataFrame
//! attribute resolution" and "SPARK-50962: Avoid StringIndexOutOfBoundsException
//! in AttributeNameParser", applied to the parser the CDC definition uses for
//! proto attributes. The originals resolve the parsed parts against a
//! DataFrame; only the parsing and the error identity are ported here.

use super::*;
use crate::pipelines::autocdc::attribute::parse_attribute_name;

#[test]
fn backticked_parts_are_split_as_in_spark_7551() -> SparkResult<()> {
    let parse = parse_attribute_name;
    assert_eq!(
        parse("`a.b`.c.`d..e`.`f`")?,
        names(&["a.b", "c", "d..e", "f"])
    );
    assert_eq!(
        parse("`a  b`.c.d  e.f")?,
        names(&["a  b", "c", "d  e", "f"])
    );
    assert_eq!(parse("value")?, names(&["value"]));
    assert_eq!(parse("a b.c")?, names(&["a b", "c"]));
    assert_eq!(parse("`a``b`")?, names(&["a`b"]));
    assert_eq!(parse("`a..b`")?, names(&["a..b"]));
    assert_eq!(parse("")?, names(&[""]));
    Ok(())
}

#[test]
fn malformed_dots_and_backticks_raise_invalid_attribute_name_syntax() {
    for name in [
        "`abc.`c`",
        "`abc`..d",
        "`a`.b.",
        ".whatever",
        "a..b",
        "`a",
        "a`b`",
        "`a`b",
        ".",
    ] {
        check_error(
            parse_attribute_name(name),
            "INVALID_ATTRIBUTE_NAME_SYNTAX",
            "42601",
            &[("name", name)],
        );
    }
}
