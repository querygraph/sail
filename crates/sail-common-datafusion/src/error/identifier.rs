use std::collections::HashMap;

use datafusion_common::{DataFusionError, Result};

use super::StructuredSparkError;

/// Format a Spark toSQLId(String) diagnostic identifier, interpreting dots and
/// backticks with AttributeNameParser syntax. Malformed input keeps Spark details.
pub fn quote_sql_identifier(name: &str) -> Result<String> {
    let invalid = || {
        DataFusionError::External(Box::new(StructuredSparkError {
            class: "org.apache.spark.sql.AnalysisException".into(),
            condition: "INVALID_ATTRIBUTE_NAME_SYNTAX".into(),
            sql_state: "42601".into(),
            message: format!(
                "Syntax error in the attribute name: {name}. Check that backticks appear in pairs, a quoted string is a complete name part and use a backtick only inside quoted name parts."
            ),
            parameters: HashMap::from([("name".into(), name.into())]),
        }))
    };
    let mut parts = vec![];
    let mut part = String::new();
    let mut quoted = false;
    let mut previous = None;
    let mut chars = name.chars().peekable();
    while let Some(c) = chars.next() {
        if quoted {
            if c == '`' {
                if chars.peek() == Some(&'`') {
                    part.push('`');
                    chars.next();
                } else {
                    quoted = false;
                    if chars.peek().is_some_and(|next| *next != '.') {
                        return Err(invalid());
                    }
                }
            } else {
                part.push(c);
            }
        } else if c == '`' {
            if !part.is_empty() {
                return Err(invalid());
            }
            quoted = true;
        } else if c == '.' {
            if previous.is_none() || previous == Some('.') || chars.peek().is_none() {
                return Err(invalid());
            }
            parts.push(std::mem::take(&mut part));
        } else {
            part.push(c);
        }
        previous = Some(c);
    }
    if quoted {
        return Err(invalid());
    }
    parts.push(part);
    if parts.len() > 1 && parts[0] == "__auto_generated_subquery_name" {
        parts.remove(0);
    }
    Ok(parts
        .iter()
        .map(|p| format!("`{}`", p.replace('`', "``")))
        .collect::<Vec<_>>()
        .join("."))
}
