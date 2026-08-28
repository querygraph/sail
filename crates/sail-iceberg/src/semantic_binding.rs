// Licensed under the Apache License, Version 2.0.

use crate::spec::{Schema, Type};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalFieldBinding<'a> {
    pub name: &'a str,
    pub expected_type: Option<&'a Type>,
    pub nullable: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalBindingError {
    MissingField(String),
    TypeMismatch {
        field: String,
        expected: String,
        actual: String,
    },
    NullabilityMismatch {
        field: String,
        expected_nullable: bool,
        actual_nullable: bool,
    },
    ExpressionFieldMissing {
        expression: String,
        field: String,
    },
}

pub fn validate_physical_bindings(
    schema: &Schema,
    bindings: &[PhysicalFieldBinding<'_>],
    expression_fields: &[(&str, &[&str])],
) -> Result<(), Vec<PhysicalBindingError>> {
    let mut errors = Vec::new();
    for binding in bindings {
        let Some(field) = schema.field_by_name(binding.name) else {
            errors.push(PhysicalBindingError::MissingField(binding.name.to_owned()));
            continue;
        };
        if let Some(expected) = binding.expected_type {
            if field.field_type.as_ref() != expected {
                errors.push(PhysicalBindingError::TypeMismatch {
                    field: binding.name.to_owned(),
                    expected: expected.to_string(),
                    actual: field.field_type.to_string(),
                });
            }
        }
        if let Some(expected_nullable) = binding.nullable {
            let actual_nullable = !field.required;
            if expected_nullable != actual_nullable {
                errors.push(PhysicalBindingError::NullabilityMismatch {
                    field: binding.name.to_owned(),
                    expected_nullable,
                    actual_nullable,
                });
            }
        }
    }
    for (expression, fields) in expression_fields {
        for field in *fields {
            if schema.field_by_name(field).is_none() {
                errors.push(PhysicalBindingError::ExpressionFieldMissing {
                    expression: (*expression).to_owned(),
                    field: (*field).to_owned(),
                });
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{NestedField, PrimitiveType};
    use std::sync::Arc;
    fn schema() -> Schema {
        Schema::builder()
            .with_fields([
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "amount",
                    Type::Primitive(PrimitiveType::Double),
                )),
            ])
            .build()
            .unwrap()
    }
    #[test]
    fn validates_and_reports_all_drift() {
        assert!(validate_physical_bindings(
            &schema(),
            &[PhysicalFieldBinding {
                name: "amount",
                expected_type: Some(&Type::Primitive(PrimitiveType::Double)),
                nullable: Some(true)
            }],
            &[("SUM(amount)", &["amount"])]
        )
        .is_ok());
        let errors = validate_physical_bindings(
            &schema(),
            &[
                PhysicalFieldBinding {
                    name: "missing",
                    expected_type: None,
                    nullable: None,
                },
                PhysicalFieldBinding {
                    name: "id",
                    expected_type: Some(&Type::Primitive(PrimitiveType::String)),
                    nullable: Some(true),
                },
            ],
            &[("SUM(ghost)", &["ghost"])],
        )
        .unwrap_err();
        assert_eq!(errors.len(), 4);
    }
}
