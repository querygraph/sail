use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Field, Time64NanosecondType, TimeUnit};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::ExprSchemable;
use sail_common::spec;
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::session::plan::PlanService;

use super::PlanResolver;
use super::state::PlanResolverState;
use crate::catalog::SparkCatalogObjectDisplay;
use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::formatter::SparkPlanFormatter;

mod casts;
mod nested_casts;

fn context() -> SessionContext {
    SessionContext::new_with_config(SessionConfig::new().with_extension(Arc::new(
        PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        ),
    )))
}

fn assert_time_field(field: &Field, precision: i32) -> PlanResult<()> {
    assert_eq!(field.data_type(), &DataType::Time64(TimeUnit::Nanosecond));
    assert_eq!(
        field.metadata().get(spec::SAIL_SPARK_TIME_METADATA_KEY),
        Some(&spec::SparkTimeMetadata::try_new(precision)?.to_json()?)
    );
    Ok(())
}

#[test]
fn spark_time_resolver_validates_semantic_precision_and_preserves_raw_types() -> PlanResult<()> {
    let context = context();
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    for precision in [-1, 7, 9] {
        assert!(
            resolver
                .resolve_data_type_for_plan(&spec::DataType::Time { precision })
                .is_err()
        );
    }
    for (logical, expected) in [
        (
            spec::DataType::Time32 {
                time_unit: spec::TimeUnit::Second,
            },
            DataType::Time32(TimeUnit::Second),
        ),
        (
            spec::DataType::Time32 {
                time_unit: spec::TimeUnit::Millisecond,
            },
            DataType::Time32(TimeUnit::Millisecond),
        ),
        (
            spec::DataType::Time64 {
                time_unit: spec::TimeUnit::Microsecond,
            },
            DataType::Time64(TimeUnit::Microsecond),
        ),
        (
            spec::DataType::Time64 {
                time_unit: spec::TimeUnit::Nanosecond,
            },
            DataType::Time64(TimeUnit::Nanosecond),
        ),
    ] {
        let field = resolver.resolve_field(
            &spec::Field {
                name: "raw".to_string(),
                data_type: logical,
                nullable: true,
                metadata: vec![],
            },
            &mut PlanResolverState::new(),
        )?;
        assert_eq!(field.data_type(), &expected);
        assert!(
            !field
                .metadata()
                .contains_key(spec::SAIL_SPARK_TIME_METADATA_KEY)
        );
    }
    Ok(())
}

#[tokio::test]
async fn spark_time_typed_null_and_alias_preserve_precision() -> PlanResult<()> {
    let context = context();
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    let schema = Arc::new(DFSchema::empty());
    for precision in 0..=6 {
        let literal = spec::data_type_to_null_literal(spec::DataType::Time { precision })?;
        let named = resolver
            .resolve_named_expression(
                spec::Expr::Alias {
                    expr: Box::new(spec::Expr::Literal(literal)),
                    name: vec!["renamed".into()],
                    metadata: Some(vec![("comment".into(), "time alias".into())]),
                },
                &schema,
                &mut PlanResolverState::new(),
            )
            .await?;
        let field = named.expr.to_field(&schema)?.1;
        assert_eq!(field.name(), "renamed");
        assert_time_field(&field, precision)?;
        assert_eq!(
            field.metadata().get("comment").map(String::as_str),
            Some("time alias")
        );
    }
    Ok(())
}

#[test]
fn spark_time_nested_typed_nulls_preserve_element_fields() -> PlanResult<()> {
    let context = context();
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    for precision in 0..=6 {
        let time = spec::DataType::Time { precision };
        for logical in [
            spec::DataType::List {
                data_type: Box::new(time.clone()),
                nullable: true,
            },
            spec::DataType::LargeList {
                data_type: Box::new(time.clone()),
                nullable: true,
            },
            spec::DataType::FixedSizeList {
                data_type: Box::new(time.clone()),
                nullable: true,
                length: 2,
            },
        ] {
            let expected = resolver.resolve_data_type_for_plan(&logical)?;
            let literal = spec::data_type_to_null_literal(logical)?;
            let scalar = resolver.resolve_literal(literal, &mut PlanResolverState::new())?;
            assert_eq!(scalar.data_type(), expected);
        }
        let logical = spec::DataType::Map {
            key_type: Box::new(time.clone()),
            value_type: Box::new(spec::DataType::List {
                data_type: Box::new(time),
                nullable: true,
            }),
            value_type_nullable: true,
            keys_sorted: false,
        };
        let expected = resolver.resolve_data_type_for_plan(&logical)?;
        let literal = spec::data_type_to_null_literal(logical)?;
        let scalar = resolver.resolve_literal(literal, &mut PlanResolverState::new())?;
        assert_eq!(scalar.data_type(), expected);
    }
    Ok(())
}

#[test]
fn spark_time_list_literals_preserve_metadata_and_nanoseconds() -> PlanResult<()> {
    let context = context();
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    for precision in 0..=6 {
        let literal = spec::Literal::List {
            data_type: spec::DataType::Time { precision },
            nullable: false,
            values: Some(vec![spec::Literal::Time {
                nanoseconds: Some(1_234_567_890),
                precision,
            }]),
        };
        let scalar = resolver.resolve_literal(literal, &mut PlanResolverState::new())?;
        let ScalarValue::List(array) = scalar else {
            return Err(PlanError::internal("expected list"));
        };
        let DataType::List(field) = array.data_type() else {
            return Err(PlanError::internal("expected list type"));
        };
        assert_time_field(field, precision)?;
        assert!(!field.is_nullable());
        assert_eq!(
            array
                .values()
                .as_primitive::<Time64NanosecondType>()
                .value(0),
            1_234_567_890
        );
    }
    Ok(())
}
