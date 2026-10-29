use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_plan::resolver::PlanResolver;

use super::graph::{Output, analysis, quoted};
use crate::error::{SparkError, SparkResult};
use crate::proto::data_type::parse_spark_data_type;
use crate::session::SparkSession;
use crate::spark::connect::pipeline_command::{DefineOutput, define_output as out};

pub(super) fn declared_schema(output: &DefineOutput) -> SparkResult<Option<spec::Schema>> {
    let Some(out::Details::TableDetails(details)) = &output.details else {
        return Ok(None);
    };
    let Some(schema) = &details.schema else {
        return Ok(None);
    };
    let data_type: spec::DataType = match schema {
        out::table_details::Schema::SchemaDataType(data_type) => data_type.clone().try_into()?,
        out::table_details::Schema::SchemaString(text) => parse_spark_data_type(text)?,
    };
    let spec::DataType::Struct { fields } = data_type else {
        return Err(SparkError::invalid(
            "a pipeline table schema must be a struct",
        ));
    };
    Ok(Some(spec::Schema { fields }))
}

pub(super) fn infer(
    ctx: &SessionContext,
    output: &Output,
    plans: &[LogicalPlan],
) -> SparkResult<Schema> {
    let inferred = Schema::try_merge(plans.iter().map(|x| x.schema().as_arrow().clone())).map_err(
        |error| {
            analysis(
                "UNABLE_TO_INFER_PIPELINE_TABLE_SCHEMA",
                format!("{}: {error}", quoted(&output.name)),
            )
        },
    )?;
    let Some(schema) = &output.schema else {
        return Ok(inferred);
    };
    let resolver = PlanResolver::new(ctx, ctx.extension::<SparkSession>()?.plan_config()?);
    let declared = resolver.resolve_data_type_for_plan(&spec::DataType::Struct {
        fields: schema.fields.clone(),
    })?;
    let DataType::Struct(fields) = declared else {
        return Err(SparkError::internal(
            "resolved pipeline schema is not a struct",
        ));
    };
    let declared = Schema::new(fields);
    Schema::try_merge([declared.clone(), inferred.clone()]).map_err(|error| {
        analysis(
            "USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE",
            format!(
                "'{}' has a user-specified schema that is incompatible: specified {declared:?}, inferred {inferred:?}: {error}",
                output.name.join(".")
            ),
        )
    })
}
