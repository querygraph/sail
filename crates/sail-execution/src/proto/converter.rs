use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::{FieldRef, Schema};
use datafusion::common::{Result, plan_datafusion_err, plan_err};
use datafusion::logical_expr::physical_planning_context::ScalarSubqueryResults;
use datafusion::logical_expr::{HigherOrderUDF, LambdaParametersProgress, ValueOrLambda};
use datafusion::physical_expr::expressions::{LambdaExpr, LambdaVariable};
use datafusion::physical_expr::{HigherOrderFunctionExpr, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr_with_converter;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalPlanDecodeContext, PhysicalPlanNodeExt,
    PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::{
    PhysicalExprNode, PhysicalExtensionExprNode, PhysicalPlanNode, physical_expr_node,
};
use prost::Message;

use crate::plan::r#gen::extended_physical_expr_node::ExprKind;
use crate::plan::r#gen::{
    ExtendedPhysicalExprNode, HigherOrderUdfExprNode, LambdaExprNode, LambdaVariableExprNode,
};
use crate::proto::decode::{try_decode_field_ref, try_decode_higher_order_udf};
use crate::proto::encode::{try_encode_field_ref, try_encode_higher_order_udf};

#[derive(Default)]
pub(super) struct RemotePhysicalProtoConverter {
    decoded_expressions: RefCell<HashMap<u64, Arc<dyn PhysicalExpr>>>,
}

impl Debug for RemotePhysicalProtoConverter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemotePhysicalProtoConverter")
    }
}

impl PhysicalProtoConverterExtension for RemotePhysicalProtoConverter {
    fn proto_to_execution_plan(
        &self,
        proto: &PhysicalPlanNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(results) = ctx.scalar_subquery_results() {
            let converter = ScopedPhysicalProtoConverter {
                inner: self,
                results,
            };
            converter.default_proto_to_execution_plan(proto, ctx)
        } else {
            self.default_proto_to_execution_plan(proto, ctx)
        }
    }

    fn execution_plan_to_proto(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalPlanNode> {
        PhysicalPlanNode::try_from_physical_plan_with_converter(Arc::clone(plan), codec, self)
    }

    fn proto_to_physical_expr(
        &self,
        proto: &PhysicalExprNode,
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let decoded = if let Some(expr) =
            super::native_expr::decode(proto, input_schema, ctx, self)?
        {
            expr
        } else {
            match decode_remote_expr_kind(proto)? {
                Some((ExprKind::HigherOrderUdf(node), inputs)) => {
                    self.higher_order_proto_to_expr(node, inputs, input_schema, ctx)
                }
                Some((ExprKind::LambdaVariable(node), _)) => {
                    let field = try_decode_field_ref(&node.field)?;
                    let index = usize::try_from(node.index).map_err(|_| {
                        plan_datafusion_err!(
                            "LambdaVariable index {} does not fit in usize",
                            node.index
                        )
                    })?;
                    Ok(Arc::new(LambdaVariable::new(index, field)) as Arc<dyn PhysicalExpr>)
                }
                Some((ExprKind::Lambda(node), inputs)) => {
                    let [body] = inputs else {
                        return plan_err!(
                            "LambdaExpr expects exactly one input, got {}",
                            inputs.len()
                        );
                    };
                    let body = self.proto_to_physical_expr(body, input_schema, ctx)?;
                    Ok(Arc::new(LambdaExpr::try_new(node.params, body)?) as Arc<dyn PhysicalExpr>)
                }
                _ => self.default_proto_to_physical_expr(proto, input_schema, ctx),
            }?
        };

        let Some(expression_id) = proto.expr_id else {
            return Ok(decoded);
        };

        let cached = self
            .decoded_expressions
            .borrow()
            .get(&expression_id)
            .cloned();
        if let Some(cached) = cached {
            let children = decoded.children().into_iter().cloned().collect();
            return cached.with_new_children(children);
        }

        self.decoded_expressions
            .borrow_mut()
            .insert(expression_id, Arc::clone(&decoded));
        Ok(decoded)
    }

    fn physical_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        if let Some(proto) = super::native_expr::encode(expr, codec, self)? {
            return Ok(proto);
        }
        if let Some(hof) = expr.downcast_ref::<HigherOrderFunctionExpr>() {
            return self.higher_order_expr_to_proto(expr, hof, codec);
        }
        if let Some(lambda) = expr.downcast_ref::<LambdaExpr>() {
            return self.lambda_expr_to_proto(expr, lambda, codec);
        }
        if let Some(var) = expr.downcast_ref::<LambdaVariable>() {
            let index = u32::try_from(var.index()).map_err(|_| {
                plan_datafusion_err!("LambdaVariable index {} does not fit in u32", var.index())
            })?;
            return extension_expr_to_proto(
                expr,
                ExprKind::LambdaVariable(LambdaVariableExprNode {
                    index,
                    field: try_encode_field_ref(var.field())?,
                }),
                vec![],
            );
        }
        serialize_physical_expr_with_converter(expr, codec, self)
    }
}

/// Extension codecs receive a TaskContext rather than a PhysicalPlanDecodeContext.
/// Carry the enclosing scalar scope through their converter so that decoding
/// embedded plans and expressions does not lose the shared results container.
struct ScopedPhysicalProtoConverter<'a> {
    inner: &'a RemotePhysicalProtoConverter,
    results: &'a ScalarSubqueryResults,
}

impl PhysicalProtoConverterExtension for ScopedPhysicalProtoConverter<'_> {
    fn proto_to_execution_plan(
        &self,
        proto: &PhysicalPlanNode,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx = self.restore_scalar_scope(ctx);
        self.inner.proto_to_execution_plan(proto, &ctx)
    }

    fn proto_to_physical_expr(
        &self,
        proto: &PhysicalExprNode,
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let ctx = self.restore_scalar_scope(ctx);
        self.inner.proto_to_physical_expr(proto, input_schema, &ctx)
    }

    fn execution_plan_to_proto(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalPlanNode> {
        self.inner.execution_plan_to_proto(plan, codec)
    }

    fn physical_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        self.inner.physical_expr_to_proto(expr, codec)
    }
}

impl ScopedPhysicalProtoConverter<'_> {
    fn restore_scalar_scope<'a>(
        &self,
        ctx: &PhysicalPlanDecodeContext<'a>,
    ) -> PhysicalPlanDecodeContext<'a> {
        // A nested ScalarSubqueryExec establishes its own scope, which must win
        // over the enclosing scope retained for extension codec callbacks.
        if ctx.scalar_subquery_results().is_some() {
            ctx.clone()
        } else {
            ctx.with_scalar_subquery_results(self.results.clone())
        }
    }
}

impl RemotePhysicalProtoConverter {
    fn higher_order_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        hof: &HigherOrderFunctionExpr,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        let inputs = expr
            .children()
            .into_iter()
            .map(|child| self.physical_expr_to_proto(child, codec))
            .collect::<Result<_>>()?;
        extension_expr_to_proto(
            expr,
            ExprKind::HigherOrderUdf(HigherOrderUdfExprNode {
                udf: Some(try_encode_higher_order_udf(hof)?),
            }),
            inputs,
        )
    }

    fn lambda_expr_to_proto(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        lambda: &LambdaExpr,
        codec: &dyn PhysicalExtensionCodec,
    ) -> Result<PhysicalExprNode> {
        let inputs = expr
            .children()
            .into_iter()
            .map(|child| self.physical_expr_to_proto(child, codec))
            .collect::<Result<_>>()?;
        extension_expr_to_proto(
            expr,
            ExprKind::Lambda(LambdaExprNode {
                params: lambda.params().to_vec(),
            }),
            inputs,
        )
    }

    fn higher_order_proto_to_expr(
        &self,
        node: HigherOrderUdfExprNode,
        inputs: &[PhysicalExprNode],
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let udf = node
            .udf
            .ok_or_else(|| plan_datafusion_err!("missing higher-order function UDF"))?;
        let fun = try_decode_higher_order_udf(&udf)?;
        let args = self.decode_higher_order_args(fun.as_ref(), inputs, input_schema, ctx)?;
        Ok(Arc::new(HigherOrderFunctionExpr::try_new_with_schema(
            fun,
            args,
            input_schema,
            Arc::clone(ctx.task_ctx().session_config().options()),
        )?))
    }

    fn decode_higher_order_args(
        &self,
        fun: &HigherOrderUDF,
        inputs: &[PhysicalExprNode],
        input_schema: &Schema,
        ctx: &PhysicalPlanDecodeContext<'_>,
    ) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
        let mut decoded_values = Vec::with_capacity(inputs.len());
        let mut value_or_lambda = Vec::with_capacity(inputs.len());

        for input in inputs {
            if lambda_proto_parts(input)?.is_some() {
                decoded_values.push(None);
                value_or_lambda.push(ValueOrLambda::Lambda(None));
            } else {
                let expr = self.proto_to_physical_expr(input, input_schema, ctx)?;
                value_or_lambda.push(ValueOrLambda::Value(expr.return_field(input_schema)?));
                decoded_values.push(Some(expr));
            }
        }

        // Some lambda parameter types depend on the output fields of earlier
        // lambdas. Decode known bodies first, then resolve dependent parameters.
        // The UDF signature owns the same iteration limit used by DataFusion.
        let num_lambdas = value_or_lambda
            .iter()
            .filter(|arg| matches!(arg, ValueOrLambda::Lambda(_)))
            .count();
        for step in 0..=fun.signature().lambda_parameters_max_iterations {
            let (complete, param_sets) = match fun.lambda_parameters(step, &value_or_lambda)? {
                LambdaParametersProgress::Complete(params) => {
                    (true, params.into_iter().map(Some).collect::<Vec<_>>())
                }
                LambdaParametersProgress::Partial(params) => (false, params),
            };
            // Partial results align with the present lambdas. Complete results
            // may include unused trailing parameter sets for optional lambdas,
            // as HigherOrderFunctionExpr permits (and this decoder did before).
            if (!complete && param_sets.len() != num_lambdas)
                || (complete && param_sets.len() < num_lambdas)
            {
                return plan_err!(
                    "`{}` returned {} parameter sets for {num_lambdas} lambdas",
                    fun.name(),
                    param_sets.len()
                );
            }
            let mut lambda_index = 0usize;
            for (index, input) in inputs.iter().enumerate() {
                if let Some((params, body)) = lambda_proto_parts(input)? {
                    let fields = param_sets.get(lambda_index).ok_or_else(|| {
                        plan_datafusion_err!("missing lambda parameter fields for `{}`", fun.name())
                    })?;
                    lambda_index += 1;
                    let Some(fields) = fields else {
                        // None means no new information; retain any prior field/body.
                        continue;
                    };
                    if params.len() > fields.len() {
                        return plan_err!(
                            "`{}` lambda declares {} parameters but only {} are available",
                            fun.name(),
                            params.len(),
                            fields.len()
                        );
                    }
                    let schema = extend_lambda_schema(input_schema, &params, fields);
                    let body = self.proto_to_physical_expr(body, &schema, ctx)?;
                    if !complete {
                        value_or_lambda[index] =
                            ValueOrLambda::Lambda(Some(body.return_field(&schema)?));
                    }
                    decoded_values[index] =
                        Some(Arc::new(LambdaExpr::try_new(params, body)?) as Arc<dyn PhysicalExpr>);
                }
            }
            if complete {
                let args = decoded_values
                    .into_iter()
                    .map(|arg| {
                        arg.ok_or_else(|| {
                            plan_datafusion_err!("missing decoded higher-order argument")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                return Ok(args);
            }
        }
        plan_err!(
            "`{}` exceeded its lambda parameter resolution limit",
            fun.name()
        )
    }
}

fn extension_expr_to_proto(
    expr: &Arc<dyn PhysicalExpr>,
    expr_kind: ExprKind,
    inputs: Vec<PhysicalExprNode>,
) -> Result<PhysicalExprNode> {
    let node = ExtendedPhysicalExprNode {
        expr_kind: Some(expr_kind),
    };
    Ok(PhysicalExprNode {
        expr_id: expr.expression_id(),
        expr_type: Some(physical_expr_node::ExprType::Extension(
            PhysicalExtensionExprNode {
                expr: node.encode_to_vec(),
                inputs,
            },
        )),
    })
}

fn decode_remote_expr_kind(
    proto: &PhysicalExprNode,
) -> Result<Option<(ExprKind, &[PhysicalExprNode])>> {
    let Some(physical_expr_node::ExprType::Extension(extension)) = proto.expr_type.as_ref() else {
        return Ok(None);
    };
    let node = ExtendedPhysicalExprNode::decode(extension.expr.as_slice())
        .map_err(|e| plan_datafusion_err!("failed to decode physical expr: {e}"))?;
    let expr_kind = node
        .expr_kind
        .ok_or_else(|| plan_datafusion_err!("missing physical expr node"))?;
    Ok(Some((expr_kind, extension.inputs.as_slice())))
}

fn lambda_proto_parts(
    proto: &PhysicalExprNode,
) -> Result<Option<(Vec<String>, &PhysicalExprNode)>> {
    match decode_remote_expr_kind(proto)? {
        Some((ExprKind::Lambda(node), inputs)) => {
            let [body] = inputs else {
                return plan_err!("LambdaExpr expects exactly one input, got {}", inputs.len());
            };
            Ok(Some((node.params, body)))
        }
        _ => Ok(None),
    }
}

fn extend_lambda_schema(base: &Schema, params: &[String], fields: &[FieldRef]) -> Schema {
    let mut output: Vec<FieldRef> = base.fields().iter().map(Arc::clone).collect();
    for (name, field) in params.iter().zip(fields) {
        output.push(Arc::new(field.as_ref().clone().with_name(name)));
    }
    Schema::new_with_metadata(output, base.metadata().clone())
}

#[cfg(test)]
#[path = "converter/staged_lambda_tests.rs"]
mod staged_lambda_tests;
