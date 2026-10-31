use datafusion_common::Result;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_expr::{Expr, Volatility};
use sail_function::scalar::math::div_mod_lazy::SparkDivModLazy;

pub(super) fn is_constant_expression(expression: &Expr) -> Result<bool> {
    let mut visitor = ConstantVisitor {
        constant: true,
        frames: Vec::new(),
    };
    expression.visit(&mut visitor)?;
    Ok(visitor.constant)
}

enum Frame<'a> {
    Ordinary,
    Evaluator,
    Lambda(&'a [String]),
}

struct ConstantVisitor<'a> {
    constant: bool,
    frames: Vec<Frame<'a>>,
}

impl<'a> TreeNodeVisitor<'a> for ConstantVisitor<'a> {
    type Node = Expr;

    fn f_down(&mut self, expression: &'a Expr) -> Result<TreeNodeRecursion> {
        let frame = match expression {
            Expr::Literal(..)
            | Expr::Alias(..)
            | Expr::BinaryExpr(..)
            | Expr::Negative(..)
            | Expr::Cast(..)
            | Expr::TryCast(..)
            | Expr::Not(..)
            | Expr::IsNull(..)
            | Expr::IsNotNull(..)
            | Expr::IsTrue(..)
            | Expr::IsNotTrue(..)
            | Expr::IsFalse(..)
            | Expr::IsNotFalse(..)
            | Expr::IsUnknown(..)
            | Expr::IsNotUnknown(..)
            | Expr::Between(..)
            | Expr::Like(..)
            | Expr::SimilarTo(..)
            | Expr::Case(..)
            | Expr::InList(..) => Some(Frame::Ordinary),
            Expr::ScalarFunction(function)
                if function.func.signature().volatility != Volatility::Volatile
                    && function.func.as_async().is_none() =>
            {
                Some(Frame::Ordinary)
            }
            Expr::HigherOrderFunction(function)
                if (function.func.inner().as_ref() as &dyn std::any::Any)
                    .is::<SparkDivModLazy>()
                    && function.args.len() == 3
                    && function
                        .args
                        .iter()
                        .all(|arg| matches!(arg, Expr::Lambda(_))) =>
            {
                Some(Frame::Evaluator)
            }
            // Only direct arguments of the internal evaluator introduce bindings.
            // Each lambda's frame is removed before visiting its next sibling.
            Expr::Lambda(lambda) if matches!(self.frames.last(), Some(Frame::Evaluator)) => {
                Some(Frame::Lambda(&lambda.params))
            }
            Expr::LambdaVariable(variable)
                if self.frames.iter().rev().any(|frame| match frame {
                    Frame::Lambda(parameters) => parameters.contains(&variable.name),
                    _ => false,
                }) =>
            {
                Some(Frame::Ordinary)
            }
            _ => None,
        };
        if let Some(frame) = frame {
            self.frames.push(frame);
            Ok(TreeNodeRecursion::Continue)
        } else {
            self.constant = false;
            Ok(TreeNodeRecursion::Stop)
        }
    }

    fn f_up(&mut self, _expression: &'a Expr) -> Result<TreeNodeRecursion> {
        self.frames.pop();
        Ok(TreeNodeRecursion::Continue)
    }
}
