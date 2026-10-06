//! Literal-only configuration reproduces the Scala DSL's constructor parameters.
use crate::{
    function::Function,
    matrix::{self, AffineGapParams, AlignmentParams, JaroWinklerParams},
    token::{self, MongeInnerMetric, TokenConfig},
};
use arrow_schema::DataType;
use datafusion_common::{plan_err, Result, ScalarValue};

#[derive(Debug, Clone)]
pub enum Options {
    Default,
    Token(TokenConfig),
    JaroWinkler(JaroWinklerParams),
    Alignment(AlignmentParams),
    AffineGap(AffineGapParams),
}

pub fn parse(function: Function, values: &[&ScalarValue]) -> Result<Options> {
    let error = |text: &str| {
        datafusion_common::DataFusionError::Plan(format!("{}: {text}", function.name()))
    };
    let integer = |index: usize| -> Result<i32> {
        match values.get(index) {
            Some(ScalarValue::Int32(Some(value))) => Ok(*value),
            Some(ScalarValue::Int64(Some(value))) => {
                i32::try_from(*value).map_err(|_| error("option exceeds Scala Int range"))
            }
            _ => Err(error("integer options must be non-null integer literals")),
        }
    };
    if function.token().is_some() {
        let (inner_metric, index) = if function == Function::MongeElkan {
            let inner = match values.first() {
                Some(ScalarValue::Utf8(Some(value)))
                | Some(ScalarValue::LargeUtf8(Some(value)))
                | Some(ScalarValue::Utf8View(Some(value))) => value.as_str(),
                _ => return Err(error("inner metric must be a non-null string literal")),
            };
            let metric = match inner {
                "jaro_winkler" => MongeInnerMetric::JaroWinkler,
                "jaro" => MongeInnerMetric::Jaro,
                "levenshtein" => MongeInnerMetric::Levenshtein,
                "needleman_wunsch" => MongeInnerMetric::NeedlemanWunsch,
                "smith_waterman" => MongeInnerMetric::SmithWaterman,
                _ => return Err(error("unsupported Monge-Elkan inner metric")),
            };
            (metric, 1)
        } else {
            (MongeInnerMetric::JaroWinkler, 0)
        };
        let ngram = integer(index)?;
        if ngram < 0 {
            return Err(error("ngramSize must be >= 0"));
        }
        return Ok(Options::Token(TokenConfig {
            ngram_size: ngram as usize,
            inner_metric,
        }));
    }
    match function {
        Function::JaroWinkler => {
            let scale = match values.first() {
                Some(value) if !value.is_null() && value.data_type().is_numeric() => {
                    match value.cast_to(&DataType::Float64)? {
                        ScalarValue::Float64(Some(value)) => value,
                        _ => return Err(error("prefixScale must be a non-null numeric literal")),
                    }
                }
                _ => return Err(error("prefixScale must be a non-null numeric literal")),
            };
            let cap = integer(1)?;
            // Spark's constructor permits NaN; preserve its comparison semantics.
            if scale <= 0.0 || scale > 0.25 || !(1..=10).contains(&cap) {
                return Err(error(
                    "prefixScale must be in (0, 0.25] and prefixCap in [1, 10]",
                ));
            }
            Ok(Options::JaroWinkler(JaroWinklerParams {
                prefix_scale: scale,
                prefix_cap: cap as usize,
            }))
        }
        Function::NeedlemanWunsch | Function::SmithWaterman => {
            let params = AlignmentParams {
                match_score: integer(0)?,
                mismatch_penalty: integer(1)?,
                gap_penalty: integer(2)?,
            };
            let penalties_valid = if function == Function::NeedlemanWunsch {
                params.mismatch_penalty < 0 && params.gap_penalty < 0
            } else {
                params.mismatch_penalty <= 0 && params.gap_penalty <= 0
            };
            if params.match_score <= 0 || !penalties_valid {
                return Err(error("invalid alignment scoring parameters"));
            }
            Ok(Options::Alignment(params))
        }
        Function::AffineGap => {
            let params = AffineGapParams {
                mismatch_penalty: integer(0)?,
                gap_open_penalty: integer(1)?,
                gap_extend_penalty: integer(2)?,
            };
            if params.mismatch_penalty >= 0
                || params.gap_open_penalty >= 0
                || params.gap_extend_penalty >= 0
            {
                return Err(error("affine gap penalties must be < 0"));
            }
            Ok(Options::AffineGap(params))
        }
        _ => plan_err!("{} has no configurable parameters", function.name()),
    }
}

pub fn score(function: Function, left: &str, right: &str, options: &Options) -> f64 {
    if let Some(metric) = function.token() {
        let defaults = TokenConfig::default();
        let config = match options {
            Options::Token(config) => config,
            _ => &defaults,
        };
        return token::score(metric, left, right, config);
    }
    match options {
        Options::JaroWinkler(params) => matrix::jaro_winkler_with_params(left, right, *params),
        Options::Alignment(params) if function == Function::NeedlemanWunsch => {
            matrix::needleman_wunsch_with_params(left, right, *params)
        }
        Options::Alignment(params) => matrix::smith_waterman_with_params(left, right, *params),
        Options::AffineGap(params) => matrix::affine_gap_with_params(left, right, *params),
        _ => function
            .matrix()
            .map(|metric| matrix::score(metric, left, right))
            .unwrap_or(0.0),
    }
}
