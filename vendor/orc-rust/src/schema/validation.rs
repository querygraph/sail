// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use snafu::{ensure, OptionExt};

use crate::error::{NoTypesSnafu, Result, UnexpectedSnafu};
use crate::proto::{self, r#type::Kind};

/// Read the stored kind without protobuf's fallback to Boolean for missing or
/// unknown enum values.
pub(super) fn kind(ty: &proto::Type, column_index: usize) -> Result<Kind> {
    let raw = ty.kind.context(UnexpectedSnafu {
        msg: format!("Missing type kind for column index {column_index}"),
    })?;
    Kind::try_from(raw).map_err(|_| {
        UnexpectedSnafu {
            msg: format!("Unknown type kind {raw} for column index {column_index}"),
        }
        .build()
    })
}

/// Validate the entire raw type tree without recursion. The root has depth 0.
pub(super) fn validate(types: &[proto::Type], max_depth: Option<usize>) -> Result<()> {
    ensure!(!types.is_empty(), NoTypesSnafu {});
    ensure!(
        kind(&types[0], 0)? == Kind::Struct,
        UnexpectedSnafu {
            msg: "Root column index 0 must have Struct type".to_string()
        }
    );

    let mut seen = vec![false; types.len()];
    seen[0] = true;
    let mut pending = vec![(0, 0usize)];
    while let Some((column_index, depth)) = pending.pop() {
        if let Some(max_depth) = max_depth {
            ensure!(
                depth <= max_depth,
                UnexpectedSnafu {
                    msg: format!(
                        "Column index {column_index} has depth {depth}, exceeding maximum schema depth {max_depth}"
                    )
                }
            );
        }
        let ty = &types[column_index];
        let kind = kind(ty, column_index)?;
        let arity_valid = match kind {
            Kind::List => ty.subtypes.len() == 1,
            Kind::Map => ty.subtypes.len() == 2,
            Kind::Struct => ty.subtypes.len() == ty.field_names.len(),
            // Preserve the existing supported range, including empty unions.
            Kind::Union => ty.subtypes.len() <= 127,
            _ => ty.subtypes.is_empty(),
        };
        ensure!(
            arity_valid,
            UnexpectedSnafu {
                msg: format!(
                    "Invalid subtype count {} for {kind:?} column index {column_index} ({} field names)",
                    ty.subtypes.len(),
                    ty.field_names.len()
                )
            }
        );

        for &child in ty.subtypes.iter().rev() {
            let child = child as usize;
            ensure!(
                child < types.len(),
                UnexpectedSnafu {
                    msg: format!(
                        "Child column index {child} of column index {column_index} is out of bounds"
                    )
                }
            );
            ensure!(
                child != 0,
                UnexpectedSnafu {
                    msg: format!("Column index {column_index} references the root column")
                }
            );
            ensure!(
                !seen[child],
                UnexpectedSnafu {
                    msg: format!(
                        "Column index {child} is referenced more than once (cycle or multiple parents)"
                    )
                }
            );
            // Mark on discovery so siblings cannot schedule a shared child twice.
            seen[child] = true;
            let child_depth = depth.checked_add(1).context(UnexpectedSnafu {
                msg: "Schema depth overflow".to_string(),
            })?;
            pending.push((child, child_depth));
        }
    }

    if let Some(column_index) = seen.iter().position(|seen| !seen) {
        return UnexpectedSnafu {
            msg: format!("Column index {column_index} is unreachable from the root"),
        }
        .fail();
    }
    Ok(())
}

#[cfg(test)]
mod tests;
