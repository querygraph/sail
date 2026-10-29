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

use crate::error::OrcError;
use crate::proto::{self, r#type::Kind};
use crate::schema::{DataType, RootDataType};

fn node(kind: Kind, children: &[u32]) -> proto::Type {
    proto::Type {
        kind: Some(kind as i32),
        subtypes: children.to_vec(),
        field_names: if kind == Kind::Struct {
            (0..children.len())
                .map(|index| format!("field_{index}"))
                .collect()
        } else {
            vec![]
        },
        ..Default::default()
    }
}

fn assert_invalid(types: &[proto::Type], expected: &str) {
    let error = RootDataType::from_proto(types).unwrap_err().to_string();
    assert!(
        error.contains(expected),
        "expected {expected:?}, got {error}"
    );
}

#[test]
fn rejects_empty_and_non_struct_roots() {
    assert!(matches!(
        RootDataType::from_proto(&[]),
        Err(OrcError::NoTypes { .. })
    ));
    assert_invalid(&[node(Kind::Int, &[])], "must have Struct type");
}

#[test]
fn rejects_missing_and_unknown_kinds_at_root_and_in_children() {
    for (raw, expected) in [
        (None, "Missing type kind"),
        (Some(-1), "Unknown type kind"),
        (Some(i32::MAX), "Unknown type kind"),
    ] {
        let invalid = proto::Type {
            kind: raw,
            ..Default::default()
        };
        assert_invalid(std::slice::from_ref(&invalid), expected);
        assert_invalid(&[node(Kind::Struct, &[1]), invalid], expected);
    }
}

#[test]
fn rejects_invalid_container_and_primitive_arities() {
    for invalid in [
        node(Kind::List, &[]),
        node(Kind::List, &[2, 3]),
        node(Kind::Map, &[]),
        node(Kind::Map, &[2]),
        node(Kind::Map, &[2, 3, 4]),
        node(Kind::Int, &[2]),
        proto::Type {
            field_names: vec![],
            ..node(Kind::Struct, &[2])
        },
    ] {
        let types = [
            node(Kind::Struct, &[1]),
            invalid,
            node(Kind::Int, &[]),
            node(Kind::String, &[]),
            node(Kind::Boolean, &[]),
        ];
        assert_invalid(&types, "Invalid subtype count");
    }
}

#[test]
fn preserves_union_arity_limits() {
    for count in [0, 127, 128] {
        let children: Vec<_> = (2..2 + count).collect();
        let mut types = vec![node(Kind::Struct, &[1]), node(Kind::Union, &children)];
        types.extend((0..count).map(|_| node(Kind::Int, &[])));
        if count <= 127 {
            RootDataType::from_proto(&types).unwrap();
        } else {
            assert_invalid(&types, "Invalid subtype count");
        }
    }
}

#[test]
fn rejects_out_of_bounds_children_and_root_references() {
    assert_invalid(&[node(Kind::Struct, &[1])], "out of bounds");
    assert_invalid(&[node(Kind::Struct, &[u32::MAX])], "out of bounds");
    assert_invalid(&[node(Kind::Struct, &[0])], "references the root");
    assert_invalid(
        &[node(Kind::Struct, &[1]), node(Kind::List, &[0])],
        "references the root",
    );
}

#[test]
fn rejects_cycles_and_shared_children() {
    for types in [
        vec![node(Kind::Struct, &[1]), node(Kind::List, &[1])],
        vec![
            node(Kind::Struct, &[1]),
            node(Kind::List, &[2]),
            node(Kind::List, &[1]),
        ],
        vec![node(Kind::Struct, &[1, 1]), node(Kind::Int, &[])],
        vec![
            node(Kind::Struct, &[1, 2]),
            node(Kind::List, &[3]),
            node(Kind::List, &[3]),
            node(Kind::Int, &[]),
        ],
    ] {
        assert_invalid(&types, "referenced more than once");
    }
}

#[test]
fn rejects_orphan_types_including_disconnected_cycles() {
    assert_invalid(
        &[node(Kind::Struct, &[]), node(Kind::Int, &[])],
        "unreachable from the root",
    );
    assert_invalid(
        &[
            node(Kind::Struct, &[]),
            node(Kind::List, &[2]),
            node(Kind::List, &[1]),
        ],
        "unreachable from the root",
    );
}

#[test]
fn depth_budget_counts_root_as_zero() {
    RootDataType::from_proto_with_max_depth(&[node(Kind::Struct, &[])], Some(0)).unwrap();
    let types = [
        node(Kind::Struct, &[1]),
        node(Kind::List, &[2]),
        node(Kind::List, &[3]),
        node(Kind::Int, &[]),
    ];
    RootDataType::from_proto_with_max_depth(&types, Some(3)).unwrap();
    RootDataType::from_proto_with_max_depth(&types, None).unwrap();
    for limit in [0, 2] {
        let error = RootDataType::from_proto_with_max_depth(&types, Some(limit))
            .unwrap_err()
            .to_string();
        assert!(error.contains("exceeding maximum schema depth"), "{error}");
    }
}

#[test]
fn validates_deep_raw_trees_iteratively_before_construction() {
    let depth = 10_000;
    let mut types = vec![node(Kind::Struct, &[1])];
    types.extend((1..depth).map(|index| node(Kind::List, &[index + 1])));
    types.push(node(Kind::Int, &[]));
    // Validate without constructing or dropping a deeply recursive DataType.
    super::validate(&types, None).unwrap();
    let error = RootDataType::from_proto_with_max_depth(&types, Some(8))
        .unwrap_err()
        .to_string();
    assert!(error.contains("exceeding maximum schema depth"), "{error}");
}

#[test]
fn constructs_valid_nested_types_with_original_column_indices() {
    let types = [
        node(Kind::Struct, &[1, 3, 6]),
        node(Kind::List, &[2]),
        node(Kind::Int, &[]),
        node(Kind::Map, &[4, 5]),
        node(Kind::String, &[]),
        node(Kind::Timestamp, &[]),
        node(Kind::Union, &[7, 9]),
        node(Kind::Struct, &[8]),
        node(Kind::Long, &[]),
        node(Kind::Boolean, &[]),
    ];
    let root = RootDataType::from_proto_with_max_depth(&types, Some(3)).unwrap();
    assert_eq!(root.children().len(), 3);
    assert_eq!(root.children()[1].name(), "field_1");
    assert!(matches!(
        root.children()[0].data_type(),
        DataType::List {
            column_index: 1,
            ..
        }
    ));
    assert!(matches!(
        root.children()[1].data_type(),
        DataType::Map {
            column_index: 3,
            ..
        }
    ));
    assert!(matches!(
        root.children()[2].data_type(),
        DataType::Union {
            column_index: 6,
            ..
        }
    ));
    assert!((0..types.len()).all(|index| root.contains_column_index(index)));
    assert!(!root.contains_column_index(types.len()));
}
