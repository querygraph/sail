#![expect(clippy::unwrap_used)]

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryReservation, UnboundedMemoryPool,
};
use datafusion::prelude::SessionContext;
use orc_rust::reader::metadata::read_metadata_with_options;

use super::*;
use crate::error::DataSourceError;
use crate::options::option_list;

const KEYS: [&str; 4] = [
    "max_encoded_metadata_bytes",
    "max_decoded_metadata_bytes",
    "max_decoded_stripe_footer_bytes",
    "max_schema_depth",
];
const FILE: &[u8] = include_bytes!("../../../tests/fixtures/orc/time_precisions.orc");

fn resolve(layers: Vec<OptionLayer>) -> DataSourceResult<OrcReadOptions> {
    OrcReadOptions::resolve(&SessionContext::new().state(), layers)
}

fn configured(options: &OrcReadOptions) -> [usize; 4] {
    [
        options.max_encoded_metadata_bytes,
        options.max_decoded_metadata_bytes,
        options.max_decoded_stripe_footer_bytes,
        options.max_schema_depth,
    ]
}

fn effective(limits: OrcReadResourceLimits) -> [usize; 4] {
    let metadata = limits.metadata_options();
    [
        metadata.max_encoded_metadata_bytes.unwrap(),
        metadata.max_decoded_metadata_bytes.unwrap(),
        limits.max_decoded_stripe_footer_bytes(),
        metadata.max_schema_depth.unwrap(),
    ]
}

#[test]
fn defaults_and_layer_precedence_preserve_explicit_zero() {
    let defaults = resolve(vec![]).unwrap();
    assert_eq!(
        configured(&defaults),
        [64 * 1024 * 1024, 64 * 1024 * 1024, 64 * 1024 * 1024, 64]
    );

    let options = resolve(vec![
        option_list(&[
            ("maxEncodedMetadataBytes", "1"),
            ("maxDecodedMetadataBytes", "2"),
            ("maxDecodedStripeFooterBytes", "3"),
            ("maxSchemaDepth", "4"),
        ]),
        OptionLayer::TablePropertyList {
            items: [
                ("option.max_encoded_metadata_bytes", "11"),
                ("option.maxDecodedMetadataBytes", "22"),
                ("option.max_decoded_stripe_footer_bytes", "33"),
                ("option.maxSchemaDepth", "0"),
            ]
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect(),
        },
        option_list(&[("max_encoded_metadata_bytes", "0")]),
    ])
    .unwrap();
    assert_eq!(configured(&options), [0, 22, 33, 0]);
    assert_eq!(
        effective(options.resource_limits(&UnboundedMemoryPool::default())),
        [0, 22, 33, 0]
    );

    let zeros = resolve(vec![option_list(&KEYS.map(|key| (key, "0")))]).unwrap();
    assert_eq!(configured(&zeros), [0; 4]);
    assert_eq!(
        effective(zeros.resource_limits(&UnboundedMemoryPool::default())),
        [0; 4]
    );
}

#[test]
fn every_limit_rejects_invalid_integers_and_platform_width_overflow() {
    let overflow = ((usize::MAX as u128) + 1).to_string();
    for key in KEYS {
        for value in ["-1", "-0", "1.5", "NaN", "1e3", "", overflow.as_str()] {
            let error = resolve(vec![option_list(&[(key, value)])]).unwrap_err();
            assert!(matches!(
                error,
                DataSourceError::InvalidOption { key: invalid_key, value: invalid_value, .. }
                    if invalid_key == key && invalid_value == value
            ));
        }
    }
    let max = usize::MAX.to_string();
    let options = resolve(vec![option_list(&KEYS.map(|key| (key, max.as_str())))]).unwrap();
    assert_eq!(configured(&options), [usize::MAX; 4]);
    assert_eq!(
        effective(options.resource_limits(&UnboundedMemoryPool::default())),
        [
            isize::MAX as usize,
            isize::MAX as usize,
            isize::MAX as usize,
            usize::MAX
        ]
    );
}

/// Uses the MemoryPool trait's default Unknown limit while delegating actual
/// accounting to an unbounded pool.
#[derive(Debug, Default)]
struct UnknownLimitPool(UnboundedMemoryPool);

impl fmt::Display for UnknownLimitPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UnknownLimitPool")
    }
}

impl MemoryPool for UnknownLimitPool {
    fn name(&self) -> &str {
        "UnknownLimitPool"
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.0.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.0.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion_common::Result<()> {
        self.0.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.0.reserved()
    }
}

#[test]
fn local_pool_limit_caps_bytes_without_changing_depth_or_configured_values() {
    let options = resolve(vec![option_list(&[
        (KEYS[0], "100"),
        (KEYS[1], "200"),
        (KEYS[2], "300"),
        (KEYS[3], "64"),
    ])])
    .unwrap();
    assert_eq!(
        effective(options.resource_limits(&GreedyMemoryPool::new(250))),
        [100, 200, 250, 64]
    );
    assert_eq!(
        effective(options.resource_limits(&GreedyMemoryPool::new(0))),
        [0, 0, 0, 64]
    );
    assert_eq!(
        effective(options.resource_limits(&UnboundedMemoryPool::default())),
        [100, 200, 300, 64]
    );
    assert_eq!(
        effective(options.resource_limits(&UnknownLimitPool::default())),
        [100, 200, 300, 64]
    );
    assert_eq!(configured(&options), [100, 200, 300, 64]);
}

#[test]
fn live_reservations_do_not_change_reader_policy() {
    let options = resolve(vec![]).unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2048));
    let before = effective(options.resource_limits(pool.as_ref()));
    let reservation = MemoryConsumer::new("existing reader").register(&pool);
    reservation.try_grow(2048).unwrap();
    assert_eq!(pool.reserved(), 2048);
    assert_eq!(effective(options.resource_limits(pool.as_ref())), before);
    assert_eq!(before, [2048, 2048, 2048, 64]);
    drop(reservation);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn actual_metadata_reads_apply_each_file_tail_limit_and_root_depth_zero() {
    let pool = UnboundedMemoryPool::default();
    for depth in [1, 64] {
        let options = resolve(vec![option_list(&[(
            "max_schema_depth",
            &depth.to_string(),
        )])])
        .unwrap();
        let mut bytes = Bytes::from_static(FILE);
        let metadata = read_metadata_with_options(
            &mut bytes,
            options.resource_limits(&pool).metadata_options(),
        )
        .unwrap();
        assert!(metadata.number_of_rows() > 0);
    }
    for key in [KEYS[0], KEYS[1]] {
        for value in ["0", "1"] {
            let options = resolve(vec![option_list(&[(key, value)])]).unwrap();
            let mut bytes = Bytes::from_static(FILE);
            let error = read_metadata_with_options(
                &mut bytes,
                options.resource_limits(&pool).metadata_options(),
            )
            .unwrap_err();
            let expected = if key == KEYS[0] {
                "Out of spec, message: Encoded footer and metadata exceed the configured limit"
            } else {
                "Failed to read, source: Out of spec, message: Decoded compression block exceeds the allowed output size"
            };
            assert_eq!(error.to_string(), expected, "{key}={value}");
        }
    }
    let options = resolve(vec![option_list(&[("max_schema_depth", "0")])]).unwrap();
    let mut bytes = Bytes::from_static(FILE);
    let error = read_metadata_with_options(
        &mut bytes,
        options.resource_limits(&pool).metadata_options(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("exceeding maximum schema depth 0"),
        "{error}"
    );
}

#[test]
fn actual_stripe_footer_reads_apply_the_independent_budget() {
    let pool = UnboundedMemoryPool::default();
    let defaults = resolve(vec![]).unwrap();
    let limits = defaults.resource_limits(&pool);
    let mut bytes = Bytes::from_static(FILE);
    let metadata =
        Arc::new(read_metadata_with_options(&mut bytes, limits.metadata_options()).unwrap());
    let expected_rows = usize::try_from(metadata.number_of_rows()).unwrap();
    let builder =
        ArrowReaderBuilder::new_with_metadata(bytes.clone(), metadata.clone()).with_batch_size(1);
    let rows = limits
        .configure_reader(builder)
        .build()
        .map(|batch| {
            let batch = batch.unwrap();
            assert!(batch.num_rows() <= 1);
            batch.num_rows()
        })
        .sum::<usize>();
    assert_eq!(rows, expected_rows);
    assert!(rows > 0);

    for value in ["0", "1"] {
        let options = resolve(vec![option_list(&[(KEYS[2], value)])]).unwrap();
        let builder = ArrowReaderBuilder::new_with_metadata(bytes.clone(), metadata.clone());
        let error = options
            .resource_limits(&pool)
            .configure_reader(builder)
            .build()
            .next()
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "External error: Failed to read, source: Out of spec, message: Decoded compression block exceeds the allowed output size",
            "{value}"
        );
    }
}
