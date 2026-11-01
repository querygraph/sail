use super::*;

#[tokio::test]
async fn a_run_reproduces_the_captured_frontier_exactly_and_ignores_later_appends()
-> SparkResult<()> {
    let source = source();
    source.create_partition("p0");
    source.create_partition("p1");
    source.append("p0", (0..5).map(|i| row(i, "p0")).collect());
    source.append("p1", vec![row(100, "p1")]);
    let frontier = RunFrontier::capture(&source)?;
    // Rows appended after capture belong to the next run.
    source.append("p0", vec![row(99, "late")]);
    let (intents, outputs, progress) = drain(&source, &frontier, Progress::default(), 2).await?;
    assert_eq!(intents.len(), 3);
    assert_eq!(
        intents[0].ranges,
        vec![
            Range {
                partition: partition("p0"),
                incarnation: Incarnation("inc:1".into()),
                start: Cursor("pos:0".into()),
                end: Cursor("pos:2".into()),
            },
            Range {
                partition: partition("p1"),
                incarnation: Incarnation("inc:1".into()),
                start: Cursor("pos:0".into()),
                end: Cursor("pos:1".into()),
            },
        ]
    );
    assert_eq!(outputs, vec![vec![0, 1], vec![100], vec![2, 3], vec![4]]);
    assert_eq!(
        progress.positions.get(&partition("p0")),
        Some(&(Incarnation("inc:1".into()), Cursor("pos:5".into())))
    );
    // The next run starts where this one stopped and sees the late row.
    let next = RunFrontier::capture(&source)?;
    let (_, outputs, _) = drain(&source, &next, progress, 10).await?;
    assert_eq!(outputs, vec![vec![99]]);
    Ok(())
}

#[tokio::test]
async fn replaying_recorded_intents_yields_the_same_rows_after_appends() -> SparkResult<()> {
    let source = source();
    source.create_partition("p0");
    source.append("p0", (0..4).map(|i| row(i, "a")).collect());
    let frontier = RunFrontier::capture(&source)?;
    let (intents, first, _) = drain(&source, &frontier, Progress::default(), 3).await?;
    source.append("p0", vec![row(4, "later"), row(5, "later")]);
    // A replay re-plans nothing: it scans the recorded ranges.
    let mut replay = Vec::new();
    for intent in &intents {
        for range in &intent.ranges {
            replay.push(ids(&source, range).await?);
        }
    }
    assert_eq!(replay, first);
    assert_eq!(replay, vec![vec![0, 1, 2], vec![3]]);
    // The records round-trip through serde unchanged.
    let json = serde_json::to_string(&(&frontier, &intents))?;
    let decoded: (RunFrontier, Vec<BatchIntent>) = serde_json::from_str(&json)?;
    assert_eq!(decoded.0, frontier);
    assert_eq!(decoded.1, intents);
    Ok(())
}

#[test]
fn a_retention_gap_fails_closed() {
    let source = source();
    source.create_partition("p0");
    source.append("p0", (0..6).map(|i| row(i, "a")).collect());
    source.expire_before("p0", 3);
    let range = Range {
        partition: partition("p0"),
        incarnation: Incarnation("inc:1".into()),
        start: Cursor("pos:1".into()),
        end: Cursor("pos:4".into()),
    };
    assert_eq!(
        refused(source.scan(&range)),
        Some(ScanError::RetentionGap {
            partition: partition("p0"),
            expected: Cursor("pos:1".into()),
            first_retained: Cursor("pos:3".into()),
        })
    );
    // A new run starts at the first retained position, not at zero.
    assert_eq!(
        source.earliest(&partition("p0")).ok(),
        Some(Cursor("pos:3".into()))
    );
}

#[test]
fn a_recreated_partition_fails_ranges_captured_under_the_old_incarnation() {
    let source = source();
    let old = source.create_partition("p0");
    source.append("p0", vec![row(1, "old")]);
    let range = Range {
        partition: partition("p0"),
        incarnation: old.clone(),
        start: Cursor("pos:0".into()),
        end: Cursor("pos:1".into()),
    };
    assert!(source.scan(&range).is_ok());
    let new = source.create_partition("p0");
    assert_ne!(old, new);
    assert_eq!(
        refused(source.scan(&range)),
        Some(ScanError::IncarnationMismatch {
            partition: partition("p0"),
            expected: old,
            actual: new,
        })
    );
}

#[test]
fn a_range_past_the_available_rows_is_not_contiguous() {
    let source = source();
    source.create_partition("p0");
    source.append("p0", vec![row(1, "a"), row(2, "b")]);
    let range = Range {
        partition: partition("p0"),
        incarnation: Incarnation("inc:1".into()),
        start: Cursor("pos:1".into()),
        end: Cursor("pos:5".into()),
    };
    assert_eq!(
        refused(source.scan(&range)),
        Some(ScanError::NonContiguous {
            partition: partition("p0"),
            expected: Cursor("pos:5".into()),
            actual: Cursor("pos:2".into()),
        })
    );
}

#[test]
fn a_partition_that_disappears_with_an_unprogressed_intent_is_an_error() -> SparkResult<()> {
    let source = source();
    source.create_partition("p0");
    source.append("p0", vec![row(1, "a")]);
    let frontier = RunFrontier::capture(&source)?;
    source.remove_partition("p0");
    assert_eq!(
        refused_plan(next_intent(&source, &frontier, &Progress::default(), 10)),
        Some(ScanError::UnknownPartition {
            partition: partition("p0")
        })
    );
    Ok(())
}

#[test]
fn progress_reached_under_an_old_incarnation_is_refused_not_caught_up() -> SparkResult<()> {
    // Old run reaches pos:2; the partition is recreated and filled back to
    // pos:2; the new frontier captures the new incarnation. Old progress must
    // not make the replacement data look already consumed.
    let source = source();
    source.create_partition("p0");
    source.append("p0", vec![row(1, "old"), row(2, "old")]);
    let first = RunFrontier::capture(&source)?;
    let (_, _, progress) = drain_sync(&source, &first, Progress::default(), 10)?;
    assert_eq!(
        progress.positions.get(&partition("p0")),
        Some(&(Incarnation("inc:1".into()), Cursor("pos:2".into())))
    );
    source.create_partition("p0");
    source.append("p0", vec![row(10, "new"), row(20, "new")]);
    let second = RunFrontier::capture(&source)?;
    let Err(PlanError::IncarnationChanged {
        partition: refused_partition,
        progressed,
        captured,
    }) = next_intent(&source, &second, &progress, 10)
    else {
        return Err(SparkError::internal("expected an incarnation refusal"));
    };
    assert_eq!(refused_partition, partition("p0"));
    assert_eq!(progressed, Incarnation("inc:1".into()));
    assert_eq!(captured, Incarnation("inc:2".into()));
    // A run that starts over under the new incarnation reads the new rows.
    let intent = next_intent(&source, &second, &Progress::default(), 10)?;
    assert_eq!(
        intent.map(|i| i.ranges),
        Some(vec![Range {
            partition: partition("p0"),
            incarnation: Incarnation("inc:2".into()),
            start: Cursor("pos:0".into()),
            end: Cursor("pos:2".into()),
        }])
    );
    Ok(())
}

#[test]
fn a_zero_limit_and_a_stalling_adapter_are_errors_not_caught_up() -> SparkResult<()> {
    let source = source();
    source.create_partition("p0");
    source.append("p0", vec![row(1, "a")]);
    let frontier = RunFrontier::capture(&source)?;
    assert!(matches!(
        next_intent(&source, &frontier, &Progress::default(), 0),
        Err(PlanError::ZeroLimit)
    ));
    let stalled = Stalling(&source);
    let Err(PlanError::Stalled {
        partition: stalled_partition,
        start,
        frontier: stalled_frontier,
    }) = next_intent(&stalled, &frontier, &Progress::default(), 10)
    else {
        return Err(SparkError::internal("expected a stall refusal"));
    };
    assert_eq!(stalled_partition, partition("p0"));
    assert_eq!(start, Cursor("pos:0".into()));
    assert_eq!(stalled_frontier, Cursor("pos:1".into()));
    Ok(())
}

/// An adapter whose `plan_end` never advances.
#[derive(Debug)]
struct Stalling<'a>(&'a DeterministicSource);

impl BoundedSource for Stalling<'_> {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
    fn describe(&self) -> Result<super::super::source::SourceDescriptor, SourceError> {
        self.0.describe()
    }
    fn earliest(&self, partition: &PartitionId) -> Result<Cursor, SourceError> {
        self.0.earliest(partition)
    }
    fn frontier(&self, partition: &PartitionId) -> Result<Cursor, SourceError> {
        self.0.frontier(partition)
    }
    fn plan_end(
        &self,
        _partition: &PartitionId,
        start: &Cursor,
        _frontier: &Cursor,
        _max_rows: usize,
    ) -> Result<Cursor, SourceError> {
        Ok(start.clone())
    }
    fn scan(
        &self,
        range: &Range,
    ) -> Result<Arc<dyn datafusion::datasource::TableProvider>, SourceError> {
        self.0.scan(range)
    }
}

#[test]
fn new_and_empty_partitions_are_handled_by_the_frontier_record() -> SparkResult<()> {
    let source = source();
    source.create_partition("p0");
    source.append("p0", vec![row(1, "a")]);
    let first = RunFrontier::capture(&source)?;
    let progress = Progress::default().advance(&BatchIntent {
        ranges: vec![Range {
            partition: partition("p0"),
            incarnation: Incarnation("inc:1".into()),
            start: Cursor("pos:0".into()),
            end: Cursor("pos:1".into()),
        }],
    });
    assert_eq!(next_intent(&source, &first, &progress, 10)?, None);
    // An empty partition contributes no range; a new one starts at earliest.
    source.create_partition("empty");
    source.create_partition("p1");
    source.append("p1", vec![row(7, "new")]);
    let second = RunFrontier::capture(&source)?;
    assert_eq!(second.partitions.len(), 3);
    let intent = next_intent(&source, &second, &progress, 10)?;
    assert_eq!(
        intent.map(|i| i.ranges),
        Some(vec![Range {
            partition: partition("p1"),
            incarnation: Incarnation("inc:1".into()),
            start: Cursor("pos:0".into()),
            end: Cursor("pos:1".into()),
        }])
    );
    Ok(())
}

#[test]
fn cursors_are_opaque_to_generic_code_and_foreign_tokens_are_rejected() -> SparkResult<()> {
    let source = source();
    source.create_partition("p0");
    source.append("p0", vec![row(1, "a")]);
    // The intent planner only compares cursors for equality; the adapter
    // does the arithmetic and refuses tokens it did not mint.
    let foreign = Cursor("offset=1".into());
    assert_eq!(
        refused(source.plan_end(&partition("p0"), &foreign, &Cursor("pos:1".into()), 1)),
        Some(ScanError::ForeignCursor {
            partition: partition("p0"),
            cursor: foreign,
        })
    );
    assert_eq!(source.schema().fields().len(), 2);
    let descriptor = source.describe()?;
    assert_eq!(descriptor.kind, "deterministic");
    assert_eq!(descriptor.partitions.len(), 1);
    let json = serde_json::to_string(&descriptor).unwrap_or_default();
    assert!(json.contains("\"name\":\"events\""));
    Ok(())
}

#[test]
fn an_outage_is_not_reported_as_the_callers_mistake() {
    let unavailable = SourceError::Unavailable {
        partition: Some(partition("p0")),
        cause: "connection refused".into(),
    };
    assert!(matches!(
        SparkError::from(unavailable),
        SparkError::InternalError(message)
            if message == "streaming source: source unavailable for partition p0: connection refused"
    ));
    let refusal = SourceError::Refused(ScanError::UnknownPartition {
        partition: partition("p0"),
    });
    assert!(matches!(
        SparkError::from(refusal),
        SparkError::InvalidArgument(message)
            if message == "streaming source: partition p0 no longer exists"
    ));
}
