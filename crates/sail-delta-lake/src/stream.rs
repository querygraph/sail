//! A bounded incremental read of a Delta table, for a triggered streaming flow.
//!
//! A flow that has read nothing starts from a snapshot of the latest version,
//! or from `startingVersion` or `startingTimestamp` in the log. After that it
//! tails the commits it has not read, taking the `Add` actions that change
//! data. The rules are those of Delta's `DeltaSource`, as measured on Spark
//! 4.2.1 with Delta 4.4.1:
//!
//! - A commit that removes data fails the read unless an option allows it:
//!   `skipChangeCommits` drops the whole commit, `ignoreChanges` keeps its
//!   `Add`s, so rewritten rows arrive again, and `ignoreDeletes` covers only a
//!   commit with no `Add`.
//! - Commits without data changes, such as `OPTIMIZE`, contribute nothing.
//! - A later commit must be readable with the latest schema. Adding a column
//!   is. Renaming or dropping one on a mapped table is not, and neither is
//!   replacing the table with a different one.
//! - The rate limit options are validated, and otherwise have no effect here,
//!   since one run reads to the end of the log in one batch.
//!
//! This module computes ranges and reads them. The caller records offsets.
use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::TableSource;
use sail_common_datafusion::datasource::OptionLayer;
use sail_data_source::options::ResolveOptions;
use serde::{Deserialize, Serialize};

use crate::change_data_feed::{self, ChangeDataFeedSource};
use crate::datasource::DeltaScanConfig;
use crate::delta_log::{LogStoreRef, get_actions};
use crate::logical::DeltaTableSource;
use crate::options::r#gen::DeltaReadOptions;
use crate::spec::{Action, Add, ColumnMappingMode, DeltaError, Metadata, TableFeature};
use crate::table::{
    DeltaSnapshot, DeltaTable, find_version_for_timestamp, load_delta_read_state,
    parse_timestamp_as_of,
};
use crate::{DeltaLakeSource, DeltaSnapshotConfig};

const PHYSICAL_NAME_KEY: &str = "delta.columnMapping.physicalName";
const SQL_STATE: &str = "XXKST";

/// A failure Delta reports under an error condition of its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaStreamFailure {
    pub condition: &'static str,
    pub sql_state: &'static str,
    pub message: String,
    /// Reported as is during analysis, not as a failed stream, and not retried.
    pub analysis: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum DeltaStreamError {
    #[error("[{}] {}", .0.condition, .0.message)]
    Failure(DeltaStreamFailure),
    #[error(transparent)]
    Other(#[from] DataFusionError),
}

impl From<DeltaError> for DeltaStreamError {
    fn from(error: DeltaError) -> Self {
        Self::Other(error.into())
    }
}

pub type DeltaStreamResult<T> = Result<T, DeltaStreamError>;

fn failure<T>(condition: &'static str, message: String) -> DeltaStreamResult<T> {
    Err(DeltaStreamError::Failure(DeltaStreamFailure {
        condition,
        sql_state: SQL_STATE,
        message,
        analysis: false,
    }))
}

fn illegal_option<T>(key: &str, value: &str, explanation: &str) -> DeltaStreamResult<T> {
    failure(
        "DELTA_ILLEGAL_OPTION",
        format!("Invalid value '{value}' for option '{key}', {explanation}"),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Start {
    Version(i64),
    Latest,
    Timestamp(String),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct StreamOptions {
    start: Option<Start>,
    ignore_changes: bool,
    ignore_deletes: bool,
    skip_change_commits: bool,
    fail_on_data_loss: bool,
    read_change_feed: bool,
    exclude: Option<String>,
}

fn parse_flag(key: &str, value: &str) -> DeltaStreamResult<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => illegal_option(key, value, "must be 'true' or 'false'"),
    }
}

/// A size such as `10g`, as Spark's byte string parser reads it.
fn is_size(value: &str) -> bool {
    let value = value.trim().to_ascii_lowercase();
    let digits = value.chars().take_while(char::is_ascii_digit).count();
    let (number, unit) = value.split_at(digits);
    !number.is_empty()
        && [
            "", "b", "k", "kb", "m", "mb", "g", "gb", "t", "tb", "p", "pb",
        ]
        .contains(&unit)
}

impl StreamOptions {
    fn parse(options: &[(String, String)]) -> DeltaStreamResult<Self> {
        let mut parsed = Self {
            fail_on_data_loss: true,
            ..Default::default()
        };
        let mut timestamp = None;
        for (key, value) in options {
            match key.to_ascii_lowercase().as_str() {
                "ignorechanges" | "ignorefiledeletion" => {
                    parsed.ignore_changes |= parse_flag(key, value)?;
                }
                "ignoredeletes" => parsed.ignore_deletes = parse_flag(key, value)?,
                "skipchangecommits" => parsed.skip_change_commits = parse_flag(key, value)?,
                "failondataloss" => parsed.fail_on_data_loss = parse_flag(key, value)?,
                "readchangefeed" | "readchangedata" => {
                    parsed.read_change_feed |= parse_flag(key, value)?;
                }
                "maxfilespertrigger" => {
                    if !value.parse::<i32>().is_ok_and(|x| x > 0) {
                        return illegal_option(key, value, "must be a positive integer");
                    }
                }
                "maxbytespertrigger" => {
                    if !is_size(value) {
                        return illegal_option(
                            key,
                            value,
                            "must be a size configuration such as '10g'",
                        );
                    }
                }
                "startingversion" => {
                    parsed.start = Some(if value == "latest" {
                        Start::Latest
                    } else {
                        match value.parse::<i64>() {
                            Ok(version) if version >= 0 => Start::Version(version),
                            _ => {
                                return illegal_option(
                                    "startingVersion",
                                    value,
                                    "must be greater than or equal to zero",
                                );
                            }
                        }
                    });
                }
                "startingtimestamp" => timestamp = Some(value.clone()),
                "excluderegex" => parsed.exclude = Some(value.clone()),
                "versionasof" | "timestampasof" => {
                    return Err(DeltaStreamError::Failure(DeltaStreamFailure {
                        condition: "DELTA_UNSUPPORTED_TIME_TRAVEL_VIEWS",
                        sql_state: "0AKDC",
                        message: "Cannot time travel views, subqueries, streams or change data \
                                  feed queries."
                            .to_string(),
                        analysis: true,
                    }));
                }
                _ => {}
            }
        }
        if let Some(timestamp) = timestamp {
            if parsed.start.is_some() {
                return failure(
                    "DELTA_STARTING_VERSION_AND_TIMESTAMP_BOTH_SET",
                    "Please either provide 'startingVersion' or 'startingTimestamp'".to_string(),
                );
            }
            parsed.start = Some(Start::Timestamp(timestamp));
        }
        Ok(parsed)
    }
}

/// How far a flow has read: every commit up to `version` of the table `table_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaStreamOffset {
    pub table_id: String,
    pub version: i64,
}

/// One batch: a snapshot of `snapshot`, or the commits from `start` to `end`.
/// Reading it again gives the same files, so a replay can use it as is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaStreamRange {
    pub table_id: String,
    pub snapshot: Option<i64>,
    pub start: i64,
    pub end: i64,
}

impl DeltaStreamRange {
    pub fn next_offset(&self) -> DeltaStreamOffset {
        DeltaStreamOffset {
            table_id: self.table_id.clone(),
            version: self.end,
        }
    }
}

/// The rows of a range.
pub struct DeltaStreamRead {
    pub source: Arc<dyn TableSource>,
    pub has_rows: bool,
}

/// A Delta table as a streaming source, at its latest version.
pub struct DeltaStream {
    options: StreamOptions,
    exclude: Option<regex::Regex>,
    read_options: DeltaReadOptions,
    latest: Arc<DeltaSnapshot>,
    log_store: LogStoreRef,
    scan_config: DeltaScanConfig,
}

impl DeltaStream {
    pub async fn open(
        ctx: &dyn Session,
        paths: Vec<String>,
        options: &[(String, String)],
    ) -> DeltaStreamResult<Self> {
        let options = StreamOptions::parse(options)?;
        let exclude = options
            .exclude
            .as_deref()
            .map(regex::Regex::new)
            .transpose()
            .map_err(|e| {
                DeltaStreamError::Failure(DeltaStreamFailure {
                    condition: "DELTA_REGEX_OPT_SYNTAX_ERROR",
                    sql_state: "2201B",
                    message: format!("Please recheck your syntax for 'excludeRegex': {e}"),
                    analysis: false,
                })
            })?;
        let url = DeltaLakeSource::parse_table_url(ctx, paths).await?;
        let read_options = DeltaReadOptions::resolve(ctx, Vec::<OptionLayer>::new())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let (latest, log_store, scan_config) =
            load_delta_read_state(ctx, url, None, read_options.clone(), true, None).await?;
        if latest
            .protocol()
            .has_reader_feature(&TableFeature::CatalogManaged)
        {
            return Err(DataFusionError::NotImplemented(
                "a streaming read of a catalog-managed Delta table".to_string(),
            )
            .into());
        }
        Ok(Self {
            options,
            exclude,
            read_options,
            latest,
            log_store,
            scan_config,
        })
    }

    fn table_id(&self) -> &str {
        self.latest.metadata().id()
    }

    fn different_table<T>(&self, other: &str) -> DeltaStreamResult<T> {
        failure(
            "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE",
            format!(
                "The streaming query was reading from an unexpected Delta table (id = '{}'). It \
                 used to read from another Delta table (id = '{other}') according to checkpoint. \
                 This may happen when you changed the code to read from a new table or you \
                 deleted and re-created a table. Please revert your change or delete your \
                 streaming query checkpoint to restart from scratch.",
                self.table_id()
            ),
        )
    }

    async fn table_at(&self, version: i64, require_files: bool) -> DeltaStreamResult<DeltaTable> {
        let mut table = DeltaTable::new(
            self.log_store.clone(),
            DeltaSnapshotConfig {
                require_files,
                ..Default::default()
            },
        );
        table.load_version(version).await?;
        Ok(table)
    }

    /// The first version of `startingTimestamp`: the first commit at or after it.
    async fn version_at(&self, timestamp: &str) -> DeltaStreamResult<i64> {
        // A bare date is its first instant.
        let parsed = parse_timestamp_as_of(timestamp)
            .or_else(|_| parse_timestamp_as_of(&format!("{timestamp} 00:00:00")));
        let Ok(datetime) = parsed else {
            return failure(
                "DELTA_TIMESTAMP_INVALID",
                format!(
                    "The provided timestamp ('{timestamp}') cannot be converted to a valid timestamp."
                ),
            );
        };
        let latest = self.latest.version();
        let mut table = self.table_at(latest, false).await?;
        let found = match find_version_for_timestamp(&mut table, datetime).await {
            Ok(version) => version,
            Err(DeltaError::MissingVersion) => return Ok(0),
            Err(error) => return Err(error.into()),
        };
        let committed = table.get_version_timestamp(found).await?;
        let version = if committed >= datetime.timestamp_millis() {
            found
        } else {
            found + 1
        };
        if version > latest {
            return failure(
                "DELTA_TIMESTAMP_GREATER_THAN_COMMIT",
                format!("Timestamp {timestamp} is after the table's latest version ({latest})."),
            );
        }
        Ok(version)
    }

    /// What this flow reads next, from `offset` to the latest version.
    /// `None` means there is nothing past the offset.
    pub async fn next_range(
        &self,
        offset: Option<&DeltaStreamOffset>,
    ) -> DeltaStreamResult<Option<DeltaStreamRange>> {
        let latest = self.latest.version();
        let (snapshot, start) = match offset {
            Some(offset) => {
                if offset.table_id != self.table_id() {
                    return self.different_table(&offset.table_id);
                }
                (None, offset.version + 1)
            }
            None => match &self.options.start {
                None => (Some(latest), latest + 1),
                Some(Start::Latest) => return Ok(None),
                Some(Start::Version(version)) => {
                    if *version > latest {
                        return failure(
                            "DELTA_VERSION_NOT_FOUND",
                            format!(
                                "Cannot time travel Delta table to version {version}. Available \
                                 versions: [0, {latest}]."
                            ),
                        );
                    }
                    (None, *version)
                }
                Some(Start::Timestamp(timestamp)) => (None, self.version_at(timestamp).await?),
            },
        };
        if self.options.read_change_feed && !change_data_feed::enabled(self.latest.metadata()) {
            let (first, last) = (snapshot.unwrap_or(start), latest + 1);
            return failure(
                "DELTA_MISSING_CHANGE_DATA",
                format!(
                    "Error getting change data for range [{first} , {last}] as change data was \
                     not recorded for version [{first}]."
                ),
            );
        }
        if snapshot.is_none() && start > latest {
            return Ok(None);
        }
        if snapshot.is_none() {
            self.check_mapped_schema(start - 1).await?;
        }
        Ok(Some(DeltaStreamRange {
            table_id: self.table_id().to_string(),
            snapshot,
            start,
            end: latest,
        }))
    }

    /// A rename or a drop on a mapped table cannot be read past. Comparing the
    /// ends of a range is what finds one, since the commit itself agrees with
    /// the latest schema.
    async fn check_mapped_schema(&self, from: i64) -> DeltaStreamResult<()> {
        if self.latest.effective_column_mapping_mode() == ColumnMappingMode::None || from < 0 {
            return Ok(());
        }
        let table = self.table_at(from, false).await?;
        let before = table.snapshot()?.schema().clone();
        if renamed_or_dropped(before.fields(), self.latest.schema().fields()) {
            return failure(
                "DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG",
                "Streaming read is not supported on tables with read-incompatible schema changes \
                 (e.g. rename or drop or datatype changes). Please provide a \
                 'schemaTrackingLocation' to enable non-additive schema evolution for Delta \
                 stream processing."
                    .to_string(),
            );
        }
        Ok(())
    }

    fn check_metadata(&self, metadata: &Metadata, version: i64) -> DeltaStreamResult<()> {
        if metadata.id() != self.table_id() {
            return self.different_table(metadata.id());
        }
        let schema = metadata.parse_schema_arrow()?;
        let compatible = metadata.partition_columns() == self.latest.metadata().partition_columns()
            && change_data_feed::validate_read_schema(&schema, self.latest.schema(), version)
                .is_ok();
        if !compatible {
            return failure(
                "DELTA_SCHEMA_CHANGED_WITH_VERSION",
                format!(
                    "Detected schema change in version {version}: streaming source schema: \
                     {:?}. Please restart your query.",
                    self.latest.schema().fields()
                ),
            );
        }
        Ok(())
    }

    /// Whether to skip this commit, or a failure if it cannot be read.
    fn check_commit(
        &self,
        actions: &[Action],
        version: i64,
        root: &str,
    ) -> DeltaStreamResult<bool> {
        let options = &self.options;
        let mut adds = false;
        let mut removed = None;
        let mut operation = None;
        for action in actions {
            match action {
                Action::Add(add) if add.data_change => adds = true,
                Action::Remove(remove) if remove.data_change => {
                    removed.get_or_insert_with(|| remove.path.clone());
                }
                Action::Metadata(metadata) => self.check_metadata(metadata, version)?,
                Action::CommitInfo(info) => {
                    operation = info.operation.clone().map(|name| {
                        let parameters = info.operation_parameters.clone().unwrap_or_default();
                        let parameters = parameters
                            .iter()
                            .map(|(key, value)| (key.clone(), value.to_string()))
                            .collect::<BTreeMap<_, _>>();
                        format!("{name} ({parameters:?})")
                    });
                }
                _ => {}
            }
        }
        let Some(removed) = removed else {
            return Ok(false);
        };
        let allow_changes = options.ignore_changes || options.skip_change_commits;
        if adds && !allow_changes {
            return failure(
                "DELTA_SOURCE_TABLE_IGNORE_CHANGES",
                format!(
                    "Detected a data update (for example {}) in the source table at version \
                     {version}. This is currently not supported. If this is going to happen \
                     regularly and you are okay to skip changes, set the option \
                     'skipChangeCommits' to 'true'. If you would like the data update to be \
                     reflected, please restart this query with a fresh checkpoint directory or \
                     do a full refresh. The source table can be found at path {root}.",
                    operation.unwrap_or(removed)
                ),
            );
        }
        if !adds && !allow_changes && !options.ignore_deletes {
            return failure(
                "DELTA_SOURCE_IGNORE_DELETE",
                format!(
                    "Detected deleted data (for example {removed}) from streaming source at \
                     version {version}. This is currently not supported. If you'd like to ignore \
                     deletes, set the option 'ignoreDeletes' to 'true'. The source table can be \
                     found at path {root}."
                ),
            );
        }
        Ok(options.skip_change_commits)
    }

    async fn commit_files(&self, range: &DeltaStreamRange) -> DeltaStreamResult<Vec<Add>> {
        let root = self.log_store.root_uri();
        let mut files = vec![];
        for version in range.start..=range.end {
            let Some(bytes) = self.log_store.read_commit_entry(version).await? else {
                if self.options.fail_on_data_loss {
                    return failure(
                        "DELTA_LOG_FILE_NOT_FOUND_FOR_STREAMING_SOURCE",
                        format!(
                            "Commit {version} of the Delta table at {root} is missing. If you \
                             expect the log to be cleaned up, set the option 'failOnDataLoss' to \
                             'false'."
                        ),
                    );
                }
                continue;
            };
            let actions = get_actions(version, &bytes)?;
            if self.check_commit(&actions, version, &root)? {
                continue;
            }
            files.extend(actions.into_iter().filter_map(|action| match action {
                Action::Add(add) if add.data_change => Some(add),
                _ => None,
            }));
        }
        Ok(files)
    }

    async fn snapshot_files(&self, version: i64) -> DeltaStreamResult<Vec<Add>> {
        let table = self.table_at(version, true).await?;
        Ok(table.snapshot()?.adds().to_vec())
    }

    fn scan(&self, files: Vec<Add>) -> DeltaStreamResult<DeltaStreamRead> {
        let has_rows = !files.is_empty();
        let source = DeltaTableSource::try_new(
            self.latest.clone(),
            self.log_store.clone(),
            self.scan_config.clone(),
        )?
        .with_files(files);
        Ok(DeltaStreamRead {
            source: Arc::new(source),
            has_rows,
        })
    }

    fn excluded(&self, mut files: Vec<Add>) -> Vec<Add> {
        if let Some(exclude) = &self.exclude {
            files.retain(|file| !exclude.is_match(&file.path));
        }
        files
    }

    async fn read_feed(&self, range: &DeltaStreamRange) -> DeltaStreamResult<DeltaStreamRead> {
        if let Some(version) = range.snapshot {
            let files = self.excluded(self.snapshot_files(version).await?);
            let table = self.table_at(version, false).await?;
            let timestamp = table.get_version_timestamp(version).await?;
            let has_rows = !files.is_empty();
            let source = ChangeDataFeedSource::from_snapshot(
                self.latest.clone(),
                self.log_store.clone(),
                files,
                timestamp,
            )?;
            return Ok(DeltaStreamRead {
                source: Arc::new(source),
                has_rows,
            });
        }
        let options = DeltaReadOptions {
            read_change_feed: true,
            starting_version: Some(range.start),
            ending_version: Some(range.end),
            ..self.read_options.clone()
        };
        let source =
            ChangeDataFeedSource::try_new(self.latest.clone(), self.log_store.clone(), &options)
                .await?;
        let has_rows = source.has_changes();
        Ok(DeltaStreamRead {
            source: Arc::new(source),
            has_rows,
        })
    }

    /// The rows of a range, or no rows for `None`, with the latest schema.
    pub async fn read(
        &self,
        range: Option<&DeltaStreamRange>,
    ) -> DeltaStreamResult<DeltaStreamRead> {
        let Some(range) = range else {
            if self.options.read_change_feed {
                let source = ChangeDataFeedSource::from_snapshot(
                    self.latest.clone(),
                    self.log_store.clone(),
                    vec![],
                    0,
                )?;
                return Ok(DeltaStreamRead {
                    source: Arc::new(source),
                    has_rows: false,
                });
            }
            return self.scan(vec![]);
        };
        if self.options.read_change_feed {
            return self.read_feed(range).await;
        }
        let files = match range.snapshot {
            Some(version) => self.snapshot_files(version).await?,
            None => self.commit_files(range).await?,
        };
        self.scan(self.excluded(files))
    }
}

/// Whether a column of `before` is gone from `after`, by its physical name, or
/// has another logical name there.
fn renamed_or_dropped(before: &Fields, after: &Fields) -> bool {
    before.iter().any(|field| {
        let Some(physical) = field.metadata().get(PHYSICAL_NAME_KEY) else {
            return false;
        };
        let target = after
            .iter()
            .find(|x| x.metadata().get(PHYSICAL_NAME_KEY) == Some(physical));
        let Some(target) = target else {
            return true;
        };
        target.name() != field.name() || nested_changed(field, target)
    })
}

fn nested_changed(before: &Field, after: &Field) -> bool {
    match (before.data_type(), after.data_type()) {
        (DataType::Struct(before), DataType::Struct(after)) => renamed_or_dropped(before, after),
        (DataType::List(before), DataType::List(after))
        | (DataType::LargeList(before), DataType::LargeList(after))
        | (DataType::Map(before, _), DataType::Map(after, _)) => nested_changed(before, after),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::Field;

    use super::*;

    fn parse(options: &[(&str, &str)]) -> DeltaStreamResult<StreamOptions> {
        let options = options
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<Vec<_>>();
        StreamOptions::parse(&options)
    }

    fn condition(options: &[(&str, &str)]) -> Option<&'static str> {
        match parse(options) {
            Err(DeltaStreamError::Failure(failure)) => Some(failure.condition),
            _ => None,
        }
    }

    fn column(name: &str, physical: &str) -> Arc<Field> {
        Arc::new(
            Field::new(name, DataType::Int32, true).with_metadata(HashMap::from([(
                PHYSICAL_NAME_KEY.to_string(),
                physical.to_string(),
            )])),
        )
    }

    #[test]
    fn options_ignore_case_and_unknown_keys() -> DeltaStreamResult<()> {
        let parsed = parse(&[
            ("SKIPCHANGECOMMITS", "TRUE"),
            ("startingversion", "3"),
            ("no_such_option", "x"),
        ])?;
        assert!(parsed.skip_change_commits);
        assert_eq!(parsed.start, Some(Start::Version(3)));
        assert!(parsed.fail_on_data_loss);
        assert_eq!(
            parse(&[("startingVersion", "latest")])?.start,
            Some(Start::Latest)
        );
        Ok(())
    }

    #[test]
    fn ignore_file_deletion_is_ignore_changes() -> DeltaStreamResult<()> {
        assert!(parse(&[("ignoreFileDeletion", "true")])?.ignore_changes);
        Ok(())
    }

    #[test]
    fn bad_options_have_delta_conditions() {
        for options in [
            [("startingVersion", "-1")],
            [("startingVersion", "abc")],
            [("maxFilesPerTrigger", "0")],
            [("maxBytesPerTrigger", "x")],
            [("ignoreDeletes", "x")],
        ] {
            assert_eq!(condition(&options), Some("DELTA_ILLEGAL_OPTION"));
        }
        assert_eq!(
            condition(&[
                ("startingVersion", "0"),
                ("startingTimestamp", "2000-01-01")
            ]),
            Some("DELTA_STARTING_VERSION_AND_TIMESTAMP_BOTH_SET")
        );
        assert_eq!(
            condition(&[("versionAsOf", "1")]),
            Some("DELTA_UNSUPPORTED_TIME_TRAVEL_VIEWS")
        );
    }

    #[test]
    fn sizes_take_a_binary_unit() {
        for size in ["1", "1b", "10g", "10GB", "5 "] {
            assert!(is_size(size), "{size}");
        }
        for size in ["", "x", "g", "1x"] {
            assert!(!is_size(size), "{size}");
        }
    }

    #[test]
    fn a_rename_or_a_drop_is_found_by_physical_name() {
        let before = Fields::from(vec![column("a", "col-1"), column("b", "col-2")]);
        let added = Fields::from(vec![
            column("a", "col-1"),
            column("b", "col-2"),
            column("c", "col-3"),
        ]);
        let renamed = Fields::from(vec![column("a", "col-1"), column("c", "col-2")]);
        let dropped = Fields::from(vec![column("a", "col-1")]);
        assert!(!renamed_or_dropped(&before, &added));
        assert!(renamed_or_dropped(&before, &renamed));
        assert!(renamed_or_dropped(&before, &dropped));
    }

    #[test]
    fn a_nested_rename_is_found() {
        let inner = |name: &str| {
            Arc::new(
                Field::new(
                    "s",
                    DataType::Struct(Fields::from(vec![column(name, "col-2")])),
                    true,
                )
                .with_metadata(HashMap::from([(
                    PHYSICAL_NAME_KEY.to_string(),
                    "col-1".to_string(),
                )])),
            )
        };
        assert!(renamed_or_dropped(
            &Fields::from(vec![inner("a")]),
            &Fields::from(vec![inner("b")])
        ));
    }
}
