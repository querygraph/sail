# Appendix: error conditions

The conditions a pipeline can raise, with Spark's names. The client exposes a condition through `getCondition()` on the exception, and the message carries it in brackets.

## Registration

| Condition | When |
|---|---|
| `PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR` | A statement of a SQL file could not be registered; the file registers nothing. The message names the file and statement. |
| `PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT` | Two datasets have the same name. |
| `PIPELINE_DUPLICATE_IDENTIFIERS.FLOW` | Two flows have the same name. |
| `PIPELINE_DATASET_WITHOUT_FLOW` | A streaming table has no query and no flow. |
| `MULTIPART_FLOW_NAME_NOT_SUPPORTED` | `CREATE FLOW a.f`. |
| `FLOW_TARGET_NOT_DEFINED` | A flow's target is not a dataset of the pipeline. |
| `FLOW_NAME_CONFLICTS_WITH_TABLE` | A flow and a table share a name. |
| `INVALID_FLOW_QUERY_TYPE` | A sink's flow has a batch query. |
| `UNSUPPORTED_PIPELINE_SPARK_SQL_COMMAND` | A statement a SQL file may not contain, such as a bare `USE`. |
| `INVALID_STATEMENT_OR_CLAUSE` | `INSERT ... IF NOT EXISTS` under a flow. |
| `PARSE_SYNTAX_ERROR` | A statement that does not parse. |
| `INVALID_TEMP_OBJ_QUALIFIER` | A temporary view qualified with something other than `session` or `system.session`. |
| `TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS` | A temporary view with a three-part name. |
| `REQUIRES_SINGLE_PART_NAMESPACE` | A namespace with more than one part where one is required. |
| `DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED` | `append_flow(once=...)`. |
| `UNSUPPORTED_FEATURE.CREATE_PIPELINE_DATASET_QUERY_EXECUTION` | `CREATE MATERIALIZED VIEW` or `CREATE STREAMING TABLE` run as an ordinary query. |
| `UNSUPPORTED_FEATURE.CREATE_FLOW_QUERY_EXECUTION` | `CREATE FLOW` run as an ordinary query. |
| `ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION` | A query function asked the server to analyze or run a plan. |
| `PIPELINE_SPEC_UNEXPECTED_FIELD` | An unknown field in the spec. |
| `PIPELINE_STORAGE_ROOT_INVALID` | A storage root without a scheme. |

## Analysis

| Condition | When |
|---|---|
| `UNRESOLVED_COLUMN`, `UNRESOLVED_TABLE_OR_VIEW`, `TABLE_OR_VIEW_NOT_FOUND` | A query refers to something that does not exist. |
| `AMBIGUOUS_REFERENCE` | A query reads a column that a view repeats. |
| `INVALID_TEMP_OBJ_REFERENCE` | A persisted view reads a temporary view. |
| `CREATE_PERMANENT_VIEW_WITHOUT_ALIAS` | A persisted view has an unnamed column. |
| `MATERIALIZED_VIEW_WITH_MULTIPLE_QUERIES` | A materialized view has more than one flow. |
| `DUPLICATE_FLOW_SQL_CONF` | A flow and a view it reads set one key to different values. |
| `CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE` | A streaming table's flows, or a flow and the table, disagree on a column's type. |
| `USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE` | A declared schema does not match the query. |
| `COLUMN_NOT_DEFINED_IN_TABLE` | A partition or cluster column is not in the query. |
| `ALL_PARTITION_COLUMNS_NOT_ALLOWED` | Every column is a partition column. |
| `SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED` | Both `cluster_by` and `partition_cols`. |
| `CANNOT_UPDATE_PARTITION_COLUMNS` | A refresh would change an existing table's partition columns. |
| `UNABLE_TO_INFER_PIPELINE_TABLE_SCHEMA` | A dataset's schema could not be determined. |
| `STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION` | A streaming query shape append mode does not allow, such as an aggregate without a watermark. |
| `CATALOG_NOT_FOUND` | `SET CATALOG` or a name names a catalog that does not exist. |
| `DATA_SOURCE_NOT_FOUND` | A `format` no source provides. |
| `PATH_NOT_FOUND` | A file path that does not exist. |

## Execution

| Condition | When |
|---|---|
| `TABLE_MATERIALIZATION_FAILED` | A table could not be created or altered. |
| `RUN_EMPTY_PIPELINE` | A run with no dataset to refresh. |
| `SPARK_CONNECT_ILLEGAL_STATE.EXECUTION_STATE_EXECUTE_HOLDER_ALREADY_EXISTS_GRAPH` | A second run on a graph whose run is in progress. |
| `STREAM_FAILED` | A streaming source failed; the message says why (for a Delta source, one of the `DELTA_*` conditions of the sources chapter). |
| `INVALID_HANDLE.SESSION_CLOSED` | A request on a released session. |
| `DIVIDE_BY_ZERO`, `REMAINDER_BY_ZERO`, `CAST_INVALID_INPUT`, `NUMERIC_VALUE_OUT_OF_RANGE` | ANSI arithmetic and casts, as in Spark. |

AutoCDC's conditions (`AUTOCDC_*`) and Iggy's are listed in their chapters.
