use std::ops::Range;
use std::sync::Arc;

use datafusion_common::{DataFusionError, Result, exec_datafusion_err, exec_err};
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::{FileRange, PartitionedFile};
use futures::StreamExt;
use object_store::ObjectStore;
use orc_rust::reader::metadata::{FileMetadata, read_metadata_async_with_options};

use super::super::requested_schema::OrcRequestedSchemaAdapter;
use super::super::{OrcObjectReader, OrcSchemaBridge, infer_orc_schema};
use super::OrcSource;

pub(super) struct OrcOpener {
    source: Arc<OrcSource>,
    store: Arc<dyn ObjectStore>,
    batch_size: usize,
}

impl OrcOpener {
    pub(super) fn new(
        source: Arc<OrcSource>,
        store: Arc<dyn ObjectStore>,
        batch_size: usize,
    ) -> Self {
        Self {
            source,
            store,
            batch_size,
        }
    }
}

impl FileOpener for OrcOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let source = self.source.clone();
        let store = self.store.clone();
        let batch_size = self.batch_size;
        let range = checked_range(file.range.as_ref(), file.object_meta.size)?;
        Ok(Box::pin(async move {
            let limits = source.options.resource_limits(source.pool.as_ref());
            let size = file.object_meta.size;
            let mut reader = OrcObjectReader::new(store, file.object_meta, source.pool.clone());
            let metadata = Arc::new(
                read_metadata_async_with_options(&mut reader, limits.metadata_options())
                    .await
                    .map_err(|error| DataFusionError::External(Box::new(error)))?,
            );
            // Keep the full actual and requested schemas for compatibility
            // validation. Required output columns only control subsequent reads.
            let file_schema = infer_orc_schema(&metadata, &source.session_timezone)?;
            if file_schema.fields().is_empty() {
                // Spark's full-schema positional timestamp check is vacuous for
                // an empty physical root, then requestedColumnIds returns None.
                // This differs from selecting no columns from a nonempty file:
                // that projection must still preserve the file's row count.
                return Ok(futures::stream::empty().boxed());
            }
            let adapter = OrcRequestedSchemaAdapter::try_new(
                file_schema,
                source.table_schema.file_schema().clone(),
                &source.projection.file_indices,
                source.read_case_sensitive,
            )?;
            preflight_stripes(&metadata, size, range.as_ref())?;
            let (builder, bridge) = OrcSchemaBridge::try_new_reader(
                reader,
                metadata,
                &source.session_timezone,
                &source.reader_timezone,
                Some(adapter.file_projection()),
            )?;
            if bridge.schema().as_ref() != adapter.input_schema().as_ref() {
                return exec_err!("ORC requested-schema mapping does not match the reader bridge");
            }
            let mut builder = limits.configure_reader(builder).with_batch_size(batch_size);
            if let Some(range) = range {
                builder = builder.with_file_byte_range(range);
            }
            // Native async decoding owns one active stripe. Keep it directly in
            // this stream: dropping open/read/scan futures drops retained buffers
            // and reservations, with no detached producer or prefetch queue.
            // Parsing and decoding inside one poll remain synchronous.
            Ok(builder
                .build_async()
                .map(move |batch| {
                    let batch =
                        batch.map_err(|error| DataFusionError::External(Box::new(error)))?;
                    adapter.adapt_batch(bridge.adapt_batch(batch)?)
                })
                .boxed())
        }))
    }
}

fn checked_range(range: Option<&FileRange>, size: u64) -> Result<Option<Range<usize>>> {
    let Some(range) = range else {
        return Ok(None);
    };
    let start = usize::try_from(range.start)
        .map_err(|_| exec_datafusion_err!("ORC file range start is negative or exceeds usize"))?;
    let end = usize::try_from(range.end)
        .map_err(|_| exec_datafusion_err!("ORC file range end is negative or exceeds usize"))?;
    if start > end || range.end as u64 > size {
        return exec_err!(
            "ORC file range {}..{} is invalid for object size {size}",
            range.start,
            range.end
        );
    }
    Ok(Some(start..end))
}

fn preflight_stripes(
    metadata: &FileMetadata,
    size: u64,
    range: Option<&Range<usize>>,
) -> Result<()> {
    for (index, stripe) in metadata.stripe_metadatas().iter().enumerate() {
        // Native range selection casts offsets to usize. Check every stripe,
        // even skipped ones, so a wide offset cannot wrap into the chosen range.
        let offset = usize::try_from(stripe.offset())
            .map_err(|_| exec_datafusion_err!("ORC stripe {index} offset exceeds usize"))?;
        if range.is_some_and(|range| !range.contains(&offset)) {
            continue;
        }
        let end = stripe
            .offset()
            .checked_add(stripe.index_length())
            .and_then(|value| value.checked_add(stripe.data_length()))
            .and_then(|value| value.checked_add(stripe.footer_length()))
            .ok_or_else(|| exec_datafusion_err!("ORC stripe {index} extent overflows u64"))?;
        if end > size {
            return exec_err!("ORC stripe {index} end {end} exceeds object size {size}");
        }
        usize::try_from(stripe.number_of_rows())
            .map_err(|_| exec_datafusion_err!("ORC stripe {index} row count exceeds usize"))?;
        // Only the footer is fetched as one buffer here. Index/data sections
        // contain separate streams; imposing a cap on their aggregate length
        // would reject otherwise addressable projected reads.
        if stripe.footer_length() > isize::MAX as u64 {
            return exec_err!("ORC stripe {index} footer length exceeds addressable memory");
        }
    }
    Ok(())
}
