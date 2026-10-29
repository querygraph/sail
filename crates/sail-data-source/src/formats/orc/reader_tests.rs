#![expect(clippy::unwrap_used)]

use std::fmt;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result,
};
use orc_rust::reader::AsyncChunkReader;

use super::OrcObjectReader;

#[derive(Debug, Clone, Copy)]
enum Response {
    Normal,
    Truncated,
    TooLong,
    WrongRange,
    WrongSize,
    WrongEtag,
    WrongVersion,
}

#[derive(Debug)]
struct RecordingStore {
    inner: InMemory,
    requests: Mutex<Vec<GetOptions>>,
    response: Response,
    version: Option<String>,
    body_polls: Arc<AtomicUsize>,
}

impl fmt::Display for RecordingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordingStore")
    }
}

#[async_trait]
impl ObjectStore for RecordingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.requests.lock().unwrap().push(options.clone());
        // InMemory has no historical versions. This wrapper checks forwarding
        // of an explicit version; ETag preconditions are checked by InMemory.
        if options.version != self.version {
            return Err(object_store::Error::Precondition {
                path: location.to_string(),
                source: "unexpected object version".into(),
            });
        }
        let mut result = self.inner.get_opts(location, options).await?;
        result.meta.version = self.version.clone();
        match self.response {
            Response::Truncated => {
                result.payload =
                    GetResultPayload::Stream(stream::iter([Ok(Bytes::from_static(b"x"))]).boxed());
            }
            Response::TooLong => {
                result.payload = GetResultPayload::Stream(
                    stream::iter([
                        Ok(Bytes::from_static(b"abc")),
                        Ok(Bytes::from_static(b"extra")),
                    ])
                    .boxed(),
                );
            }
            Response::WrongRange => result.range = 0..result.meta.size,
            Response::WrongSize => result.meta.size += 1,
            Response::WrongEtag => result.meta.e_tag = Some("different-etag".to_string()),
            Response::WrongVersion => result.meta.version = Some("different-version".to_string()),
            Response::Normal => {}
        }
        let GetResult {
            payload,
            meta,
            range,
            attributes,
        } = result;
        let polls = Arc::clone(&self.body_polls);
        let result = GetResult {
            payload,
            meta: meta.clone(),
            range: range.clone(),
            attributes: attributes.clone(),
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(
                result
                    .into_stream()
                    .inspect_ok(move |_| {
                        polls.fetch_add(1, Ordering::Relaxed);
                    })
                    .boxed(),
            ),
            meta,
            range,
            attributes,
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

async fn fixture(response: Response, version: Option<&str>) -> (Arc<RecordingStore>, ObjectMeta) {
    let store = Arc::new(RecordingStore {
        inner: InMemory::new(),
        requests: Mutex::new(Vec::new()),
        response,
        version: version.map(str::to_string),
        body_polls: Arc::new(AtomicUsize::new(0)),
    });
    let location = Path::from("test.orc");
    store
        .inner
        .put(&location, Bytes::from_static(b"0123456789").into())
        .await
        .unwrap();
    let mut meta = store.inner.head(&location).await.unwrap();
    meta.version = store.version.clone();
    (store, meta)
}

#[tokio::test]
async fn reads_exact_ranges_and_keeps_etag_on_every_request() {
    let (store, meta) = fixture(Response::Normal, None).await;
    let mut reader = OrcObjectReader::new(store.clone(), meta.clone());
    assert_eq!(reader.len().await.unwrap(), 10);
    assert_eq!(reader.get_bytes(0, 3).await.unwrap(), b"012"[..]);
    assert_eq!(reader.get_bytes(7, 3).await.unwrap(), b"789"[..]);
    let requests = store.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (request, range) in requests.iter().zip([0..3, 7..10]) {
        assert_eq!(request.range, Some(GetRange::Bounded(range)));
        assert_eq!(request.if_match, meta.e_tag);
        assert_eq!(request.version, None);
        assert!(!request.head);
    }
}

#[tokio::test]
async fn empty_ranges_need_no_io_or_object_validator() {
    let (store, mut meta) = fixture(Response::Normal, None).await;
    meta.e_tag = None;
    let mut reader = OrcObjectReader::new(store.clone(), meta.clone());
    for offset in [0, 3, 10] {
        assert!(reader.get_bytes(offset, 0).await.unwrap().is_empty());
    }
    meta.size = 0;
    let mut empty_reader = OrcObjectReader::new(store.clone(), meta);
    assert_eq!(empty_reader.len().await.unwrap(), 0);
    assert!(empty_reader.get_bytes(0, 0).await.unwrap().is_empty());
    assert!(store.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn invalid_ranges_are_rejected_before_io() {
    let (store, mut meta) = fixture(Response::Normal, None).await;
    let mut reader = OrcObjectReader::new(store.clone(), meta.clone());
    for (offset, length) in [(10, 1), (11, 0), (0, 11), (u64::MAX, 1)] {
        assert_eq!(
            reader.get_bytes(offset, length).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    meta.size = u64::MAX;
    let mut huge_reader = OrcObjectReader::new(store.clone(), meta);
    assert_eq!(
        huge_reader.get_bytes(0, u64::MAX).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert!(store.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn missing_or_unusable_validators_are_rejected_before_io() {
    let (store, original) = fixture(Response::Normal, None).await;
    for etag in [None, Some(""), Some("*"), Some("W/\"weak\"")] {
        let mut meta = original.clone();
        meta.e_tag = etag.map(str::to_string);
        meta.version = Some(String::new());
        let mut reader = OrcObjectReader::new(store.clone(), meta);
        let error = reader.get_bytes(0, 3).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert!(error.to_string().contains("version or usable ETag"));
    }
    assert!(store.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn changed_etag_preserves_precondition_failure_without_retry() {
    let (store, meta) = fixture(Response::Normal, None).await;
    let mut reader = OrcObjectReader::new(store.clone(), meta.clone());
    reader.get_bytes(0, 3).await.unwrap();
    store
        .inner
        .put(&meta.location, Bytes::from_static(b"abcdefghij").into())
        .await
        .unwrap();
    let error = reader.get_bytes(7, 3).await.unwrap_err();
    assert!(matches!(
        error
            .get_ref()
            .and_then(|error| error.downcast_ref::<object_store::Error>()),
        Some(object_store::Error::Precondition { .. })
    ));
    let requests = store.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.if_match == meta.e_tag)
    );
}

#[tokio::test]
async fn forwards_version_and_etag_on_every_range() {
    let (store, meta) = fixture(Response::Normal, Some("version-1")).await;
    let mut reader = OrcObjectReader::new(store.clone(), meta.clone());
    reader.get_bytes(0, 3).await.unwrap();
    reader.get_bytes(7, 3).await.unwrap();
    let requests = store.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for request in requests.iter() {
        assert_eq!(request.version, meta.version);
        assert_eq!(request.if_match, meta.e_tag);
    }
}

#[tokio::test]
async fn explicit_version_does_not_require_an_etag() {
    let (store, mut meta) = fixture(Response::Normal, Some("version-1")).await;
    meta.e_tag = None;
    let mut reader = OrcObjectReader::new(store.clone(), meta);
    assert_eq!(reader.get_bytes(7, 3).await.unwrap(), b"789"[..]);
    let requests = store.requests.lock().unwrap();
    assert_eq!(requests[0].version.as_deref(), Some("version-1"));
    assert_eq!(requests[0].if_match, None);
}

#[tokio::test]
async fn rejects_truncated_or_excess_response_bytes() {
    for (response, kind) in [
        (Response::Truncated, ErrorKind::UnexpectedEof),
        (Response::TooLong, ErrorKind::InvalidData),
    ] {
        let (store, meta) = fixture(response, None).await;
        let mut reader = OrcObjectReader::new(store, meta);
        assert_eq!(reader.get_bytes(2, 3).await.unwrap_err().kind(), kind);
    }
}

#[tokio::test]
async fn rejects_changed_response_identity_or_range_before_reading_body() {
    for response in [
        Response::WrongRange,
        Response::WrongSize,
        Response::WrongEtag,
        Response::WrongVersion,
    ] {
        let (store, meta) = fixture(response, Some("version-1")).await;
        let mut reader = OrcObjectReader::new(store.clone(), meta);
        assert_eq!(
            reader.get_bytes(2, 3).await.unwrap_err().kind(),
            ErrorKind::InvalidData
        );
        assert_eq!(store.body_polls.load(Ordering::Relaxed), 0);
    }
}
