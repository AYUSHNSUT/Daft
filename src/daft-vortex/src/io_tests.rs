use std::{
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use daft_io::{GetResult, IOClient, IOStatsContext};
use futures::{FutureExt, StreamExt, poll};
use tempfile::NamedTempFile;
use tokio::sync::Semaphore;
use vortex::{buffer::Alignment, io::VortexReadAt};

use super::{DaftReadAt, RangeReader, display_uri, validate_options};
use crate::VortexReadOptions;

struct MockReader {
    chunks: Vec<Bytes>,
    hint: Option<usize>,
    gate: Option<Arc<Semaphore>>,
    calls: Mutex<Vec<Range<usize>>>,
    active: Arc<AtomicUsize>,
    fail: bool,
}

struct Active(Arc<AtomicUsize>);

impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl RangeReader for MockReader {
    fn get(
        &self,
        range: Range<usize>,
    ) -> futures::future::BoxFuture<'static, daft_io::Result<GetResult>> {
        self.calls.lock().unwrap().push(range);
        let chunks = self.chunks.clone();
        let hint = self.hint;
        let gate = self.gate.clone();
        let active = self.active.clone();
        let fail = self.fail;
        async move {
            active.fetch_add(1, Ordering::SeqCst);
            let guard = Active(active);
            let stream = futures::stream::once(async move {
                let _guard = guard;
                if let Some(gate) = gate {
                    gate.acquire().await.unwrap().forget();
                }
                if fail {
                    return Err(daft_io::Error::SocketError {
                        path: "https://user:secret@example.test/a?token=hidden".into(),
                        source: std::io::Error::other("credential hidden and secret").into(),
                    });
                }
                Ok(chunks)
            })
            .map(|result| match result {
                Ok(chunks) => futures::stream::iter(chunks.into_iter().map(Ok)).boxed(),
                Err(error) => futures::stream::iter([Err(error)]).boxed(),
            })
            .flatten()
            .boxed();
            Ok(GetResult::Stream(stream, hint, None, None))
        }
        .boxed()
    }
}

fn mock_reader(
    chunks: Vec<Bytes>,
    hint: Option<usize>,
    blocked: bool,
    fail: bool,
    max_requests: usize,
    max_bytes: usize,
) -> (DaftReadAt, Arc<MockReader>) {
    let backend = Arc::new(MockReader {
        chunks,
        hint,
        gate: blocked.then(|| Arc::new(Semaphore::new(0))),
        calls: Mutex::new(Vec::new()),
        active: Arc::new(AtomicUsize::new(0)),
        fail,
    });
    (
        DaftReadAt {
            backend: backend.clone(),
            uri: "https://example.test/data".into(),
            file_size: 128,
            stats: IOStatsContext::new("mock Vortex reader"),
            requests: Arc::new(Semaphore::new(max_requests)),
            bytes: Arc::new(Semaphore::new(max_bytes)),
            max_requests,
            max_bytes,
        },
        backend,
    )
}

#[tokio::test]
async fn local_exact_ranges_alignment_zero_length_and_stats() {
    let file = NamedTempFile::new().unwrap();
    let contents = (0..128).collect::<Vec<u8>>();
    std::fs::write(file.path(), &contents).unwrap();
    let stats = IOStatsContext::new("local adapter");
    let reader = DaftReadAt::new(
        file.path().to_string_lossy().into_owned(),
        Arc::new(IOClient::new(Default::default()).unwrap()),
        stats.clone(),
        &VortexReadOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(reader.size().await.unwrap(), 128);
    let bytes = reader
        .read_at(3, 19, Alignment::new(64))
        .await
        .unwrap()
        .unwrap_host();
    assert_eq!(bytes.as_slice(), &contents[3..22]);
    assert!(Alignment::new(64).is_ptr_aligned(bytes.as_ptr()));
    assert_eq!(stats.load_get_requests(), 1);
    assert_eq!(stats.load_bytes_read(), 19);
    assert!(
        reader
            .read_at(128, 0, Alignment::new(64))
            .await
            .unwrap()
            .unwrap_host()
            .is_empty()
    );
    assert!(reader.read_at(120, 9, Alignment::none()).await.is_err());
    assert!(
        reader
            .read_at(u64::MAX, 2, Alignment::none())
            .await
            .is_err()
    );
    assert_eq!(stats.load_get_requests(), 1);
    assert_eq!(stats.load_bytes_read(), 19);
    // Output buffers own their allocation independently of the reader/budgets.
    drop(reader);
    assert_eq!(bytes.as_slice(), &contents[3..22]);
}

#[tokio::test]
async fn stream_length_validation_rejects_short_oversized_and_wrong_hint() {
    for (chunks, hint, expected) in [
        (vec![Bytes::from_static(b"123")], None, "short"),
        (
            vec![Bytes::from_static(b"12"), Bytes::from_static(b"345")],
            None,
            "oversized",
        ),
        (
            vec![Bytes::from_static(b"1234")],
            Some(5),
            "length mismatch",
        ),
    ] {
        let (reader, _) = mock_reader(chunks, hint, false, false, 2, 16);
        let error = reader.read_at(3, 4, Alignment::new(64)).await.unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(reader.requests.available_permits(), 2);
        assert_eq!(reader.bytes.available_permits(), 16);
    }
    let (reader, backend) = mock_reader(
        vec![Bytes::from_static(b"12"), Bytes::from_static(b"34")],
        Some(4),
        false,
        false,
        2,
        16,
    );
    let bytes = reader
        .read_at(7, 4, Alignment::new(64))
        .await
        .unwrap()
        .unwrap_host();
    assert_eq!(bytes.as_slice(), b"1234");
    assert!(Alignment::new(64).is_ptr_aligned(bytes.as_ptr()));
    assert_eq!(*backend.calls.lock().unwrap(), vec![7..11]);
}

#[tokio::test]
async fn request_and_byte_limits_block_and_cancel_without_leaking_permits() {
    // First isolate the count limit, then the byte limit. Polling is explicit:
    // no timing/sleep assumption is needed to prove a waiting read stays idle.
    for (max_requests, max_bytes) in [(1, 32), (3, 8)] {
        let (reader, backend) = mock_reader(
            vec![Bytes::from_static(b"12345678")],
            None,
            true,
            false,
            max_requests,
            max_bytes,
        );
        let mut first = reader.read_at(0, 8, Alignment::none());
        assert!(poll!(&mut first).is_pending());
        assert_eq!(backend.active.load(Ordering::SeqCst), 1);
        let mut waiting = reader.read_at(8, 8, Alignment::none());
        assert!(poll!(&mut waiting).is_pending());
        assert_eq!(backend.calls.lock().unwrap().len(), 1);
        drop(waiting);
        assert_eq!(backend.active.load(Ordering::SeqCst), 1);
        let mut replacement = reader.read_at(16, 8, Alignment::none());
        assert!(poll!(&mut replacement).is_pending());
        drop(first);
        assert_eq!(backend.active.load(Ordering::SeqCst), 0);
        assert!(poll!(&mut replacement).is_pending());
        assert_eq!(backend.calls.lock().unwrap().len(), 2);
        backend.gate.as_ref().unwrap().add_permits(1);
        assert_eq!(
            replacement.await.unwrap().unwrap_host().as_slice(),
            b"12345678"
        );
        assert_eq!(reader.requests.available_permits(), max_requests);
        assert_eq!(reader.bytes.available_permits(), max_bytes);
        assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn oversized_reads_fail_before_io_and_errors_release_budgets_without_secrets() {
    let (reader, backend) = mock_reader(vec![], None, false, true, 2, 8);
    let error = reader
        .read_at(0, 9, Alignment::none())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("max_in_flight_bytes=8"));
    assert!(backend.calls.lock().unwrap().is_empty());
    let error = reader
        .read_at(0, 4, Alignment::none())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("response body/socket error"));
    assert!(!error.contains("secret"));
    assert!(!error.contains("hidden"));
    assert_eq!(reader.requests.available_permits(), 2);
    assert_eq!(reader.bytes.available_permits(), 8);
}

#[tokio::test]
async fn opens_refresh_metadata_and_keep_input_statistics_separate() {
    let file = NamedTempFile::new().unwrap();
    let client = Arc::new(IOClient::new(Default::default()).unwrap());
    let path = file.path().to_string_lossy().into_owned();
    let first_stats = IOStatsContext::new("first open");
    let second_stats = IOStatsContext::new("second open");
    std::fs::write(file.path(), b"1234").unwrap();
    let first = DaftReadAt::new(
        path.clone(),
        client.clone(),
        first_stats.clone(),
        &VortexReadOptions::default(),
    )
    .await
    .unwrap();
    first.read_at(0, 4, Alignment::none()).await.unwrap();
    std::fs::write(file.path(), b"87654321").unwrap();
    let second = DaftReadAt::new(
        path,
        client,
        second_stats.clone(),
        &VortexReadOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(second.size().await.unwrap(), 8);
    assert_eq!(
        second
            .read_at(4, 4, Alignment::none())
            .await
            .unwrap()
            .unwrap_host()
            .as_slice(),
        b"4321"
    );
    for stats in [&first_stats, &second_stats] {
        assert_eq!(stats.load_bytes_read(), 4);
        assert_eq!(stats.load_get_requests(), 1);
    }
}

#[test]
fn invalid_budgets_and_url_redaction_are_checked_before_semaphores() {
    for (count, bytes) in [(0, 1), (1, 0), (usize::MAX, 1), (1, usize::MAX)] {
        let options = VortexReadOptions {
            max_concurrent_requests: count,
            max_in_flight_bytes: bytes,
            ..Default::default()
        };
        assert!(validate_options(&options).is_err());
    }
    assert_eq!(
        display_uri("https://user:secret@example.test/a?token=hidden#private"),
        "https://example.test/a"
    );
    assert_eq!(
        display_uri("gs://user:secret@example.test/a?token=hidden#private"),
        "gs://example.test/a"
    );
    for uri in [
        "https://user:secret@example.test:invalid/a?token=hidden#private",
        "file://user:secret@example.test/a?token=hidden#private",
    ] {
        assert_eq!(display_uri(uri), "<invalid URL>");
    }
    assert_eq!(
        display_uri("file://localhost/a%20b?token=hidden#private"),
        "file://localhost/a%20b"
    );
    for path in [
        "/tmp/local name.vortex",
        "/tmp/local?name#with@characters.vortex",
        "local?name#with@characters.vortex",
        "/tmp/literal://name.vortex",
    ] {
        assert_eq!(display_uri(path), path);
    }
}
