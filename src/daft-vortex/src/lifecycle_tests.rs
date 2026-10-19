//! Exercise the production scan with real Vortex files and positional reads.
//!
//! The supplied footer avoids the opener's initial-read cache, and disabling
//! coalescing makes each generated flat chunk one observable I/O request. Gates
//! act before FileReadAt starts an OS read: cancellation assertions concern
//! queued async work, not interruption of an already-running blocking pread.

use std::{sync::Arc, time::Duration};

use arrow_array::{Int64Array, RecordBatch as ArrowRecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture, stream::BoxStream};
use tempfile::NamedTempFile;
use tokio::sync::{Semaphore, watch};
use vortex::{
    VortexSessionDefault,
    array::buffer::BufferHandle,
    arrow::ArrowSessionExt,
    buffer::{Alignment, ByteBufferMut},
    error::VortexResult,
    file::{Footer, OpenOptionsSessionExt, WriteOptionsSessionExt},
    io::{VortexReadAt, session::RuntimeSessionExt, std_file::FileReadAt},
    layout::layouts::{chunked::writer::ChunkedLayoutStrategy, flat::writer::FlatLayoutStrategy},
    session::VortexSession,
};

use super::{schemas, stream_opened_vortex};
use crate::{VortexConvertOptions, VortexReadOptions};

const CHUNK_ROWS: usize = 8;
const TEST_TIMEOUT: Duration = Duration::from_secs(30);
type DaftStream = BoxStream<'static, common_error::DaftResult<daft_recordbatch::RecordBatch>>;

struct SegmentedFile {
    path: NamedTempFile,
    footer: Footer,
    size: u64,
    chunks: usize,
}

impl SegmentedFile {
    async fn new(chunks: usize) -> Self {
        let session = VortexSession::default().with_tokio();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let arrays = (0..chunks)
            .map(|chunk| {
                let start = (chunk * CHUNK_ROWS) as i64;
                let batch = ArrowRecordBatch::try_new(
                    arrow_schema.clone(),
                    vec![Arc::new(Int64Array::from_iter_values(
                        start..start + CHUNK_ROWS as i64,
                    ))],
                )
                .unwrap();
                session
                    .arrow()
                    .from_arrow_record_batch(batch, arrow_schema.as_ref())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut bytes = ByteBufferMut::empty();
        let mut writer = session
            .write_options()
            .with_strategy(Arc::new(ChunkedLayoutStrategy::new(
                FlatLayoutStrategy::default(),
            )))
            .writer(&mut bytes, arrays[0].dtype().clone());
        for array in arrays {
            writer.push(array).await.unwrap();
        }
        let summary = writer.finish().await.unwrap();
        let footer = summary.footer().clone();
        assert_eq!(footer.segment_map().len(), chunks);
        let path = NamedTempFile::new().unwrap();
        std::fs::write(path.path(), bytes.as_slice()).unwrap();
        Self {
            path,
            footer,
            size: summary.size(),
            chunks,
        }
    }

    fn max_segment_bytes(&self) -> usize {
        self.footer
            .segment_map()
            .iter()
            .map(|segment| segment.length as usize)
            .max()
            .unwrap()
    }

    async fn scan(&self, control: &Arc<ReadControl>, limit: Option<usize>) -> DaftStream {
        let session = VortexSession::default().with_tokio();
        let reader = Arc::new(RecordingReadAt {
            inner: Arc::new(FileReadAt::open(self.path.path(), session.handle()).unwrap()),
            control: control.clone(),
        });
        let file = session
            .open_options()
            .with_footer(self.footer.clone())
            .with_file_size(self.size)
            .open(reader)
            .await
            .unwrap();
        let uri = self.path.path().to_str().unwrap();
        let (_, schema) = schemas(uri, &session, file.dtype()).unwrap();
        let stream = stream_opened_vortex(
            uri,
            session,
            file,
            VortexReadOptions {
                batch_size: CHUNK_ROWS,
            },
            VortexConvertOptions {
                schema,
                predicate: None,
                include_columns: None,
                limit,
            },
        )
        .unwrap();
        assert_eq!(
            control.snapshot().calls,
            0,
            "opening must not cache payloads"
        );
        stream
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ReadCounts {
    calls: usize,
    started: usize,
    active: usize,
    peak_active: usize,
    active_bytes: usize,
    peak_active_bytes: usize,
    completed: usize,
    completed_bytes: usize,
    failed: usize,
    cancelled: usize,
    reader_drops: usize,
}

struct ReadControl {
    counts: watch::Sender<ReadCounts>,
    gate: Option<Semaphore>,
    fail_offset: Option<u64>,
}

impl ReadControl {
    fn new(blocked: bool, fail_offset: Option<u64>) -> Arc<Self> {
        Arc::new(Self {
            counts: watch::channel(ReadCounts::default()).0,
            gate: blocked.then(|| Semaphore::new(0)),
            fail_offset,
        })
    }

    fn snapshot(&self) -> ReadCounts {
        *self.counts.borrow()
    }

    async fn wait_for(&self, predicate: impl Fn(&ReadCounts) -> bool) -> ReadCounts {
        let mut receiver = self.counts.subscribe();
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                let counts = *receiver.borrow_and_update();
                if predicate(&counts) {
                    return counts;
                }
                receiver.changed().await.unwrap();
            }
        })
        .await
        .unwrap_or_else(|_| panic!("I/O lifecycle did not settle: {:?}", self.snapshot()))
    }

    async fn released(&self) -> ReadCounts {
        let counts = self
            .wait_for(|counts| counts.reader_drops == 1 && counts.active == 0)
            .await;
        assert_eq!(
            counts.started,
            counts.completed + counts.failed + counts.cancelled
        );
        assert_eq!(counts.active_bytes, 0);
        counts
    }
}

struct ReadGuard {
    control: Arc<ReadControl>,
    bytes: usize,
    outcome: Option<bool>,
}

impl ReadGuard {
    fn new(control: Arc<ReadControl>, bytes: usize) -> Self {
        control.counts.send_modify(|counts| {
            counts.started += 1;
            counts.active += 1;
            counts.active_bytes += bytes;
            counts.peak_active = counts.peak_active.max(counts.active);
            counts.peak_active_bytes = counts.peak_active_bytes.max(counts.active_bytes);
        });
        Self {
            control,
            bytes,
            outcome: None,
        }
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.control.counts.send_modify(|counts| {
            counts.active -= 1;
            counts.active_bytes -= self.bytes;
            match self.outcome {
                Some(true) => {
                    counts.completed += 1;
                    counts.completed_bytes += self.bytes;
                }
                Some(false) => counts.failed += 1,
                None => counts.cancelled += 1,
            }
        });
    }
}

struct RecordingReadAt {
    inner: Arc<FileReadAt>,
    control: Arc<ReadControl>,
}

impl Drop for RecordingReadAt {
    fn drop(&mut self) {
        self.control
            .counts
            .send_modify(|counts| counts.reader_drops += 1);
    }
}

impl VortexReadAt for RecordingReadAt {
    fn concurrency(&self) -> usize {
        self.inner.concurrency()
    }

    // Deliberately retain the trait's no-coalescing default. The production
    // FileReadAt also coalesces nearby unpolled segments, so its byte read-ahead
    // need not equal this fixture's independently requested split window.
    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        self.inner.size()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        self.control.counts.send_modify(|counts| counts.calls += 1);
        let inner = self.inner.clone();
        let control = self.control.clone();
        async move {
            let mut guard = ReadGuard::new(control.clone(), length);
            if let Some(gate) = &control.gate {
                gate.acquire().await.unwrap().forget();
            }
            let result = if control.fail_offset == Some(offset) {
                Err(std::io::Error::other("injected positional read failure").into())
            } else {
                inner.read_at(offset, length, alignment).await
            };
            guard.outcome = Some(result.is_ok());
            result
        }
        .boxed()
    }
}

async fn next_batch(stream: &mut DaftStream) -> daft_recordbatch::RecordBatch {
    tokio::time::timeout(TEST_TIMEOUT, stream.try_next())
        .await
        .expect("scan stalled")
        .expect("scan failed")
        .expect("scan ended early")
}

fn assert_ids(batch: &daft_recordbatch::RecordBatch, expected: std::ops::Range<i64>) {
    assert_eq!(
        batch
            .get_column(0)
            .i64()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>(),
        expected.map(Some).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn eof_releases_reader_while_returned_batch_remains_usable() {
    let fixture = SegmentedFile::new(4).await;
    let control = ReadControl::new(false, None);
    let mut stream = fixture.scan(&control, None).await;
    let first = next_batch(&mut stream).await;
    let mut rows = first.len();
    while let Some(batch) = stream.try_next().await.unwrap() {
        rows += batch.len();
    }
    assert_eq!(rows, fixture.chunks * CHUNK_ROWS);
    // Keep the exhausted stream and returned batch alive during the assertion.
    let counts = control.released().await;
    assert_eq!(counts.completed, fixture.chunks);
    assert_eq!(counts.failed + counts.cancelled, 0);
    assert_ids(&first, 0..CHUNK_ROWS as i64);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn exact_limit_releases_reader_without_another_consumer_poll() {
    let fixture = SegmentedFile::new(4).await;
    let control = ReadControl::new(false, None);
    let mut stream = fixture.scan(&control, Some(CHUNK_ROWS + 3)).await;
    assert_eq!(next_batch(&mut stream).await.len(), CHUNK_ROWS);
    let last = next_batch(&mut stream).await;
    assert_eq!(last.len(), 3);
    // The next poll cannot be responsible for cleanup: it happens afterward.
    control.released().await;
    assert_ids(&last, CHUNK_ROWS as i64..CHUNK_ROWS as i64 + 3);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn dropping_partial_scan_releases_reader_and_preserves_returned_batch() {
    let fixture = SegmentedFile::new(4).await;
    let control = ReadControl::new(false, None);
    let mut stream = fixture.scan(&control, None).await;
    let retained = next_batch(&mut stream).await;
    drop(stream);
    control.released().await;
    assert_ids(&retained, 0..CHUNK_ROWS as i64);
    // Reopening is supplemental; the counters above prove source ownership ends.
    let reopened = ReadControl::new(false, None);
    let mut stream = fixture.scan(&reopened, Some(1)).await;
    assert_eq!(next_batch(&mut stream).await.len(), 1);
    reopened.released().await;
}

#[tokio::test]
async fn source_error_after_a_batch_releases_reader() {
    let fixture = SegmentedFile::new(4).await;
    let failing_offset = fixture.footer.segment_map().last().unwrap().offset;
    let control = ReadControl::new(false, Some(failing_offset));
    let mut stream = fixture.scan(&control, None).await;
    assert_eq!(next_batch(&mut stream).await.len(), CHUNK_ROWS);
    let error = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match stream.try_next().await {
                Ok(Some(_)) => {}
                Ok(None) => panic!("injected source error was lost"),
                Err(error) => break error,
            }
        }
    })
    .await
    .unwrap();
    assert!(
        error
            .to_string()
            .contains("injected positional read failure")
    );
    assert!(
        error
            .to_string()
            .contains(fixture.path.path().to_str().unwrap())
    );
    let counts = control.released().await;
    assert_eq!(counts.failed, 1);
}

#[tokio::test]
async fn cancelling_pending_next_drops_queued_io_without_completing_it() {
    let fixture = SegmentedFile::new(4).await;
    let control = ReadControl::new(true, None);
    let mut stream = fixture.scan(&control, None).await;
    let pending = tokio::spawn(async move { stream.try_next().await });
    control.wait_for(|counts| counts.active > 0).await;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    // No gate is released: cancellation must drop the blocked future itself.
    let counts = control.released().await;
    assert!(counts.cancelled > 0);
    assert_eq!(counts.completed + counts.failed, 0);
}

#[tokio::test]
async fn slow_consumer_limits_requested_splits_and_in_flight_bytes() {
    // Vortex's scan concurrency is per available CPU worker. The known flat
    // layout has one read per split, so this is a measurable request-window
    // bound, not a promise about RSS or arbitrarily large physical segments.
    let workers = std::thread::available_parallelism().map_or(1, usize::from);
    let fixture = SegmentedFile::new(workers * 3 + 2).await;
    let control = ReadControl::new(false, None);
    let mut stream = fixture.scan(&control, None).await;
    let mut output_bytes = 0;
    for consumed in 1..=fixture.chunks {
        let batch = next_batch(&mut stream).await;
        output_bytes += batch.size_bytes();
        assert_eq!(batch.len(), CHUNK_ROWS);
        // Pause the consumer until its current prefetch window has settled.
        // Notifications, rather than sleeps, synchronize this slow-consumer test.
        let expected_reads = (workers + consumed - 1).min(fixture.chunks);
        let counts = control
            .wait_for(|counts| counts.completed >= expected_reads && counts.active == 0)
            .await;
        assert_eq!(counts.started, expected_reads);
        assert!(counts.peak_active <= workers.min(32));
        assert!(counts.peak_active_bytes <= workers.min(32) * fixture.max_segment_bytes());
    }
    assert!(stream.next().await.is_none());
    let counts = control.released().await;
    assert_eq!(counts.completed, fixture.chunks);
    eprintln!(
        "slow consumer: workers={workers}, chunks={}, physical_bytes={}, emitted_batch_bytes={output_bytes}, peak_io_bytes={}, peak_reads={}",
        fixture.chunks, counts.completed_bytes, counts.peak_active_bytes, counts.peak_active,
    );
}

#[tokio::test]
async fn concurrent_files_cancel_independently_and_release_all_pending_reads() {
    let mut fixtures = Vec::new();
    for _ in 0..3 {
        fixtures.push(SegmentedFile::new(4).await);
    }
    let mut controls = Vec::new();
    let mut tasks = Vec::new();
    for fixture in &fixtures {
        let control = ReadControl::new(true, None);
        let mut stream = fixture.scan(&control, None).await;
        tasks.push(tokio::spawn(async move { stream.try_next().await }));
        controls.push(control);
    }
    for control in &controls {
        control.wait_for(|counts| counts.active > 0).await;
    }
    let pending_bytes: usize = controls
        .iter()
        .map(|control| control.snapshot().active_bytes)
        .sum();
    let first = tasks.remove(0);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    controls[0].released().await;
    for control in &controls[1..] {
        let counts = control.snapshot();
        assert_eq!(counts.reader_drops, 0);
        assert!(counts.active > 0);
    }
    for task in tasks {
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
    for control in &controls {
        let counts = control.released().await;
        assert!(counts.cancelled > 0);
        assert_eq!(counts.completed + counts.failed, 0);
    }
    eprintln!("three concurrent blocked files: observed_pending_io_bytes={pending_bytes}");
}
