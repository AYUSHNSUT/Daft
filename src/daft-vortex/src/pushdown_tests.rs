//! Verify selective scans with real column-separated files and recorded reads.
//!
//! Supplied footers and disabled coalescing isolate payload requests from
//! opening read-ahead. These counters describe positional reads, not device I/O.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

use arrow_array::{Int64Array, RecordBatch as ArrowRecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use daft_dsl::{ExprRef, lit, resolved_col};
use futures::{FutureExt, TryStreamExt, future::BoxFuture};
use tempfile::NamedTempFile;
use vortex::{
    VortexSessionDefault,
    array::buffer::BufferHandle,
    arrow::ArrowSessionExt,
    buffer::{Alignment, ByteBufferMut},
    error::VortexResult,
    file::{Footer, OpenOptionsSessionExt, WriteOptionsSessionExt},
    io::{VortexReadAt, session::RuntimeSessionExt, std_file::FileReadAt},
    layout::{
        LayoutStrategy,
        layouts::{
            chunked::writer::ChunkedLayoutStrategy,
            flat::writer::FlatLayoutStrategy,
            struct_::StructStrategy,
            zoned::writer::{ZonedLayoutOptions, ZonedStrategy},
        },
    },
    session::VortexSession,
};

use super::{schemas, stream_opened_vortex};
use crate::{VortexConvertOptions, VortexReadOptions};

const CHUNK_ROWS: usize = 32;
const CHUNKS: usize = 4;
const FIELD_NAMES: [&str; 5] = ["id", "value", "unused_a", "unused_b", "unused_c"];
const TEST_TIMEOUT: Duration = Duration::from_secs(30);
type ReadRange = (u64, usize);

struct ColumnFile {
    path: NamedTempFile,
    footer: Footer,
    size: u64,
    values: Vec<Option<i64>>,
    payload_ranges: BTreeMap<&'static str, Vec<ReadRange>>,
}

impl ColumnFile {
    async fn new(zoned: bool, values: Vec<Option<i64>>) -> Self {
        assert_eq!(values.len(), CHUNKS * CHUNK_ROWS);
        let session = VortexSession::default().with_tokio();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
            Field::new("unused_a", DataType::Utf8, false),
            Field::new("unused_b", DataType::Utf8, false),
            Field::new("unused_c", DataType::Utf8, false),
        ]));
        let arrays = values
            .chunks_exact(CHUNK_ROWS)
            .enumerate()
            .map(|(chunk, values)| {
                let start = chunk * CHUNK_ROWS;
                let strings = |prefix| {
                    Arc::new(StringArray::from_iter_values(
                        (start..start + CHUNK_ROWS)
                            .map(|row| format!("{prefix}-{row:04}-{}", "wide-payload-".repeat(24))),
                    ))
                };
                let batch = ArrowRecordBatch::try_new(
                    arrow_schema.clone(),
                    vec![
                        Arc::new(Int64Array::from_iter_values(
                            (start..start + CHUNK_ROWS).map(|row| row as i64),
                        )),
                        Arc::new(Int64Array::from(values.to_vec())),
                        strings("a"),
                        strings("b"),
                        strings("c"),
                    ],
                )
                .unwrap();
                session
                    .arrow()
                    .from_arrow_record_batch(batch, arrow_schema.as_ref())
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let leaf: Arc<dyn LayoutStrategy> =
            Arc::new(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()));
        let mut strategy = StructStrategy::new(Arc::new(FlatLayoutStrategy::default()), leaf);
        if zoned {
            strategy = strategy.with_field_writer(
                "value",
                Arc::new(ZonedStrategy::new(
                    ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()),
                    FlatLayoutStrategy::default(),
                    ZonedLayoutOptions {
                        block_size: NonZeroUsize::new(CHUNK_ROWS).unwrap(),
                        concurrency: NonZeroUsize::new(1).unwrap(),
                        ..Default::default()
                    },
                )),
            );
        }
        let mut bytes = ByteBufferMut::empty();
        let mut writer = session
            .write_options()
            .with_strategy(Arc::new(strategy))
            // Isolate zone pruning from whole-file statistics pruning.
            .with_file_statistics(vec![])
            .writer(&mut bytes, arrays[0].dtype().clone());
        for array in arrays {
            writer.push(array).await.unwrap();
        }
        let summary = writer.finish().await.unwrap();
        let footer = summary.footer().clone();
        assert!(footer.statistics().is_none());
        assert_eq!(footer.layout().encoding_id().as_ref(), "vortex.struct");

        let payload_ranges = FIELD_NAMES
            .iter()
            .enumerate()
            .map(|(index, &name)| {
                // Struct slot zero is reserved for the absent top-level validity.
                let field = footer.layout().slot(index + 1).unwrap().unwrap();
                let data = if zoned && name == "value" {
                    assert_eq!(field.encoding_id().as_ref(), "vortex.zoned");
                    field.slot(0).unwrap().unwrap()
                } else {
                    field
                };
                assert_eq!(data.encoding_id().as_ref(), "vortex.chunked");
                let chunks = data.children().unwrap();
                assert_eq!(chunks.len(), CHUNKS);
                let ranges = chunks
                    .iter()
                    .map(|chunk| {
                        let ids = chunk.segment_ids();
                        assert_eq!(ids.len(), 1);
                        let segment = &footer.segment_map()[*ids[0] as usize];
                        (segment.offset, segment.length as usize)
                    })
                    .collect();
                (name, ranges)
            })
            .collect();
        let path = NamedTempFile::new().unwrap();
        std::fs::write(path.path(), bytes.as_slice()).unwrap();
        Self {
            path,
            footer,
            size: summary.size(),
            values,
            payload_ranges,
        }
    }

    async fn scan(
        &self,
        output: &[&str],
        predicate: Option<ExprRef>,
        enable_projection_pushdown: bool,
        enable_filter_pushdown: bool,
    ) -> ScanResult {
        let session = VortexSession::default().with_tokio();
        let observed = Arc::new(Mutex::new(ReadObservation::default()));
        let reader = Arc::new(RecordingReadAt {
            inner: Arc::new(FileReadAt::open(self.path.path(), session.handle()).unwrap()),
            observed: observed.clone(),
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
                enable_projection_pushdown,
                enable_filter_pushdown,
                ..Default::default()
            },
            VortexConvertOptions {
                schema,
                predicate,
                include_columns: Some(output.iter().map(|name| (*name).to_string()).collect()),
                limit: None,
            },
            None,
        )
        .unwrap();
        assert!(observed.lock().unwrap().ranges.is_empty());
        let batches = tokio::time::timeout(TEST_TIMEOUT, stream.try_collect::<Vec<_>>())
            .await
            .expect("selective scan stalled")
            .expect("selective scan failed");
        let mut rows = Vec::new();
        for batch in batches {
            assert_eq!(batch.schema.field_names().collect::<Vec<_>>(), output);
            let columns = (0..output.len())
                .map(|index| {
                    batch
                        .get_column(index)
                        .i64()
                        .unwrap()
                        .into_iter()
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            rows.extend(
                (0..batch.len()).map(|row| columns.iter().map(|column| column[row]).collect()),
            );
        }
        let observed = observed.lock().unwrap().clone();
        ScanResult { rows, observed }
    }

    fn expected(
        &self,
        output: &[&str],
        ids: impl IntoIterator<Item = usize>,
    ) -> Vec<Vec<Option<i64>>> {
        ids.into_iter()
            .map(|id| {
                output
                    .iter()
                    .map(|name| match *name {
                        "id" => Some(id as i64),
                        "value" => self.values[id],
                        _ => panic!("unexpected oracle column {name}"),
                    })
                    .collect()
            })
            .collect()
    }

    fn payload_bytes(&self, observation: &ReadObservation) -> usize {
        let payloads = self
            .payload_ranges
            .values()
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>();
        observation
            .ranges
            .iter()
            .filter(|range| payloads.contains(range))
            .map(|(_, length)| length)
            .sum()
    }

    fn assert_column_not_read(&self, name: &str, observation: &ReadObservation) {
        for range in &self.payload_ranges[name] {
            assert!(
                !observation.ranges.contains(range),
                "unexpected {name} read {range:?}"
            );
        }
    }
}

#[derive(Clone, Debug, Default)]
struct ReadObservation {
    ranges: Vec<ReadRange>,
    completed_bytes: usize,
}

struct ScanResult {
    rows: Vec<Vec<Option<i64>>>,
    observed: ReadObservation,
}

struct RecordingReadAt {
    inner: Arc<FileReadAt>,
    observed: Arc<Mutex<ReadObservation>>,
}

impl VortexReadAt for RecordingReadAt {
    fn concurrency(&self) -> usize {
        self.inner.concurrency()
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        self.inner.size()
    }

    // No coalescing: every payload segment is independently observable.
    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let inner = self.inner.clone();
        let observed = self.observed.clone();
        async move {
            observed.lock().unwrap().ranges.push((offset, length));
            let buffer = inner.read_at(offset, length, alignment).await?;
            observed.lock().unwrap().completed_bytes += length;
            Ok(buffer)
        }
        .boxed()
    }
}

fn clustered_values() -> Vec<Option<i64>> {
    (0..CHUNKS)
        .flat_map(|chunk| (0..CHUNK_ROWS).map(move |row| Some((chunk * 1000 + row) as i64)))
        .collect()
}

#[tokio::test]
async fn projection_avoids_unused_column_payloads_and_preserves_output_order() {
    let fixture = ColumnFile::new(false, clustered_values()).await;
    for output in [vec!["id"], vec!["value", "id"]] {
        for with_filter in [false, true] {
            let predicate = with_filter.then(|| resolved_col("value").gt_eq(lit(2000_i64)));
            let start = if with_filter { 2 * CHUNK_ROWS } else { 0 };
            let expected = fixture.expected(&output, start..CHUNKS * CHUNK_ROWS);
            let baseline = fixture.scan(&output, predicate.clone(), false, false).await;
            let projected = fixture.scan(&output, predicate, true, false).await;
            assert_eq!(baseline.rows, expected);
            assert_eq!(projected.rows, expected);
            for name in ["unused_a", "unused_b", "unused_c"] {
                fixture.assert_column_not_read(name, &projected.observed);
                assert!(
                    fixture.payload_ranges[name]
                        .iter()
                        .all(|range| baseline.observed.ranges.contains(range))
                );
            }
            if !with_filter && output == ["id"] {
                fixture.assert_column_not_read("value", &projected.observed);
            }
            assert!(projected.observed.completed_bytes < baseline.observed.completed_bytes);
            eprintln!(
                "projection: output={output:?}, residual={with_filter}, baseline_request_bytes={}, projected_request_bytes={}",
                baseline.observed.completed_bytes, projected.observed.completed_bytes
            );
        }
    }
}

#[tokio::test]
async fn empty_projection_preserves_rows_and_reads_only_residual_dependencies() {
    let fixture = ColumnFile::new(false, clustered_values()).await;
    for with_filter in [false, true] {
        let predicate = with_filter.then(|| resolved_col("value").gt_eq(lit(2000_i64)));
        let start = if with_filter { 2 * CHUNK_ROWS } else { 0 };
        let baseline = fixture.scan(&[], predicate.clone(), false, false).await;
        let projected = fixture.scan(&[], predicate, true, false).await;
        let expected = fixture.expected(&[], start..CHUNKS * CHUNK_ROWS);
        assert_eq!(baseline.rows, expected);
        assert_eq!(projected.rows, expected);
        for name in ["id", "unused_a", "unused_b", "unused_c"] {
            fixture.assert_column_not_read(name, &projected.observed);
        }
        if with_filter {
            assert!(!projected.observed.ranges.is_empty());
        } else {
            assert!(projected.observed.ranges.is_empty());
        }
    }
}

#[tokio::test]
async fn zoned_integer_filters_skip_only_excluded_payload_ranges() {
    let fixture = ColumnFile::new(true, clustered_values()).await;
    for (threshold, first_matching_chunk) in [(4000_i64, 4), (2000, 2), (0, 0)] {
        let predicate = resolved_col("value").gt_eq(lit(threshold));
        let baseline = fixture
            .scan(&["id"], Some(predicate.clone()), true, false)
            .await;
        let filtered = fixture.scan(&["id"], Some(predicate), true, true).await;
        let expected = fixture.expected(
            &["id"],
            first_matching_chunk * CHUNK_ROWS..CHUNKS * CHUNK_ROWS,
        );
        assert_eq!(baseline.rows, expected);
        assert_eq!(filtered.rows, expected);
        for name in ["id", "value"] {
            for (chunk, range) in fixture.payload_ranges[name].iter().enumerate() {
                assert!(baseline.observed.ranges.contains(range));
                assert_eq!(
                    filtered.observed.ranges.contains(range),
                    chunk >= first_matching_chunk
                );
            }
        }
        for name in ["unused_a", "unused_b", "unused_c"] {
            fixture.assert_column_not_read(name, &filtered.observed);
        }
        let baseline_bytes = fixture.payload_bytes(&baseline.observed);
        let filtered_bytes = fixture.payload_bytes(&filtered.observed);
        if first_matching_chunk == 0 {
            assert_eq!(filtered_bytes, baseline_bytes);
        } else {
            assert!(filtered_bytes < baseline_bytes);
        }
        eprintln!(
            "zone pruning: threshold={threshold}, baseline_payload_bytes={baseline_bytes}, filtered_payload_bytes={filtered_bytes}"
        );
    }
}

#[tokio::test]
async fn zoned_null_checks_prune_nonmatching_zones_without_losing_mixed_nulls() {
    let values = (0..CHUNKS * CHUNK_ROWS)
        .map(|row| match row / CHUNK_ROWS {
            0 | 3 => None,
            1 => Some(row as i64),
            _ if row % 2 == 0 => None,
            _ => Some(row as i64),
        })
        .collect::<Vec<_>>();
    let fixture = ColumnFile::new(true, values).await;
    for is_null in [false, true] {
        let predicate = if is_null {
            resolved_col("value").is_null()
        } else {
            resolved_col("value").not_null()
        };
        let expected_ids = fixture
            .values
            .iter()
            .enumerate()
            .filter_map(|(id, value)| (value.is_none() == is_null).then_some(id));
        let expected = fixture.expected(&["id"], expected_ids);
        let baseline = fixture
            .scan(&["id"], Some(predicate.clone()), true, false)
            .await;
        let filtered = fixture.scan(&["id"], Some(predicate), true, true).await;
        assert_eq!(baseline.rows, expected);
        assert_eq!(filtered.rows, expected);
        let excluded_chunks = if is_null { vec![1] } else { vec![0, 3] };
        for name in ["id", "value"] {
            for chunk in &excluded_chunks {
                assert!(
                    !filtered
                        .observed
                        .ranges
                        .contains(&fixture.payload_ranges[name][*chunk])
                );
            }
        }
        assert!(
            fixture.payload_bytes(&filtered.observed) < fixture.payload_bytes(&baseline.observed)
        );
        eprintln!(
            "null pruning: is_null={is_null}, baseline_payload_bytes={}, filtered_payload_bytes={}",
            fixture.payload_bytes(&baseline.observed),
            fixture.payload_bytes(&filtered.observed)
        );
    }
}

#[tokio::test]
async fn non_zoned_filter_does_not_promise_fewer_positional_reads() {
    let fixture = ColumnFile::new(false, clustered_values()).await;
    let predicate = resolved_col("value").gt_eq(lit(2000_i64));
    let baseline = fixture
        .scan(&["value"], Some(predicate.clone()), true, false)
        .await;
    let filtered = fixture.scan(&["value"], Some(predicate), true, true).await;
    let expected = fixture.expected(&["value"], 2 * CHUNK_ROWS..CHUNKS * CHUNK_ROWS);
    assert_eq!(baseline.rows, expected);
    assert_eq!(filtered.rows, expected);
    assert_eq!(
        baseline.observed.completed_bytes,
        filtered.observed.completed_bytes
    );
    assert_eq!(
        baseline
            .observed
            .ranges
            .iter()
            .copied()
            .collect::<BTreeSet<_>>(),
        filtered
            .observed
            .ranges
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
    );
}
