//! Local compatibility experiment, not a Daft connector or a throughput benchmark.

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail, ensure};
use arrow_array::{Array, RecordBatch as ArrowRecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Schema, SchemaRef};
use daft_recordbatch::RecordBatch as DaftRecordBatch;
use futures::TryStreamExt;
use serde::Serialize;
use vortex::{
    VortexSessionDefault,
    arrow::ArrowSessionExt,
    expr::{root, select},
    file::OpenOptionsSessionExt,
    io::session::RuntimeSessionExt,
    layout::scan::split_by::SplitBy,
    session::VortexSession,
};

#[derive(Serialize)]
struct Normalization {
    column: String,
    from: String,
    to: String,
}

#[derive(Default, Serialize)]
struct ScanSummary {
    batches: usize,
    rows: u64,
    largest_batch_rows: usize,
    null_counts: Vec<u64>,
}

#[derive(Serialize)]
struct Summary {
    probe: &'static str,
    vortex_version: &'static str,
    arrow_version: &'static str,
    vortex_features: [&'static str; 3],
    input: PathBuf,
    file_bytes: u64,
    footer_rows: u64,
    batch_size: usize,
    scan_concurrency_per_vortex_worker: usize,
    tokio_worker_threads: usize,
    vortex_dtype: String,
    inferred_arrow_schema: String,
    requested_arrow_schema: String,
    daft_schema: String,
    normalizations: Vec<Normalization>,
    full_scan: ScanSummary,
    zero_column_scan: ScanSummary,
    synthetic_zero_column_rows: usize,
    first_batch_rows_before_drop: usize,
    first_batch_read_after_owner_drop: bool,
    reopened_scan_matches_footer: bool,
    values_and_nulls_roundtrip_checked: bool,
    cancellation_and_memory_bounds_verified: bool,
    daft_io_metrics_available: bool,
}

/// Daft's current native schema conversion rejects Arrow's view string/binary
/// types. Ask Vortex to export ordinary variable-width arrays instead. This may
/// allocate and is deliberately visible in the result, not called zero-copy.
fn export_schema(inferred: &Schema) -> Result<(SchemaRef, Vec<Normalization>)> {
    let mut normalizations = Vec::new();
    let mut fields = Vec::with_capacity(inferred.fields().len());
    for field in inferred.fields() {
        let target = match field.data_type() {
            DataType::Utf8View => DataType::Utf8,
            DataType::BinaryView => DataType::Binary,
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary => field.data_type().clone(),
            unsupported => bail!(
                "probe only supports flat primitive fixtures; column {:?} has {unsupported:?}",
                field.name()
            ),
        };
        if &target != field.data_type() {
            normalizations.push(Normalization {
                column: field.name().clone(),
                from: format!("{:?}", field.data_type()),
                to: format!("{target:?}"),
            });
        }
        fields.push(field.as_ref().clone().with_data_type(target));
    }
    Ok((
        Arc::new(Schema::new_with_metadata(
            fields,
            inferred.metadata().clone(),
        )),
        normalizations,
    ))
}

fn to_daft(batch: &ArrowRecordBatch) -> Result<DaftRecordBatch> {
    if batch.num_columns() == 0 {
        // RecordBatch::from_arrow rejects zero arrays. Carry cardinality through
        // the explicit-size constructor instead of inferring it from columns.
        Ok(DaftRecordBatch::new_with_size(
            DaftRecordBatch::empty(None).schema,
            vec![],
            batch.num_rows(),
        )?)
    } else {
        Ok(DaftRecordBatch::try_from(batch)?)
    }
}

/// Verify conversion, including values and null locations. Daft normalizes
/// variable-width offsets, so cast the roundtrip back to the input Arrow type
/// before comparing array data. Arrow 59.2 compares valid float bytes exactly
/// (including NaN payloads and signed zero), checks null masks, and ignores the
/// storage underneath nulls. This checking is not timed as a scan.
fn verify_roundtrip(expected: &ArrowRecordBatch, actual: &DaftRecordBatch) -> Result<()> {
    ensure!(
        actual.len() == expected.num_rows(),
        "conversion changed row count"
    );
    ensure!(
        actual.num_columns() == expected.num_columns(),
        "conversion changed column count"
    );
    if expected.num_columns() == 0 {
        return Ok(());
    }
    let roundtrip = ArrowRecordBatch::try_from(actual.clone())?;
    for (index, expected_array) in expected.columns().iter().enumerate() {
        ensure!(
            expected.schema().field(index).name() == roundtrip.schema().field(index).name(),
            "conversion changed column name at index {index}"
        );
        let restored = arrow_cast::cast(roundtrip.column(index), expected_array.data_type())
            .with_context(|| format!("restore Arrow type of column {index}"))?;
        ensure!(
            expected_array.to_data() == restored.to_data(),
            "conversion changed values/nulls in column {:?}",
            expected.schema().field(index).name()
        );
    }
    Ok(())
}

fn record_batch(
    summary: &mut ScanSummary,
    batch: &ArrowRecordBatch,
    batch_size: usize,
) -> Result<()> {
    ensure!(
        batch.num_rows() <= batch_size,
        "scan exceeded requested row split size"
    );
    ensure!(
        summary.null_counts.len() == batch.num_columns(),
        "scan changed its column count"
    );
    summary.batches += 1;
    summary.rows += u64::try_from(batch.num_rows())?;
    summary.largest_batch_rows = summary.largest_batch_rows.max(batch.num_rows());
    for (count, array) in summary.null_counts.iter_mut().zip(batch.columns()) {
        *count += u64::try_from(array.null_count())?;
    }
    Ok(())
}

async fn run(path: PathBuf, batch_size: usize) -> Result<Summary> {
    let metadata = std::fs::metadata(&path)
        .with_context(|| format!("inspect local fixture {}", path.display()))?;
    ensure!(
        metadata.is_file(),
        "expected a local file: {}",
        path.display()
    );

    let session = VortexSession::default().with_tokio();
    let file = session.open_options().open_path(&path).await?;
    let footer_rows = file.row_count();
    let vortex_dtype = file.dtype().to_string();
    let inferred = session.arrow().to_arrow_schema(file.dtype())?;
    let (schema, normalizations) = export_schema(&inferred)?;

    // Consume at most one batch, retain its arrays, then drop stream, file, and
    // session. This exercises early drop/reopen and array ownership; it does not
    // establish cancellation latency or prove all background work was aborted.
    let mut early_stream = Box::pin(
        file.scan()?
            .with_split_by(SplitBy::RowCount(batch_size))
            .with_concurrency(1)
            .into_record_batch_stream(schema.clone())?,
    );
    let first = early_stream.try_next().await?;
    let first_daft = first.as_ref().map(to_daft).transpose()?;
    drop(early_stream);
    drop(file);
    drop(session);
    if let (Some(arrow), Some(daft)) = (&first, &first_daft) {
        verify_roundtrip(arrow, daft).context("read retained batch after reader/session drop")?;
    }

    let session = VortexSession::default().with_tokio();
    let file = session.open_options().open_path(&path).await?;
    ensure!(
        file.row_count() == footer_rows,
        "row count changed on reopen"
    );
    let (reopened_schema, _) = export_schema(&session.arrow().to_arrow_schema(file.dtype())?)?;
    ensure!(reopened_schema == schema, "schema changed on reopen");
    let mut stream = Box::pin(
        file.scan()?
            .with_split_by(SplitBy::RowCount(batch_size))
            .with_concurrency(1)
            .into_record_batch_stream(schema.clone())?,
    );
    let mut full_scan = ScanSummary {
        null_counts: vec![0; schema.fields().len()],
        ..Default::default()
    };
    // Infer even an empty file's Daft schema through a schema-bearing empty batch.
    let mut daft_schema = format!(
        "{:?}",
        to_daft(&ArrowRecordBatch::new_empty(schema.clone()))?.schema
    );
    while let Some(batch) = stream.try_next().await? {
        let converted = to_daft(&batch)?;
        verify_roundtrip(&batch, &converted)?;
        daft_schema = format!("{:?}", converted.schema);
        record_batch(&mut full_scan, &batch, batch_size)?;
    }
    drop(stream);
    ensure!(
        full_scan.rows == footer_rows,
        "full scan disagrees with footer count"
    );

    // Ask Vortex for an actual zero-column projection, rather than synthesizing
    // counts from footer metadata. This still exercises the reader boundary.
    let empty_projection = select(Vec::<&str>::new(), root()).bind(file.dtype())?;
    let mut zero_stream = Box::pin(
        file.scan()?
            .with_projection(empty_projection)
            .with_split_by(SplitBy::RowCount(batch_size))
            .with_concurrency(1)
            .into_record_batch_stream(Arc::new(Schema::empty()))?,
    );
    let mut zero_column_scan = ScanSummary::default();
    while let Some(batch) = zero_stream.try_next().await? {
        ensure!(
            batch.num_columns() == 0,
            "zero-column projection returned columns"
        );
        verify_roundtrip(&batch, &to_daft(&batch)?)?;
        record_batch(&mut zero_column_scan, &batch, batch_size)?;
    }
    drop(zero_stream);
    ensure!(
        zero_column_scan.rows == footer_rows,
        "zero-column projection lost cardinality"
    );

    // Ensure the explicit-size path is exercised even for an empty fixture.
    let synthetic_rows = 3;
    let synthetic_zero = ArrowRecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(synthetic_rows)),
    )?;
    verify_roundtrip(&synthetic_zero, &to_daft(&synthetic_zero)?)?;

    Ok(Summary {
        probe: "native-vortex-arrow-daft-phase0",
        vortex_version: "0.88.0",
        arrow_version: "59.2.0",
        vortex_features: ["files", "tokio", "zstd"],
        input: path,
        file_bytes: metadata.len(),
        footer_rows,
        batch_size,
        scan_concurrency_per_vortex_worker: 1,
        tokio_worker_threads: 2,
        vortex_dtype,
        inferred_arrow_schema: format!("{inferred:?}"),
        requested_arrow_schema: format!("{schema:?}"),
        daft_schema,
        normalizations,
        full_scan,
        zero_column_scan,
        synthetic_zero_column_rows: synthetic_rows,
        first_batch_rows_before_drop: first.as_ref().map_or(0, ArrowRecordBatch::num_rows),
        first_batch_read_after_owner_drop: first.is_some(),
        reopened_scan_matches_footer: true,
        values_and_nulls_roundtrip_checked: true,
        cancellation_and_memory_bounds_verified: false,
        daft_io_metrics_available: false,
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let path = args.next().map(PathBuf::from).context(
        "usage: daft-vortex-compatibility-probe <local-file.vortex> [positive-batch-size]",
    )?;
    let batch_size = args
        .next()
        .map(|value| {
            value
                .into_string()
                .map_err(|_| anyhow::anyhow!("batch size must be UTF-8"))?
                .parse::<usize>()
                .context("batch size must be a positive integer")
        })
        .transpose()?
        .unwrap_or(7);
    ensure!(batch_size > 0, "batch size must be a positive integer");
    ensure!(args.next().is_none(), "unexpected extra argument");
    let summary = run(path.clone(), batch_size)
        .await
        .with_context(|| format!("Vortex compatibility probe failed for {}", path.display()))?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
