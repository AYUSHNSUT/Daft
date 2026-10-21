use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use arrow_array::RecordBatch as ArrowRecordBatch;
use arrow_schema::{DataType as ArrowDataType, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use common_error::{DaftError, DaftResult};
use daft_core::prelude::{Field, Schema, SchemaRef};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_io::{IOClient, IOStatsRef};
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use vortex::{
    VortexSessionDefault,
    arrow::ArrowSessionExt,
    dtype::{DType, Nullability},
    file::{OpenOptionsSessionExt, VortexFile},
    io::session::RuntimeSessionExt,
    layout::scan::split_by::SplitBy,
    session::VortexSession,
};

use crate::{
    VortexConvertOptions, VortexReadOptions,
    diagnostics::{self, Phase},
    io::{DaftReadAt, display_uri, validate_options},
    plan::plan_scan,
    snapshot::{Budget, MAX_SNAPSHOT_BYTES},
};

// Daft Field has no nullability member, and its equality ignores metadata. Keep
// this marker on the inferred scan schema and explicitly compare it for every
// input file. It is not attached to the Arrow export or output record batches.
pub(crate) const NULLABILITY_METADATA_KEY: &str = "daft.vortex.nullable";
pub(crate) const MAX_CONCURRENT_METADATA_FILES: usize = 64;

fn read_error(uri: &str, operation: &str, error: impl std::fmt::Display) -> DaftError {
    let uri = display_uri(uri);
    DaftError::ComputeError(format!("Vortex {operation} for {uri:?}: {error}"))
}

fn local_path(uri: &str) -> DaftResult<PathBuf> {
    if uri.is_empty() {
        return Err(DaftError::ValueError(
            "Vortex path must not be empty".into(),
        ));
    }
    // In particular, Windows drive paths must not be interpreted as URL schemes.
    if Path::new(uri).is_absolute() {
        return Ok(PathBuf::from(uri));
    }
    if let Some((scheme, after_scheme)) = uri.split_once(':')
        && scheme.eq_ignore_ascii_case("file")
    {
        if uri.contains('?') || uri.contains('#') {
            return Err(DaftError::ValueError(format!(
                "Vortex file URL must not contain a query or fragment: {:?}",
                display_uri(uri)
            )));
        }
        // Follow Daft local I/O's literal file-URI convention. Url::to_file_path
        // would percent-decode and could select a different existing file.
        // Inspect the original authority because Url normalizes localhost away.
        let path = if let Some(authority_and_path) = after_scheme.strip_prefix("//") {
            if !authority_and_path.starts_with('/') && !authority_and_path.is_empty() {
                return Err(DaftError::ValueError(format!(
                    "Vortex file:// URLs must not contain a host, got {:?}",
                    display_uri(uri)
                )));
            }
            authority_and_path
        } else {
            after_scheme
        };
        #[cfg(windows)]
        let path = if path.starts_with('/') && path.as_bytes().get(2) == Some(&b':') {
            &path[1..]
        } else {
            path
        };
        return Ok(PathBuf::from(path));
    }
    if url::Url::parse(uri).is_ok() {
        return Err(DaftError::NotImplemented(format!(
            "Vortex supports only local paths, file://, http(s):// and s3:// URLs, got {:?}",
            display_uri(uri)
        )));
    }
    Ok(PathBuf::from(uri))
}

fn input_uri(uri: &str) -> DaftResult<String> {
    if !Path::new(uri).is_absolute() {
        match url::Url::parse(uri) {
            Ok(parsed) if matches!(parsed.scheme(), "http" | "https" | "s3") => {
                if parsed.host_str().is_none() {
                    return Err(DaftError::ValueError(format!(
                        "Vortex remote URL requires a host: {:?}",
                        display_uri(uri)
                    )));
                }
                return Ok(uri.to_string());
            }
            Err(_) if uri.contains("://") => {
                return Err(DaftError::ValueError(format!(
                    "Invalid Vortex URL: {:?}",
                    display_uri(uri)
                )));
            }
            _ => {}
        }
    }
    local_path(uri).map(|path| path.to_string_lossy().into_owned())
}

/// Fresh scan-processor state. Never serialized or shared between queries.
#[derive(Default)]
pub struct VortexExecutionContext {
    session: OnceLock<VortexSession>,
}

fn session_for_open(
    phase: Phase,
    uri: &str,
    shared_session: Option<&OnceLock<VortexSession>>,
) -> VortexSession {
    let create = |uri| {
        let trace = diagnostics::begin(phase, "session_creation", uri);
        let session = if diagnostics::instant_ns().is_some() {
            VortexSession::default().with_handle(crate::runtime::handle())
        } else {
            VortexSession::default().with_tokio()
        };
        diagnostics::complete(trace, false);
        session
    };
    match shared_session {
        // A row-count bulk call reuses session initialization. Its file opens
        // keep independent readers, footers and snapshot state.
        Some(shared) => shared.get_or_init(|| create(None)).clone(),
        None => create(Some(uri)),
    }
}

async fn open_file(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    options: &VortexReadOptions,
    phase: Phase,
    shared_session: Option<&OnceLock<VortexSession>>,
) -> DaftResult<(VortexSession, VortexFile)> {
    validate_options(options)?;
    let uri = input_uri(uri)?;
    tokio::runtime::Handle::try_current()
        .map_err(|error| read_error(&uri, "requires an active Tokio runtime", error))?;
    let trace = diagnostics::begin(phase, "open", Some(&uri));
    let result: DaftResult<_> = async {
        let session = session_for_open(phase, &uri, shared_session);
        let reader = Arc::new(
            DaftReadAt::new_with_phase(uri.clone(), io_client, io_stats, options, phase).await?,
        );
        let sdk_trace = diagnostics::begin(phase, "sdk_open", Some(&uri));
        let file = session
            .open_options()
            .with_initial_read_size(options.max_in_flight_bytes.min(65535))
            .open(reader.clone())
            .await
            .map_err(|error| read_error(&uri, "open", error));
        diagnostics::complete(sdk_trace, file.is_err());
        let file = file?;
        reader.mark_data_phase();
        Ok((session, file))
    }
    .await;
    diagnostics::complete(trace, result.is_err());
    result
}

/// Build the Arrow export schema and the full Daft scan schema separately, so
/// private validation metadata does not leak into output Arrow fields.
pub(crate) fn schemas(
    uri: &str,
    session: &VortexSession,
    dtype: &DType,
) -> DaftResult<(ArrowSchemaRef, SchemaRef)> {
    schemas_with_offsets(uri, session, dtype, false)
}

fn schemas_with_offsets(
    uri: &str,
    session: &VortexSession,
    dtype: &DType,
    large_offsets: bool,
) -> DaftResult<(ArrowSchemaRef, SchemaRef)> {
    let safe_uri = display_uri(uri);
    let uri = safe_uri.as_str();
    if !matches!(dtype, DType::Struct(_, Nullability::NonNullable)) {
        return Err(DaftError::TypeError(format!(
            "Vortex {uri:?} requires a non-nullable top-level struct, got {dtype}"
        )));
    }
    let inferred = session
        .arrow()
        .to_arrow_schema(dtype)
        .map_err(|error| read_error(uri, "schema inference", error))?;
    let mut arrow_fields = Vec::with_capacity(inferred.fields().len());
    let mut daft_fields = Vec::with_capacity(inferred.fields().len());
    let mut names = HashSet::new();
    for field in inferred.fields() {
        if !names.insert(field.name()) {
            return Err(DaftError::SchemaMismatch(format!(
                "Vortex {uri:?} contains duplicate column name {:?}",
                field.name()
            )));
        }
        if field.extension_type_name().is_some() {
            return Err(DaftError::TypeError(format!(
                "Vortex {uri:?} does not support extension column {:?}",
                field.name()
            )));
        }
        let target = match field.data_type() {
            ArrowDataType::Utf8View | ArrowDataType::Utf8 if large_offsets => {
                ArrowDataType::LargeUtf8
            }
            ArrowDataType::BinaryView | ArrowDataType::Binary if large_offsets => {
                ArrowDataType::LargeBinary
            }
            ArrowDataType::Utf8View => ArrowDataType::Utf8,
            ArrowDataType::BinaryView => ArrowDataType::Binary,
            ArrowDataType::Null
            | ArrowDataType::Boolean
            | ArrowDataType::Int8
            | ArrowDataType::Int16
            | ArrowDataType::Int32
            | ArrowDataType::Int64
            | ArrowDataType::UInt8
            | ArrowDataType::UInt16
            | ArrowDataType::UInt32
            | ArrowDataType::UInt64
            | ArrowDataType::Float32
            | ArrowDataType::Float64
            | ArrowDataType::Utf8
            | ArrowDataType::LargeUtf8
            | ArrowDataType::Binary
            | ArrowDataType::LargeBinary => field.data_type().clone(),
            unsupported => {
                return Err(DaftError::TypeError(format!(
                    "Vortex {uri:?} supports only flat primitive columns; {:?} has {unsupported:?}",
                    field.name()
                )));
            }
        };
        // View normalization can allocate; no zero-copy guarantee is made.
        let mut export_metadata = field.metadata().clone();
        export_metadata.remove(NULLABILITY_METADATA_KEY);
        let export_field = field
            .as_ref()
            .clone()
            .with_data_type(target)
            .with_metadata(export_metadata);
        let daft_field = Field::try_from(&export_field)
            .map_err(|error| read_error(uri, "schema conversion", error))?;
        let mut metadata = daft_field.metadata.as_ref().clone();
        metadata.insert(
            NULLABILITY_METADATA_KEY.to_string(),
            field.is_nullable().to_string(),
        );
        daft_fields.push(daft_field.with_metadata(metadata));
        arrow_fields.push(export_field);
    }
    Ok((
        Arc::new(ArrowSchema::new_with_metadata(
            arrow_fields,
            inferred.metadata().clone(),
        )),
        Arc::new(Schema::new(daft_fields)),
    ))
}

pub(crate) fn validate_schema(uri: &str, expected: &Schema, actual: &Schema) -> DaftResult<()> {
    let safe_uri = display_uri(uri);
    let uri = safe_uri.as_str();
    if expected.len() != actual.len() {
        return Err(DaftError::SchemaMismatch(format!(
            "Vortex {uri:?} has {} columns; expected {} in the full first-file schema",
            actual.len(),
            expected.len()
        )));
    }
    for (index, (expected, actual)) in expected.fields().iter().zip(actual.fields()).enumerate() {
        if expected.name != actual.name || expected.dtype != actual.dtype {
            return Err(DaftError::SchemaMismatch(format!(
                "Vortex {uri:?} full schema differs at column {index}: expected {expected}, got {actual}"
            )));
        }
        let nullable = expected.metadata.get(NULLABILITY_METADATA_KEY);
        if !matches!(nullable.map(String::as_str), Some("true" | "false")) {
            return Err(DaftError::SchemaMismatch(format!(
                "Vortex {uri:?} expected schema lacks source nullability for column {:?}; use the full schema returned by read_vortex_schema",
                expected.name
            )));
        }
        if nullable != actual.metadata.get(NULLABILITY_METADATA_KEY) {
            return Err(DaftError::SchemaMismatch(format!(
                "Vortex {uri:?} source nullability differs for column {:?}: expected {:?}, got {:?}",
                expected.name,
                nullable,
                actual.metadata.get(NULLABILITY_METADATA_KEY)
            )));
        }
    }
    Ok(())
}

fn to_daft(batch: &ArrowRecordBatch) -> DaftResult<RecordBatch> {
    if batch.num_columns() == 0 {
        RecordBatch::new_with_size(Schema::empty(), vec![], batch.num_rows())
    } else {
        RecordBatch::try_from(batch)
    }
}

/// Infer the complete schema, retaining source nullability for strict validation
/// across files. The same configured client and budgets govern footer reads.
pub async fn read_vortex_schema(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    read_options: VortexReadOptions,
) -> DaftResult<SchemaRef> {
    Ok(
        read_metadata(uri, io_client, io_stats, read_options, Phase::Schema)
            .await?
            .0,
    )
}

/// Read schema and unfiltered row count from the footer without decoding rows.
///
/// Refresh this metadata when planning tasks rather than caching counts in a
/// source that can be reused or transported to another worker.
pub async fn read_vortex_metadata(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    read_options: VortexReadOptions,
) -> DaftResult<(SchemaRef, u64)> {
    read_metadata(
        uri,
        io_client,
        io_stats,
        read_options,
        Phase::PlanningMetadata,
    )
    .await
}

pub(crate) async fn read_metadata(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    read_options: VortexReadOptions,
    phase: Phase,
) -> DaftResult<(SchemaRef, u64)> {
    let (schema, rows) =
        read_metadata_inner(uri, io_client, io_stats, read_options, phase, true, None).await?;
    Ok((schema.expect("schema conversion was requested"), rows))
}

async fn read_metadata_inner(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    read_options: VortexReadOptions,
    phase: Phase,
    convert_schema: bool,
    shared_session: Option<&OnceLock<VortexSession>>,
) -> DaftResult<(Option<SchemaRef>, u64)> {
    let (session, file) = open_file(
        uri,
        io_client,
        io_stats,
        &read_options,
        phase,
        shared_session,
    )
    .await?;
    let schema = if convert_schema {
        let trace = diagnostics::begin(phase, "schema_conversion", Some(uri));
        let schema = schemas(&display_uri(uri), &session, file.dtype());
        diagnostics::complete(trace, schema.is_err());
        Some(schema?.1)
    } else {
        None
    };
    Ok((schema, file.row_count()))
}

/// Refresh metadata for several files, preserving their input order.
///
/// At most `max_concurrent_files` files are opened concurrently. Each open keeps
/// its own request and byte budgets from `read_options`; these budgets are not
/// shared across files. Dropping the future drops pending opens, with no spawned
/// tasks or metadata cache retained by this helper.
pub async fn read_vortex_metadata_bulk(
    uris: &[String],
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    read_options: VortexReadOptions,
    max_concurrent_files: usize,
) -> DaftResult<Vec<(SchemaRef, u64)>> {
    Ok(read_metadata_bulk_inner(
        uris,
        io_client,
        io_stats,
        read_options,
        max_concurrent_files,
        true,
    )
    .await?
    .into_iter()
    .map(|(schema, rows)| (schema.expect("schema conversion was requested"), rows))
    .collect())
}

/// Refresh row counts with one session per bulk call and no unused schema conversion.
/// Execution still validates every full schema using its own session and reader.
#[cfg(any(feature = "python", test))]
pub(crate) async fn read_vortex_row_counts_bulk(
    uris: &[String],
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    read_options: VortexReadOptions,
    max_concurrent_files: usize,
) -> DaftResult<Vec<u64>> {
    Ok(read_metadata_bulk_inner(
        uris,
        io_client,
        io_stats,
        read_options,
        max_concurrent_files,
        false,
    )
    .await?
    .into_iter()
    .map(|(_, rows)| rows)
    .collect())
}

async fn read_metadata_bulk_inner(
    uris: &[String],
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    read_options: VortexReadOptions,
    max_concurrent_files: usize,
    convert_schema: bool,
) -> DaftResult<Vec<(Option<SchemaRef>, u64)>> {
    validate_options(&read_options)?;
    if !(1..=MAX_CONCURRENT_METADATA_FILES).contains(&max_concurrent_files) {
        return Err(DaftError::ValueError(format!(
            "Vortex max_concurrent_files must be between 1 and {MAX_CONCURRENT_METADATA_FILES}"
        )));
    }
    // Keep the full-schema path as the baseline. Lazy initialization preserves
    // per-file validation order and avoids runtime work for empty input.
    let shared_session = (!convert_schema).then(OnceLock::new);
    futures::stream::iter(uris.iter().cloned())
        .map(|uri| {
            let io_client = io_client.clone();
            let io_stats = io_stats.clone();
            let read_options = read_options.clone();
            let shared_session = shared_session.as_ref();
            async move {
                read_metadata_inner(
                    &uri,
                    io_client,
                    io_stats,
                    read_options,
                    Phase::PlanningMetadata,
                    convert_schema,
                    shared_session,
                )
                .await
            }
        })
        .buffered(max_concurrent_files)
        .try_collect()
        .await
}

/// Fresh metadata plus bounded opening ranges for S3 objects with strong validators.
/// Snapshots are query-scoped and contain no live reader/session handles.
pub type VortexMetadataWithSnapshot = (SchemaRef, u64, Option<Vec<u8>>);

pub async fn read_vortex_metadata_bulk_cached(
    uris: &[String],
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    read_options: VortexReadOptions,
    max_concurrent_files: usize,
    max_snapshot_bytes: usize,
) -> DaftResult<Vec<VortexMetadataWithSnapshot>> {
    Ok(read_metadata_bulk_cached_inner(
        uris,
        io_client,
        io_stats,
        read_options,
        max_concurrent_files,
        max_snapshot_bytes,
        true,
    )
    .await?
    .into_iter()
    .map(|(schema, rows, snapshot)| {
        (
            schema.expect("schema conversion was requested"),
            rows,
            snapshot,
        )
    })
    .collect())
}

#[cfg(any(feature = "python", test))]
pub(crate) async fn read_vortex_row_counts_bulk_cached(
    uris: &[String],
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    read_options: VortexReadOptions,
    max_concurrent_files: usize,
    max_snapshot_bytes: usize,
) -> DaftResult<Vec<(u64, Option<Vec<u8>>)>> {
    Ok(read_metadata_bulk_cached_inner(
        uris,
        io_client,
        io_stats,
        read_options,
        max_concurrent_files,
        max_snapshot_bytes,
        false,
    )
    .await?
    .into_iter()
    .map(|(_, rows, snapshot)| (rows, snapshot))
    .collect())
}

type OptionalMetadataWithSnapshot = (Option<SchemaRef>, u64, Option<Vec<u8>>);

async fn read_metadata_bulk_cached_inner(
    uris: &[String],
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    read_options: VortexReadOptions,
    max_concurrent_files: usize,
    max_snapshot_bytes: usize,
    convert_schema: bool,
) -> DaftResult<Vec<OptionalMetadataWithSnapshot>> {
    validate_options(&read_options)?;
    if !(1..=MAX_CONCURRENT_METADATA_FILES).contains(&max_concurrent_files)
        || max_snapshot_bytes > MAX_SNAPSHOT_BYTES
    {
        return Err(DaftError::ValueError(
            "Vortex cached metadata requires concurrency 1..=64 and a budget of at most 64 MiB"
                .into(),
        ));
    }
    let budget = Arc::new(Budget::new(max_snapshot_bytes));
    let shared_session = (!convert_schema).then(OnceLock::new);
    let reads = futures::stream::iter(uris.iter().cloned()).map(|uri| {
        let shared_session = shared_session.as_ref();
        let (client, stats, options, budget) = (
            io_client.clone(),
            io_stats.clone(),
            read_options.clone(),
            budget.clone(),
        );
        async move {
            let uri = input_uri(&uri)?;
            tokio::runtime::Handle::try_current()
                .map_err(|error| read_error(&uri, "requires an active Tokio runtime", error))?;
            let phase = Phase::PlanningMetadata;
            let trace = diagnostics::begin(phase, "open", Some(&uri));
            let result: DaftResult<_> = async {
                let session = session_for_open(phase, &uri, shared_session);
                let reader = Arc::new(
                    DaftReadAt::new_for_snapshot(
                        uri.clone(),
                        client,
                        stats,
                        &options,
                        phase,
                        budget,
                    )
                    .await?,
                );
                let sdk_trace = diagnostics::begin(phase, "sdk_open", Some(&uri));
                let file = session
                    .open_options()
                    .with_initial_read_size(options.max_in_flight_bytes.min(65535))
                    .open(reader.clone())
                    .await
                    .map_err(|error| read_error(&uri, "open", error));
                diagnostics::complete(sdk_trace, file.is_err());
                let file = file?;
                let schema = if convert_schema {
                    let schema_trace = diagnostics::begin(phase, "schema_conversion", Some(&uri));
                    let schema = schemas(&uri, &session, file.dtype());
                    diagnostics::complete(schema_trace, schema.is_err());
                    Some(schema?.1)
                } else {
                    None
                };
                let snapshot_trace = diagnostics::begin(phase, "snapshot_encode", Some(&uri));
                let snapshot = reader.take_snapshot(max_snapshot_bytes);
                diagnostics::complete(snapshot_trace, snapshot.is_err());
                let snapshot = snapshot?;
                Ok((schema, file.row_count(), snapshot))
            }
            .await;
            diagnostics::complete(trace, result.is_err());
            result
        }
    });
    let mut reads = reads.buffered(max_concurrent_files);
    let mut remaining = max_snapshot_bytes;
    let mut results = Vec::with_capacity(uris.len());
    while let Some((schema, rows, mut snapshot)) = reads.try_next().await? {
        if let Some(bytes) = &snapshot {
            if bytes.len() <= remaining {
                remaining -= bytes.len();
            } else {
                snapshot = None;
            }
        }
        results.push((schema, rows, snapshot));
    }
    Ok(results)
}

struct ScanState {
    batches: Option<BoxStream<'static, DaftResult<ArrowRecordBatch>>>,
    // Keep the session/file owners alive until EOF, limit, or stream drop.
    reader: Option<(VortexSession, VortexFile)>,
    predicate: Option<BoundExpr>,
    projection: Option<Vec<usize>>,
    remaining: Option<usize>,
    uri: String,
    first_batch: Option<diagnostics::Span>,
}

/// Stream one file. Validate the complete schema first, then apply the
/// complete residual filter, final projection, and exact post-filter limit.
pub async fn stream_vortex(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    read_options: VortexReadOptions,
    convert_options: VortexConvertOptions,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    if read_options.batch_size == 0 {
        return Err(DaftError::ValueError(format!(
            "Vortex batch_size must be positive for {:?}",
            display_uri(uri)
        )));
    }
    let uri = input_uri(uri)?;
    let first_batch = diagnostics::begin(Phase::Execution, "first_batch", Some(&uri));
    let (session, file) = open_file(
        &uri,
        io_client,
        io_stats,
        &read_options,
        Phase::ExecutionOpen,
        read_options
            .execution_context
            .as_ref()
            .map(|context| &context.session),
    )
    .await?;
    stream_opened_vortex(
        &uri,
        session,
        file,
        read_options,
        convert_options,
        first_batch,
    )
}

// The returned stream owns the opened file and session until EOF, an error,
// its exact limit, or consumer cancellation.
fn stream_opened_vortex(
    uri: &str,
    session: VortexSession,
    file: VortexFile,
    read_options: VortexReadOptions,
    convert_options: VortexConvertOptions,
    first_batch: Option<diagnostics::Span>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let trace = diagnostics::begin(Phase::ExecutionOpen, "schema_conversion", Some(uri));
    let schema = schemas_with_offsets(
        uri,
        &session,
        file.dtype(),
        read_options.enable_large_offsets,
    );
    diagnostics::complete(trace, schema.is_err());
    let (arrow_schema, actual_schema) = schema?;
    let trace = diagnostics::begin(Phase::ExecutionOpen, "schema_validation", Some(uri));
    let valid = validate_schema(uri, &convert_options.schema, &actual_schema);
    diagnostics::complete(trace, valid.is_err());
    valid?;
    let trace = diagnostics::begin(Phase::ExecutionOpen, "scan_planning", Some(uri));
    let plan = plan_scan(
        &actual_schema,
        file.dtype(),
        &read_options,
        &convert_options,
    )
    .map_err(|error| read_error(uri, "scan planning", error));
    diagnostics::complete(trace, plan.is_err());
    let plan = plan?;
    if read_options.enable_file_pruning
        && let Some(predicate) = &plan.pruning_filter
    {
        let trace = diagnostics::begin(Phase::ExecutionOpen, "file_pruning", Some(uri));
        // Unknown or unusable statistics retain the normal scan path.
        let pruned = file.can_prune(predicate).unwrap_or(false);
        diagnostics::complete(trace, false);
        if pruned {
            diagnostics::complete(first_batch, false);
            return Ok(futures::stream::empty().boxed());
        }
    }
    let trace = diagnostics::begin(Phase::ExecutionOpen, "stream_preparation", Some(uri));
    let prepared = (|| -> DaftResult<_> {
        let arrow_schema = Arc::new(
            arrow_schema
                .project(&plan.physical_indices)
                .map_err(|error| read_error(uri, "physical Arrow schema", error))?,
        );
        // Even limit=0 validates the full schema, including projected-away fields.
        if convert_options.limit == Some(0) {
            return Ok(None);
        }
        let error_uri = uri.to_string();
        let batches = file
            .scan()
            .map_err(|error| read_error(uri, "scan preparation", error))?
            .with_projection(plan.projection)
            .with_some_filter(plan.pushed_filter)
            .with_split_by(if read_options.enable_layout_splitting {
                SplitBy::LayoutSubSplitting {
                    max_rows: read_options.batch_size as u64,
                }
            } else {
                SplitBy::RowCount(read_options.batch_size)
            })
            // Vortex interprets this per Vortex worker, not as a global I/O bound.
            .with_concurrency(1)
            .into_record_batch_stream(arrow_schema)
            .map_err(|error| read_error(uri, "Arrow stream preparation", error))?
            .map_err(move |error| read_error(&error_uri, "decode", error))
            .boxed();
        Ok(Some(batches))
    })();
    diagnostics::complete(trace, prepared.is_err());
    let Some(batches) = prepared? else {
        return Ok(futures::stream::empty().boxed());
    };
    let state = ScanState {
        batches: Some(batches),
        reader: Some((session, file)),
        predicate: plan.residual,
        projection: Some(plan.output_indices),
        remaining: convert_options.limit,
        uri: uri.to_string(),
        first_batch,
    };
    Ok(futures::stream::try_unfold(state, |mut state| async move {
        loop {
            let Some(source) = state.batches.as_mut() else {
                return Ok(None);
            };
            // Awaiting the next batch includes remote I/O and decode work.
            let decoded = diagnostics::measure_async(
                Phase::Execution,
                "batch_wait_decode",
                Some(&state.uri),
                source.try_next(),
            )
            .await;
            let Some(arrow) = decoded? else {
                return Ok(None);
            };
            let trace = diagnostics::begin(Phase::Execution, "arrow_conversion", Some(&state.uri));
            let converted = to_daft(&arrow);
            diagnostics::complete(trace, converted.is_err());
            let mut batch =
                converted.map_err(|error| read_error(&state.uri, "Arrow conversion", error))?;
            if let Some(predicate) = &state.predicate {
                let trace =
                    diagnostics::begin(Phase::Execution, "residual_filter", Some(&state.uri));
                let filtered = batch.filter(std::slice::from_ref(predicate));
                diagnostics::complete(trace, filtered.is_err());
                batch =
                    filtered.map_err(|error| read_error(&state.uri, "residual filter", error))?;
            }
            if batch.is_empty() {
                continue;
            }
            if let Some(indices) = &state.projection {
                batch = if indices.is_empty() {
                    RecordBatch::new_with_size(Schema::empty(), vec![], batch.len())?
                } else {
                    batch.get_columns(indices)
                };
            }
            if let Some(remaining) = state.remaining {
                let emit = remaining.min(batch.len());
                batch = batch
                    .head(emit)
                    .map_err(|error| read_error(&state.uri, "limit", error))?;
                state.remaining = Some(remaining - emit);
                if state.remaining == Some(0) {
                    // Stop polling and release source ownership immediately.
                    state.batches = None;
                    state.reader = None;
                }
            }
            diagnostics::complete(state.first_batch.take(), false);
            return Ok(Some((batch, state)));
        }
    })
    .boxed())
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod lifecycle_tests;

#[cfg(test)]
#[path = "reader_controls_tests.rs"]
mod reader_controls_tests;

#[cfg(test)]
#[path = "pushdown_tests.rs"]
mod pushdown_tests;
