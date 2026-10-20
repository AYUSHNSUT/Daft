use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
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
    io::{DaftReadAt, display_uri, validate_options},
    plan::plan_scan,
};

// Daft Field has no nullability member, and its equality ignores metadata. Keep
// this marker on the inferred scan schema and explicitly compare it for every
// input file. It is not attached to the Arrow export or output record batches.
pub(crate) const NULLABILITY_METADATA_KEY: &str = "daft.vortex.nullable";

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

async fn open_file(
    uri: &str,
    io_client: Arc<IOClient>,
    io_stats: IOStatsRef,
    options: &VortexReadOptions,
) -> DaftResult<(VortexSession, VortexFile)> {
    validate_options(options)?;
    let uri = input_uri(uri)?;
    tokio::runtime::Handle::try_current()
        .map_err(|error| read_error(&uri, "requires an active Tokio runtime", error))?;
    let session = VortexSession::default().with_tokio();
    let reader = Arc::new(DaftReadAt::new(uri.clone(), io_client, io_stats, options).await?);
    let file = session
        .open_options()
        .with_initial_read_size(options.max_in_flight_bytes.min(65535))
        .open(reader)
        .await
        .map_err(|error| read_error(&uri, "open", error))?;
    Ok((session, file))
}

/// Build the Arrow export schema and the full Daft scan schema separately, so
/// private validation metadata does not leak into output Arrow fields.
pub(crate) fn schemas(
    uri: &str,
    session: &VortexSession,
    dtype: &DType,
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
    let (session, file) = open_file(uri, io_client, io_stats, &read_options).await?;
    Ok(schemas(&display_uri(uri), &session, file.dtype())?.1)
}

struct ScanState {
    batches: Option<BoxStream<'static, DaftResult<ArrowRecordBatch>>>,
    // Keep the session/file owners alive until EOF, limit, or stream drop.
    reader: Option<(VortexSession, VortexFile)>,
    predicate: Option<BoundExpr>,
    projection: Option<Vec<usize>>,
    remaining: Option<usize>,
    uri: String,
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
    let (session, file) = open_file(uri, io_client, io_stats, &read_options).await?;
    stream_opened_vortex(
        &display_uri(uri),
        session,
        file,
        read_options,
        convert_options,
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
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    let (arrow_schema, actual_schema) = schemas(uri, &session, file.dtype())?;
    validate_schema(uri, &convert_options.schema, &actual_schema)?;
    let plan = plan_scan(
        &actual_schema,
        file.dtype(),
        &read_options,
        &convert_options,
    )
    .map_err(|error| read_error(uri, "scan planning", error))?;
    let arrow_schema = Arc::new(
        arrow_schema
            .project(&plan.physical_indices)
            .map_err(|error| read_error(uri, "physical Arrow schema", error))?,
    );
    // Even limit=0 validates the full schema, including projected-away fields.
    if convert_options.limit == Some(0) {
        return Ok(futures::stream::empty().boxed());
    }
    let error_uri = uri.to_string();
    let batches = file
        .scan()
        .map_err(|error| read_error(uri, "scan preparation", error))?
        .with_projection(plan.projection)
        .with_some_filter(plan.pushed_filter)
        .with_split_by(SplitBy::RowCount(read_options.batch_size))
        // Vortex interprets this per Vortex worker, not as a global I/O bound.
        .with_concurrency(1)
        .into_record_batch_stream(arrow_schema)
        .map_err(|error| read_error(uri, "Arrow stream preparation", error))?
        .map_err(move |error| read_error(&error_uri, "decode", error))
        .boxed();
    let state = ScanState {
        batches: Some(batches),
        reader: Some((session, file)),
        predicate: plan.residual,
        projection: Some(plan.output_indices),
        remaining: convert_options.limit,
        uri: uri.to_string(),
    };
    Ok(futures::stream::try_unfold(state, |mut state| async move {
        loop {
            let Some(source) = state.batches.as_mut() else {
                return Ok(None);
            };
            let Some(arrow) = source.try_next().await? else {
                return Ok(None);
            };
            let mut batch = to_daft(&arrow)
                .map_err(|error| read_error(&state.uri, "Arrow conversion", error))?;
            if let Some(predicate) = &state.predicate {
                batch = batch
                    .filter(std::slice::from_ref(predicate))
                    .map_err(|error| read_error(&state.uri, "residual filter", error))?;
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
            return Ok(Some((batch, state)));
        }
    })
    .boxed())
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod lifecycle_tests;

#[cfg(test)]
#[path = "pushdown_tests.rs"]
mod pushdown_tests;
