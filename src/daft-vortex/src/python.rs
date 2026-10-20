use std::time::Instant;

use common_io_config::python::IOConfig;
use daft_core::{prelude::Field, python::PySchema};
use daft_dsl::python::PyExpr;
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyBoolMethods, PyDict, PyInt, PyList},
    wrap_pyfunction,
};
use vortex::{VortexSessionDefault, arrow::ArrowSessionExt, session::VortexSession};

use crate::{
    VortexConvertOptions, VortexReadOptions,
    diagnostics::{self, Phase},
    plan::plan_scan,
    read::{MAX_CONCURRENT_METADATA_FILES, NULLABILITY_METADATA_KEY},
};

type PyMetadataBatch<'py> = (Vec<(PySchema, u64)>, Bound<'py, PyDict>);

fn strict_vortex_bool(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(value.cast::<PyBool>()?.is_true())
}

fn strict_coalesce_gap(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    if value.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "Vortex read_coalesce_gap_bytes must be an integer, not bool",
        ));
    }
    let gap = value.cast::<PyInt>()?.extract::<usize>()?;
    if gap > isize::MAX as usize {
        return Err(PyValueError::new_err(
            "Vortex read_coalesce_gap_bytes exceeds the platform limit",
        ));
    }
    Ok(gap)
}

fn strict_vortex_budget(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    if value.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "Vortex request budgets must be positive integers, not bool",
        ));
    }
    let budget = value.cast::<PyInt>()?.extract::<usize>()?;
    if budget == 0 {
        return Err(PyValueError::new_err(
            "Vortex request budgets must be positive integers",
        ));
    }
    Ok(budget)
}

fn strict_metadata_concurrency(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    if value.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "Vortex max_concurrent_files must be an integer, not bool",
        ));
    }
    let concurrency = value.cast::<PyInt>()?.extract::<usize>()?;
    if !(1..=MAX_CONCURRENT_METADATA_FILES).contains(&concurrency) {
        return Err(PyValueError::new_err(format!(
            "Vortex max_concurrent_files must be between 1 and {MAX_CONCURRENT_METADATA_FILES}"
        )));
    }
    Ok(concurrency)
}

#[allow(clippy::too_many_arguments)]
#[pyfunction(signature = (uri, io_config=None, *, max_concurrent_requests=8, max_in_flight_bytes=67108864, enable_read_coalescing=true, read_coalesce_gap_bytes=65536, read_coalesce_max_bytes=4194304))]
fn read_vortex_schema(
    py: Python<'_>,
    uri: &str,
    io_config: Option<IOConfig>,
    #[pyo3(from_py_with = strict_vortex_budget)] max_concurrent_requests: usize,
    #[pyo3(from_py_with = strict_vortex_budget)] max_in_flight_bytes: usize,
    #[pyo3(from_py_with = strict_vortex_bool)] enable_read_coalescing: bool,
    #[pyo3(from_py_with = strict_coalesce_gap)] read_coalesce_gap_bytes: usize,
    #[pyo3(from_py_with = strict_vortex_budget)] read_coalesce_max_bytes: usize,
) -> PyResult<PySchema> {
    metadata_inference(
        py,
        uri,
        io_config,
        VortexReadOptions {
            max_concurrent_requests,
            max_in_flight_bytes,
            enable_read_coalescing,
            read_coalesce_gap_bytes,
            read_coalesce_max_bytes,
            ..Default::default()
        },
        Phase::Schema,
    )
    .map(|(schema, _)| schema)
}

#[allow(clippy::too_many_arguments)]
#[pyfunction(signature = (uri, io_config=None, *, max_concurrent_requests=8, max_in_flight_bytes=67108864, enable_read_coalescing=true, read_coalesce_gap_bytes=65536, read_coalesce_max_bytes=4194304))]
fn read_vortex_metadata(
    py: Python<'_>,
    uri: &str,
    io_config: Option<IOConfig>,
    #[pyo3(from_py_with = strict_vortex_budget)] max_concurrent_requests: usize,
    #[pyo3(from_py_with = strict_vortex_budget)] max_in_flight_bytes: usize,
    #[pyo3(from_py_with = strict_vortex_bool)] enable_read_coalescing: bool,
    #[pyo3(from_py_with = strict_coalesce_gap)] read_coalesce_gap_bytes: usize,
    #[pyo3(from_py_with = strict_vortex_budget)] read_coalesce_max_bytes: usize,
) -> PyResult<(PySchema, u64)> {
    metadata_inference(
        py,
        uri,
        io_config,
        VortexReadOptions {
            max_concurrent_requests,
            max_in_flight_bytes,
            enable_read_coalescing,
            read_coalesce_gap_bytes,
            read_coalesce_max_bytes,
            ..Default::default()
        },
        Phase::PlanningMetadata,
    )
}

fn metadata_inference(
    py: Python<'_>,
    uri: &str,
    io_config: Option<IOConfig>,
    options: VortexReadOptions,
    phase: Phase,
) -> PyResult<(PySchema, u64)> {
    let uri = uri.to_string();
    py.detach(move || {
        crate::io::validate_options(&options)?;
        let client = daft_io::get_io_client(true, io_config.unwrap_or_default().config.into())?;
        let stats = daft_io::IOStatsContext::new("Vortex metadata inference");
        let runtime = common_runtime::get_io_runtime(true);
        let (schema, num_rows) = runtime.block_within_async_context(async move {
            crate::read::read_metadata(&uri, client, stats, options, phase).await
        })??;
        Ok((schema.into(), num_rows))
    })
}

/// Read fresh metadata with bounded file concurrency and report this pass's I/O.
/// Counters come from IOStatsContext, not wire requests or unique file bytes.
#[allow(clippy::too_many_arguments)]
#[pyfunction(signature = (
    uris,
    io_config=None,
    *,
    max_concurrent_files=8,
    max_concurrent_requests=8,
    max_in_flight_bytes=67108864,
    enable_read_coalescing=true,
    read_coalesce_gap_bytes=65536,
    read_coalesce_max_bytes=4194304,
))]
fn read_vortex_metadata_bulk(
    py: Python<'_>,
    uris: Vec<String>,
    io_config: Option<IOConfig>,
    #[pyo3(from_py_with = strict_metadata_concurrency)] max_concurrent_files: usize,
    #[pyo3(from_py_with = strict_vortex_budget)] max_concurrent_requests: usize,
    #[pyo3(from_py_with = strict_vortex_budget)] max_in_flight_bytes: usize,
    #[pyo3(from_py_with = strict_vortex_bool)] enable_read_coalescing: bool,
    #[pyo3(from_py_with = strict_coalesce_gap)] read_coalesce_gap_bytes: usize,
    #[pyo3(from_py_with = strict_vortex_budget)] read_coalesce_max_bytes: usize,
) -> PyResult<PyMetadataBatch<'_>> {
    let num_files = uris.len();
    let (metadata, stats, elapsed_seconds) = py.detach(move || -> PyResult<_> {
        let started = Instant::now();
        let options = VortexReadOptions {
            max_concurrent_requests,
            max_in_flight_bytes,
            enable_read_coalescing,
            read_coalesce_gap_bytes,
            read_coalesce_max_bytes,
            ..Default::default()
        };
        crate::io::validate_options(&options)?;
        let client = daft_io::get_io_client(true, io_config.unwrap_or_default().config.into())?;
        let stats = daft_io::IOStatsContext::new("Vortex bulk metadata inference");
        let bulk_stats = stats.clone();
        let runtime = common_runtime::get_io_runtime(true);
        let metadata = runtime.block_within_async_context(async move {
            crate::read_vortex_metadata_bulk(
                &uris,
                client,
                bulk_stats,
                options,
                max_concurrent_files,
            )
            .await
        })??;
        Ok((metadata, stats, started.elapsed().as_secs_f64()))
    })?;
    let metrics = PyDict::new(py);
    metrics.set_item("num_files", num_files)?;
    metrics.set_item("max_concurrent_files", max_concurrent_files)?;
    metrics.set_item("elapsed_seconds", elapsed_seconds)?;
    metrics.set_item("head_requests", stats.load_head_requests())?;
    metrics.set_item("get_requests", stats.load_get_requests())?;
    metrics.set_item("list_requests", stats.load_list_requests())?;
    metrics.set_item("bytes_read", stats.load_bytes_read())?;
    let metadata = metadata
        .into_iter()
        .map(|(schema, num_rows)| (schema.into(), num_rows))
        .collect();
    Ok((metadata, metrics))
}

/// Inspect the same scan plan used for execution, without opening a file.
/// The caller supplies the complete native schema, including source nullability.
#[pyfunction(signature = (
    schema,
    columns=None,
    predicate=None,
    *,
    enable_projection_pushdown=true,
    enable_filter_pushdown=true,
))]
fn _vortex_scan_diagnostics(
    py: Python<'_>,
    schema: PySchema,
    columns: Option<Vec<String>>,
    predicate: Option<PyExpr>,
    enable_projection_pushdown: bool,
    enable_filter_pushdown: bool,
) -> PyResult<Bound<'_, PyDict>> {
    let arrow_schema = schema.schema.to_arrow()?;
    let fields = arrow_schema
        .fields()
        .iter()
        .zip(schema.schema.fields())
        .map(|(arrow, source)| {
            Ok(arrow
                .as_ref()
                .clone()
                .with_nullable(source_nullability(source)?))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let session = VortexSession::default();
    let dtype = session
        .arrow()
        .from_arrow_schema(&arrow_schema::Schema::new(fields))
        .map_err(|error| PyValueError::new_err(format!("Vortex diagnostic schema: {error}")))?;
    let (_, actual_schema) = crate::read::schemas("diagnostics", &session, &dtype)?;
    crate::read::validate_schema("diagnostics", &schema.schema, &actual_schema)?;
    let read_options = VortexReadOptions {
        enable_projection_pushdown,
        enable_filter_pushdown,
        ..Default::default()
    };
    let convert_options = VortexConvertOptions {
        schema: schema.schema,
        predicate: predicate.map(|predicate| predicate.expr),
        include_columns: columns,
        limit: None,
    };
    let plan = plan_scan(&actual_schema, &dtype, &read_options, &convert_options)?;
    let result = PyDict::new(py);
    result.set_item("output_columns", plan.output_columns)?;
    result.set_item("physical_columns", plan.physical_columns)?;
    result.set_item(
        "pushed_filter",
        plan.pushed_filter.as_ref().map(ToString::to_string),
    )?;
    result.set_item(
        "residual_filter",
        plan.residual.as_ref().map(ToString::to_string),
    )?;
    result.set_item("filter_fallbacks", plan.filter_fallbacks)?;
    result.set_item("enable_projection_pushdown", enable_projection_pushdown)?;
    result.set_item("enable_filter_pushdown", enable_filter_pushdown)?;
    Ok(result)
}

fn source_nullability(field: &Field) -> PyResult<bool> {
    match field
        .metadata
        .get(NULLABILITY_METADATA_KEY)
        .map(String::as_str)
    {
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        _ => Err(PyValueError::new_err(format!(
            "Vortex diagnostics need source nullability for {:?}; use read_vortex_schema",
            field.name,
        ))),
    }
}

#[pyfunction]
fn _vortex_diagnostics_start() -> PyResult<()> {
    diagnostics::start()?;
    Ok(())
}

#[pyfunction]
fn _vortex_diagnostics_finish(py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
    let snapshot = diagnostics::finish()?;
    let result = PyDict::new(py);
    result.set_item("schema_version", 1)?;
    result.set_item("scope", "process_local_logical_operations")?;
    result.set_item("scope_note", "Logical API operations; SDK/transport retries are not separate events. batch_wait_decode includes I/O wait. Timings are wall time and overlapping spans must not be summed.")?;
    result.set_item("max_events", diagnostics::MAX_EVENTS)?;
    result.set_item("dropped_events", snapshot.dropped_events)?;
    result.set_item("unfinished_events", snapshot.unfinished_events)?;
    result.set_item("elapsed_ns", snapshot.elapsed_ns)?;
    result.set_item("file_count", snapshot.file_count)?;
    let events = PyList::empty(py);
    for event in snapshot.events {
        let record = PyDict::new(py);
        record.set_item("file_id", event.file_id)?;
        record.set_item("phase", event.phase.name())?;
        record.set_item("operation", event.operation)?;
        record.set_item("started_ns", event.started_ns)?;
        record.set_item("duration_ns", event.duration_ns)?;
        record.set_item("offset", event.offset)?;
        record.set_item("requested_bytes", event.requested_bytes)?;
        record.set_item("returned_bytes", event.returned_bytes)?;
        record.set_item("request_wait_ns", event.request_wait_ns)?;
        record.set_item("byte_wait_ns", event.byte_wait_ns)?;
        record.set_item("header_ns", event.header_ns)?;
        record.set_item("body_ns", event.body_ns)?;
        record.set_item("error", event.error)?;
        record.set_item("cancelled", event.cancelled)?;
        events.append(record)?;
    }
    result.set_item("events", events)?;
    Ok(result)
}

pub fn register_python(parent: &Bound<'_, PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(read_vortex_schema, parent)?)?;
    parent.add_function(wrap_pyfunction!(read_vortex_metadata, parent)?)?;
    parent.add_function(wrap_pyfunction!(read_vortex_metadata_bulk, parent)?)?;
    parent.add_function(wrap_pyfunction!(_vortex_scan_diagnostics, parent)?)?;
    parent.add_function(wrap_pyfunction!(_vortex_diagnostics_start, parent)?)?;
    parent.add_function(wrap_pyfunction!(_vortex_diagnostics_finish, parent)?)?;
    Ok(())
}
