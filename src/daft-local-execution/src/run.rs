#[cfg(any(feature = "python", test))]
use std::sync::atomic::AtomicBool;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use common_daft_config::DaftExecutionConfig;
use common_display::{DisplayLevel, mermaid::MermaidDisplayOptions};
use common_error::DaftResult;
use common_metrics::{QueryEndState, QueryID};
use common_runtime::{
    RuntimeTask,
    query_profile::{self, Phase},
};
use common_tracing::flush_opentelemetry_providers;
use daft_context::{
    DaftContext, Subscriber,
    subscribers::{
        Event, event_header,
        events::{TaskInfo, TaskStartEvent},
    },
};
use daft_local_plan::{ExecutionStats, Input, InputId, LocalPhysicalPlanRef, SourceId, translate};
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::MicroPartition;
use daft_partition_refs::FlightPartitionRef;
use daft_shuffles::server::flight_server::{
    FlightServerConnectionHandle, ShuffleFlightServer, start_server_loop,
};
use futures::{FutureExt, future::BoxFuture};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "python")]
use {
    common_daft_config::PyDaftExecutionConfig,
    daft_context::python::PyDaftContext,
    daft_local_plan::python::PyExecutionStats,
    daft_logical_plan::PyLogicalPlanBuilder,
    daft_micropartition::python::PyMicroPartition,
    daft_partition_refs::PyFlightPartitionRef,
    pyo3::{
        Bound, IntoPyObject, PyAny, PyRef, PyResult, Python,
        exceptions::{PyTypeError, PyValueError},
        pyclass, pymethods,
        sync::MutexExt,
        types::{PyAnyMethods, PyBool, PyInt},
    },
};

#[cfg(feature = "python")]
fn extract_results_buffer_size(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    if value.is_none() {
        return Ok(None);
    }
    if value.is_instance_of::<PyBool>() || value.cast::<PyInt>().is_err() {
        return Err(PyTypeError::new_err(
            "results_buffer_size must be a positive integer or None",
        ));
    }
    let size = value.extract::<usize>().map_err(|_| {
        PyValueError::new_err("results_buffer_size must be a positive integer or None")
    })?;
    if size == 0 || size > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(PyValueError::new_err(format!(
            "results_buffer_size must be between 1 and {} or None",
            tokio::sync::Semaphore::MAX_PERMITS,
        )));
    }
    Ok(Some(size))
}

use crate::{
    ExecutionRuntimeContext,
    channel::{Sender, UnboundedSender, create_channel, create_unbounded_channel},
    pipeline::{
        BuilderContext, PipelineMessage, translate_physical_plan_to_pipeline, viz_pipeline_ascii,
        viz_pipeline_mermaid,
    },
    resource_manager::get_or_init_memory_manager,
    runtime_stats::{RuntimeStatsManager, RuntimeStatsManagerHandle},
};

enum ExecutionEngineResultItem {
    Partition(MicroPartition),
    FlightPartitionRef(FlightPartitionRef),
}

#[derive(Clone)]
enum ResultSender {
    Bounded(Sender<ExecutionEngineResultItem>),
    Unbounded(UnboundedSender<ExecutionEngineResultItem>),
}

impl ResultSender {
    async fn send(&self, item: ExecutionEngineResultItem) {
        match self {
            Self::Bounded(sender) => {
                let _ = sender.send(item).await;
            }
            Self::Unbounded(sender) => {
                let _ = sender.send(item);
            }
        }
    }
}

enum ResultReceiver {
    Bounded(crate::channel::Receiver<ExecutionEngineResultItem>),
    Unbounded(crate::channel::UnboundedReceiver<ExecutionEngineResultItem>),
}

impl ResultReceiver {
    async fn recv(&mut self) -> Option<ExecutionEngineResultItem> {
        match self {
            Self::Bounded(receiver) => receiver.recv().await,
            Self::Unbounded(receiver) => receiver.recv().await,
        }
    }
}

#[derive(Clone)]
struct ResultOutput {
    sender: ResultSender,
    cancel: CancellationToken,
}

impl ResultOutput {
    async fn send(self, item: ExecutionEngineResultItem) {
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => {}
            () = self.sender.send(item) => {}
        }
    }
}

fn create_result_channel(capacity: Option<usize>) -> (ResultOutput, ExecutionEngineResult) {
    let (sender, receiver) = match capacity {
        Some(capacity) => {
            let (sender, receiver) = create_channel(capacity);
            (
                ResultSender::Bounded(sender),
                ResultReceiver::Bounded(receiver),
            )
        }
        None => {
            let (sender, receiver) = create_unbounded_channel();
            (
                ResultSender::Unbounded(sender),
                ResultReceiver::Unbounded(receiver),
            )
        }
    };
    let cancel = CancellationToken::new();
    (
        ResultOutput {
            sender,
            cancel: cancel.clone(),
        },
        ExecutionEngineResult {
            receiver,
            cancel,
            exhausted: false,
        },
    )
}

fn result_buffer_capacity(
    is_flotilla_worker: bool,
    requested: Option<usize>,
) -> DaftResult<Option<usize>> {
    if requested.is_some_and(|size| size == 0 || size > tokio::sync::Semaphore::MAX_PERMITS) {
        return Err(common_error::DaftError::ValueError(format!(
            "results_buffer_size must be between 1 and {} or None",
            tokio::sync::Semaphore::MAX_PERMITS,
        )));
    }
    // Local callers always have a finite result queue. Shared Flotilla workers
    // keep their existing transport unless a caller explicitly opts in.
    Ok(requested.or_else(|| (!is_flotilla_worker).then_some(1)))
}

/// Global tokio runtime shared by all NativeExecutor instances
static GLOBAL_RUNTIME: OnceLock<Handle> = OnceLock::new();

/// Get or initialize the global tokio runtime
#[cfg(feature = "python")]
fn get_global_runtime() -> &'static Handle {
    GLOBAL_RUNTIME.get_or_init(|| {
        let mut builder = tokio::runtime::Builder::new_current_thread();
        builder.enable_all();
        pyo3_async_runtimes::tokio::init(builder);
        std::thread::spawn(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(futures::future::pending::<()>());
        });
        pyo3_async_runtimes::tokio::get_runtime().handle().clone()
    })
}

#[cfg(not(feature = "python"))]
fn get_global_runtime() -> &'static Handle {
    GLOBAL_RUNTIME.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build global tokio runtime for NativeExecutor");
        let handle = rt.handle().clone();
        // Keep the runtime alive for the duration of the process.
        std::thread::spawn(move || {
            rt.block_on(futures::future::pending::<()>());
        });
        handle
    })
}

/// Message sent to the execution task to enqueue inputs
pub(crate) struct EnqueueInputMessage {
    /// The input_id for this enqueue operation
    input_id: InputId,
    /// Plan inputs grouped by source_id
    inputs: HashMap<SourceId, Input>,
    /// Sender for results of this input_id
    result_sender: ResultOutput,
}

/// Routes pipeline messages to per-input_id channels.
struct MessageRouter {
    output_senders: HashMap<InputId, ResultOutput>,
    /// Wall-clock start instant when each `input_id` was enqueued to the pipeline.
    input_start_times: HashMap<InputId, Instant>,
}

impl MessageRouter {
    fn new() -> Self {
        Self {
            output_senders: HashMap::new(),
            input_start_times: HashMap::new(),
        }
    }

    /// Route a message to the appropriate channel based on its input_id.
    fn route_message(&mut self, msg: PipelineMessage) -> Option<BoxFuture<'static, ()>> {
        match msg {
            PipelineMessage::Flush(input_id) => {
                self.input_start_times.remove(&input_id);
                self.output_senders.remove(&input_id);
                None
            }
            PipelineMessage::Morsel {
                input_id,
                partition,
            } => self.output_senders.get(&input_id).cloned().map(|sender| {
                sender
                    .send(ExecutionEngineResultItem::Partition(partition))
                    .boxed()
            }),
            PipelineMessage::FlightPartitionRef {
                input_id,
                partition_ref,
            } => self.output_senders.get(&input_id).cloned().map(|sender| {
                sender
                    .send(ExecutionEngineResultItem::FlightPartitionRef(partition_ref))
                    .boxed()
            }),
        }
    }

    fn insert_output_sender(&mut self, input_id: InputId, sender: ResultOutput) {
        self.input_start_times.insert(input_id, Instant::now());
        self.output_senders.insert(input_id, sender);
    }

    fn has_unfinished_consumers(&self) -> bool {
        self.output_senders
            .values()
            .any(|output| !output.cancel.is_cancelled())
    }
}

impl Drop for MessageRouter {
    fn drop(&mut self) {
        for (input_id, started) in self.input_start_times.drain() {
            log::debug!(
                "NativeExecutor: input_id={input_id} ended without Flush after {:?} (cancel/shutdown?)",
                started.elapsed()
            );
        }
    }
}

/// Per-plan execution state
struct PlanState {
    task_handle: RuntimeTask<DaftResult<()>>,
    cancel: CancellationToken,
    enqueue_input_sender: Sender<EnqueueInputMessage>,
    stats_handle: RuntimeStatsManagerHandle,
    active_input_ids: HashSet<InputId>,
    // A canceled input can still have work/Flush messages in the shared
    // pipeline. Its id must not be assigned to a new consumer in this plan.
    abandoned_input_ids: HashSet<InputId>,
    skipped_corrupt_files: Arc<std::sync::Mutex<Vec<(String, String, bool)>>>,
}

#[cfg_attr(
    feature = "python",
    pyclass(module = "daft.daft", name = "NativeExecutor", frozen)
)]
pub struct PyNativeExecutor {
    executor: Arc<Mutex<NativeExecutor>>,
    address: Option<String>,
}

#[cfg(feature = "python")]
impl Default for PyNativeExecutor {
    fn default() -> Self {
        Self::new(false, "")
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl PyNativeExecutor {
    #[new]
    pub fn new(is_flotilla_worker: bool, ip: &str) -> Self {
        let executor = NativeExecutor::new(is_flotilla_worker, ip);
        let address = executor.shuffle_address();
        Self {
            executor: Arc::new(Mutex::new(executor)),
            address,
        }
    }

    pub fn shuffle_address(&self) -> Option<String> {
        self.address.clone()
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (local_physical_plan, daft_ctx, input_id, inputs, context=None, maintain_order=true, *, results_buffer_size=None))]
    pub fn run<'py>(
        &self,
        py: Python<'py>,
        local_physical_plan: &daft_local_plan::PyLocalPhysicalPlan,
        daft_ctx: &PyDaftContext,
        input_id: InputId,
        inputs: HashMap<SourceId, Input>,
        context: Option<HashMap<String, String>>,
        maintain_order: bool,
        #[pyo3(from_py_with = extract_results_buffer_size)] results_buffer_size: Option<usize>,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let daft_ctx: &DaftContext = daft_ctx.into();
        let plan = local_physical_plan.plan.clone();
        let exec_cfg = daft_ctx.execution_config();
        let subscribers = daft_ctx.subscribers();
        let (fingerprint, enqueue_future) = {
            self.executor
                .lock_py_attached(py)
                .unwrap()
                .run_with_result_buffer(
                    &plan,
                    exec_cfg,
                    subscribers,
                    context,
                    inputs,
                    input_id,
                    maintain_order,
                    results_buffer_size,
                )?
        };

        let executor = self.executor.clone();
        let registration = InputRegistration {
            executor,
            fingerprint,
            input_id,
            active: AtomicBool::new(true),
        };
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = enqueue_future.await?;
            Ok(PyResultReceiver {
                handle: Arc::new(ResultHandle::new(result, registration)),
            })
        })
    }

    pub fn active_plan_count(&self, py: Python<'_>) -> usize {
        self.executor.lock_py_attached(py).unwrap().plans.len()
    }

    pub fn cancel_plan(&self, py: Python<'_>, fingerprint: u64) -> PyResult<()> {
        self.executor
            .lock_py_attached(py)
            .unwrap()
            .cancel_plan(fingerprint);
        Ok(())
    }

    #[staticmethod]
    pub fn repr_ascii(
        logical_plan_builder: &PyLogicalPlanBuilder,
        cfg: PyDaftExecutionConfig,
        simple: bool,
    ) -> PyResult<String> {
        Ok(NativeExecutor::repr_ascii(
            &logical_plan_builder.builder,
            cfg.config,
            simple,
        ))
    }

    #[staticmethod]
    pub fn repr_mermaid(
        logical_plan_builder: &PyLogicalPlanBuilder,
        cfg: PyDaftExecutionConfig,
        options: MermaidDisplayOptions,
    ) -> PyResult<String> {
        Ok(NativeExecutor::repr_mermaid(
            &logical_plan_builder.builder,
            cfg.config,
            options,
        ))
    }
}

/// Returns a fingerprint that is unique for each call when the caller does not
/// supply one. Using a fixed value (e.g. 0) caused `NativeExecutor::run` to
/// reuse the cached pipeline from a prior execution when the new plan requires
/// a different `InputSender` variant, which reached the `unreachable!` branch
/// in `InputSender::send` (see GitHub issue #7087).
fn next_auto_fingerprint() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn parse_context(ctx: Option<&HashMap<String, String>>) -> (QueryID, u64, Option<u32>) {
    let query_id = ctx
        .as_ref()
        .and_then(|c| c.get("query_id"))
        .map(|s| QueryID::from(s.as_str()))
        .unwrap_or_else(|| QueryID::from(""));
    let fingerprint = ctx
        .as_ref()
        .and_then(|c| c.get("plan_fingerprint"))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(next_auto_fingerprint);
    let task_id = ctx
        .as_ref()
        .and_then(|c| c.get("task_id"))
        .and_then(|s| s.parse::<u32>().ok());

    (query_id, fingerprint, task_id)
}

// TODO: fix configuration for events
// This is copied from task_lifecycle.rs to avoid the daft-distributed dependency
pub fn task_events_enabled() -> bool {
    if let Ok(val) = std::env::var("DAFT_TASK_EVENTS_ENABLED") {
        matches!(val.trim().to_lowercase().as_str(), "1" | "true")
    } else {
        false // Disabled by default; enable with DAFT_TASK_EVENTS_ENABLED=true
    }
}

/// The core execution loop that drives a pipeline to completion.
/// Receives inputs via `enqueue_input_rx`, routes pipeline outputs to
/// per-input_id channels, and runs until the pipeline finishes, errors,
/// or is cancelled.
async fn run_execution_loop(
    cancel: CancellationToken,
    stats_manager: RuntimeStatsManager,
    mut enqueue_input_rx: crate::channel::Receiver<EnqueueInputMessage>,
    input_senders: Arc<HashMap<SourceId, crate::input_sender::InputSender>>,
    pipeline: Box<dyn crate::pipeline::PipelineNode>,
    maintain_order: bool,
) -> DaftResult<()> {
    let stats_manager_handle = stats_manager.handle();
    let memory_manager = get_or_init_memory_manager();
    let mut runtime_handle =
        ExecutionRuntimeContext::new(memory_manager.clone(), stats_manager_handle);
    let trace = query_profile::begin(Phase::Engine, "pipeline_start", None);
    let output = pipeline.start(maintain_order, &mut runtime_handle);
    query_profile::complete(trace, output.is_err());
    let mut output_receiver = output?;

    let mut message_router = MessageRouter::new();
    let mut input_senders = Some(input_senders);
    let mut input_exhausted = false;
    // Keep at most one result outside the bounded receiver. While a send is
    // blocked, still observe cancellation, input registration, and task errors.
    let mut pending_output: Option<BoxFuture<'static, ()>> = None;
    let mut drain_workers = false;

    let (result, finish_status) = loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                log::debug!("Execution engine cancelled");
                break (Ok(()), QueryEndState::Canceled);
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Received Ctrl-C, shutting down execution engine");
                break (Ok(()), QueryEndState::Canceled);
            }
            Some(join_result) = runtime_handle.join_next() => {
                if let Err(e) = join_result {
                    if matches!(&e, common_error::DaftError::JoinError(source) if source.is_cancelled()) {
                        break (Ok(()), QueryEndState::Canceled);
                    }
                    break (Err(e), QueryEndState::Failed);
                }
            }
            enqueue_msg = enqueue_input_rx.recv(), if !input_exhausted => {
                if let Some(EnqueueInputMessage { input_id, inputs, result_sender }) = enqueue_msg {
                    message_router.insert_output_sender(input_id, result_sender);
                    let senders = input_senders.as_ref().unwrap();
                    for (key, plan_input) in inputs {
                        if let Some(sender) = senders.get(&key) {
                            let _ = sender.send(input_id, plan_input);
                        }
                    }
                } else {
                    // All senders dropped — drop input channels so
                    // pipeline sources see EOF.
                    input_senders.take();
                    input_exhausted = true;
                }
            }
            () = async { pending_output.as_mut().unwrap().await }, if pending_output.is_some() => {
                pending_output = None;
            }
            msg = output_receiver.recv(), if pending_output.is_none() => {
                match msg {
                    Some(msg) => {
                        pending_output = message_router.route_message(msg);
                    }
                    None => {
                        // A channel can close before a nested worker reports its
                        // error. Without a successful Flush, do not abort that
                        // worker and turn the failure into successful EOF.
                        drain_workers = message_router.has_unfinished_consumers();
                        break (Ok(()), QueryEndState::Finished);
                    }
                }
            }
        }
    };

    drop(pending_output);
    // No more inputs can run once the root output closes. Mark the pipeline
    // dead before waking consumers, including while delayed errors drain.
    drop(enqueue_input_rx);
    drop(message_router);
    drop(output_receiver);
    drop(input_senders);
    let trace = query_profile::begin(Phase::Engine, "worker_shutdown", None);
    let (result, finish_status) = finish_runtime_tasks(
        &mut runtime_handle,
        &cancel,
        drain_workers,
        result,
        finish_status,
    )
    .await;
    query_profile::complete(trace, result.is_err());
    let trace = query_profile::begin(Phase::Engine, "stats_finalize", None);
    stats_manager.finish(finish_status).await;
    flush_opentelemetry_providers();
    query_profile::complete(trace, false);
    result
}

async fn finish_runtime_tasks(
    runtime_handle: &mut ExecutionRuntimeContext,
    cancel: &CancellationToken,
    drain_workers: bool,
    mut result: DaftResult<()>,
    mut finish_status: QueryEndState,
) -> (DaftResult<()>, QueryEndState) {
    if drain_workers {
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    finish_status = QueryEndState::Canceled;
                    break;
                }
                _ = tokio::signal::ctrl_c() => {
                    finish_status = QueryEndState::Canceled;
                    break;
                }
                joined = runtime_handle.join_next() => {
                    match joined {
                        None => break,
                        Some(Ok(())) => {},
                        Some(Err(error)) => {
                            result = Err(error);
                            finish_status = QueryEndState::Failed;
                            break;
                        }
                    }
                }
            }
        }
    }
    // Explicit close and genuine Flush/limit completion still abort pending
    // work; they must not wait for blocked or unneeded upstream input.
    let shutdown_result = runtime_handle.shutdown().await;
    if result.is_ok()
        && matches!(finish_status, QueryEndState::Finished)
        && let Err(error) = shutdown_result
    {
        result = Err(error);
        finish_status = QueryEndState::Failed;
    }
    (result, finish_status)
}

pub struct NativeExecutor {
    cancel: CancellationToken,
    is_flotilla_worker: bool,
    shuffle_server: Option<Arc<ShuffleFlightServer>>,
    shuffle_server_connection: Option<FlightServerConnectionHandle>,
    plans: HashMap<u64, PlanState>,
}

impl NativeExecutor {
    pub fn new(is_flotilla_worker: bool, ip: &str) -> Self {
        // Determine if we are running in a flotilla worker.
        if is_flotilla_worker {
            let shuffle_server = Arc::new(ShuffleFlightServer::new());
            let shuffle_server_connection = Some(start_server_loop(ip, shuffle_server.clone()));

            Self {
                cancel: CancellationToken::new(),
                is_flotilla_worker: true,
                shuffle_server: Some(shuffle_server),
                shuffle_server_connection,
                plans: HashMap::new(),
            }
        } else {
            Self {
                cancel: CancellationToken::new(),
                is_flotilla_worker: false,
                shuffle_server: None,
                shuffle_server_connection: None,
                plans: HashMap::new(),
            }
        }
    }

    pub fn shuffle_address(&self) -> Option<String> {
        self.shuffle_server_connection
            .as_ref()
            .map(|conn| conn.shuffle_address())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run(
        &mut self,
        local_physical_plan: &LocalPhysicalPlanRef,
        exec_cfg: Arc<DaftExecutionConfig>,
        subscribers: Vec<Arc<dyn Subscriber>>,
        additional_context: Option<HashMap<String, String>>,
        inputs: HashMap<SourceId, Input>,
        input_id: InputId,
        maintain_order: bool,
    ) -> DaftResult<(u64, BoxFuture<'static, DaftResult<ExecutionEngineResult>>)> {
        self.run_with_result_buffer(
            local_physical_plan,
            exec_cfg,
            subscribers,
            additional_context,
            inputs,
            input_id,
            maintain_order,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn run_with_result_buffer(
        &mut self,
        local_physical_plan: &LocalPhysicalPlanRef,
        exec_cfg: Arc<DaftExecutionConfig>,
        subscribers: Vec<Arc<dyn Subscriber>>,
        additional_context: Option<HashMap<String, String>>,
        inputs: HashMap<SourceId, Input>,
        input_id: InputId,
        maintain_order: bool,
        results_buffer_size: Option<usize>,
    ) -> DaftResult<(u64, BoxFuture<'static, DaftResult<ExecutionEngineResult>>)> {
        let capacity = result_buffer_capacity(self.is_flotilla_worker, results_buffer_size)?;
        let (query_id, fingerprint, task_id) = parse_context(additional_context.as_ref());

        if self.is_flotilla_worker {
            debug_assert_eq!(
                task_id,
                Some(input_id),
                "Flotilla invariant violated: task_id must match input_id"
            );
        }

        let task_start_dispatch = if self.is_flotilla_worker
            && task_events_enabled()
            && let Some(task_id) = task_id
        {
            Some((
                Event::TaskStart(TaskStartEvent {
                    header: event_header(query_id.clone()),
                    task: Arc::new(TaskInfo {
                        id: task_id,
                        last_node_id: 0,  // TODO: propagate last_node_id
                        node_ids: vec![], // TODO: propagate node_ids
                        plan_fingerprint: fingerprint as u32,
                        name: None,
                    }),
                    worker_id: None, // TODO: propagate worker id
                }),
                subscribers.clone(),
            ))
        } else {
            None
        };

        if !self.plans.contains_key(&fingerprint) {
            let cancel = self.cancel.child_token();
            let additional_context = additional_context.unwrap_or_default();
            let shuffle_address = self.shuffle_address();
            let ctx = BuilderContext::new_with_context(
                query_id.clone(),
                additional_context,
                self.shuffle_server
                    .as_ref()
                    .map(|server| (server.clone(), shuffle_address.unwrap())),
            );
            let trace = query_profile::begin(Phase::Engine, "pipeline_translation", None);
            let translated =
                translate_physical_plan_to_pipeline(local_physical_plan, &exec_cfg, &ctx);
            query_profile::complete(trace, translated.is_err());
            let (pipeline, input_senders) = translated?;

            let handle = get_global_runtime();
            let stats_manager = RuntimeStatsManager::try_new(
                handle,
                &pipeline,
                subscribers,
                query_id,
                self.is_flotilla_worker,
            )?;
            let stats_handle = stats_manager.handle();

            let (enqueue_input_tx, enqueue_input_rx) = create_channel::<EnqueueInputMessage>(1);

            let input_senders = Arc::new(input_senders);
            let task = run_execution_loop(
                cancel.clone(),
                stats_manager,
                enqueue_input_rx,
                input_senders,
                pipeline,
                maintain_order,
            );

            let task_handle = RuntimeTask::new(handle, task);
            self.plans.insert(
                fingerprint,
                PlanState {
                    task_handle,
                    cancel,
                    enqueue_input_sender: enqueue_input_tx,
                    stats_handle,
                    active_input_ids: HashSet::new(),
                    abandoned_input_ids: HashSet::new(),
                    skipped_corrupt_files: ctx.skipped_corrupt_files.clone(),
                },
            );
        }

        let plan_state = self.plans.get_mut(&fingerprint).unwrap();
        let enqueue_input_sender = plan_state.enqueue_input_sender.clone();
        if plan_state.abandoned_input_ids.contains(&input_id) {
            return Err(common_error::DaftError::ValueError(format!(
                "input_id {input_id} was abandoned and cannot be reused while plan {fingerprint} remains active"
            )));
        }
        if !plan_state.active_input_ids.insert(input_id) {
            return Err(common_error::DaftError::ValueError(format!(
                "input_id {input_id} is already active for plan {fingerprint}"
            )));
        }

        Ok((
            fingerprint,
            async move {
                let (result_tx, result) = create_result_channel(capacity);
                let enqueue_msg = EnqueueInputMessage {
                    input_id,
                    inputs,
                    result_sender: result_tx,
                };
                if enqueue_input_sender.send(enqueue_msg).await.is_err() {
                    return Err(common_error::DaftError::InternalError(
                        "Plan execution task has died; cannot enqueue new input".to_string(),
                    ));
                }

                // Send the event after the task has been enqueued for execution
                if let Some((event, subscribers)) = task_start_dispatch {
                    dispatch_task_start_event(&subscribers, &event);
                }

                Ok(result)
            }
            .boxed(),
        ))
    }

    /// Finish tracking an input_id. If no active input_ids remain (or the
    /// enqueue channel is closed), removes the plan and awaits the exec task.
    pub fn try_finish(
        &mut self,
        fingerprint: u64,
        input_id: InputId,
    ) -> DaftResult<BoxFuture<'static, DaftResult<ExecutionStats>>> {
        let Some(plan_state) = self.plans.get_mut(&fingerprint) else {
            // Plan already removed (pipeline died and another input_id cleaned it up).
            // Return empty stats; the actual error was already surfaced by the first caller.
            let query_id = QueryID::from("");
            return Ok(async move { Ok(ExecutionStats::new(query_id, vec![])) }.boxed());
        };

        plan_state.active_input_ids.remove(&input_id);
        let pipeline_dead = plan_state.enqueue_input_sender.is_closed();
        let should_remove = plan_state.active_input_ids.is_empty() || pipeline_dead;

        if should_remove {
            let plan_state = self.plans.remove(&fingerprint).unwrap();
            Ok(async move {
                // Try to get stats for this input_id. If the pipeline already died,
                // the stats manager may be finished so this can fail — that's OK.
                let stats = plan_state.stats_handle.take_input_snapshot(input_id).await;
                drop(plan_state.enqueue_input_sender);
                plan_state.task_handle.await??;
                let skipped = plan_state
                    .skipped_corrupt_files
                    .lock()
                    .map(|v| v.clone())
                    .unwrap_or_default();
                // If the snapshot failed (e.g. pipeline died), return empty stats.
                Ok(stats
                    .unwrap_or_else(|_| ExecutionStats::new(QueryID::from(""), vec![]))
                    .with_skipped_corrupt_files(skipped))
            }
            .boxed())
        } else {
            let stats_handle = plan_state.stats_handle.clone();
            let skipped_corrupt_files = plan_state.skipped_corrupt_files.clone();
            Ok(async move {
                let skipped = skipped_corrupt_files
                    .lock()
                    .map(|v| v.clone())
                    .unwrap_or_default();
                Ok(stats_handle
                    .take_input_snapshot(input_id)
                    .await
                    .unwrap_or_else(|_| ExecutionStats::new(QueryID::from(""), vec![]))
                    .with_skipped_corrupt_files(skipped))
            }
            .boxed())
        }
    }

    pub fn cancel_plan(&mut self, fingerprint: u64) {
        // RuntimeTask drop cancels the spawned task
        self.plans.remove(&fingerprint);
    }

    /// Abandon one result consumer. Other inputs sharing the pipeline remain
    /// active; only the last consumer can cancel the entire plan.
    #[cfg(any(feature = "python", test))]
    fn cancel_input(&mut self, fingerprint: u64, input_id: InputId) -> BoxFuture<'static, ()> {
        let Some(plan) = self.plans.get_mut(&fingerprint) else {
            return async {}.boxed();
        };
        if !plan.active_input_ids.remove(&input_id) {
            return async {}.boxed();
        }
        if !plan.active_input_ids.is_empty() {
            plan.abandoned_input_ids.insert(input_id);
            return async {}.boxed();
        }
        let plan = self.plans.remove(&fingerprint).unwrap();
        plan.cancel.cancel();
        drop(plan.enqueue_input_sender);
        async move {
            // Deliberate cancellation must not replace the consumer's error.
            // Normal EOF uses try_finish and still propagates execution errors.
            let _ = plan.task_handle.await;
        }
        .boxed()
    }

    fn repr_ascii(
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
        simple: bool,
    ) -> String {
        let logical_plan = logical_plan_builder.build();
        let (physical_plan, _) = translate(&logical_plan, &HashMap::new()).unwrap();
        let ctx = BuilderContext::new();
        let (pipeline_node, _) =
            translate_physical_plan_to_pipeline(&physical_plan, &cfg, &ctx).unwrap();

        viz_pipeline_ascii(pipeline_node.as_ref(), simple)
    }

    fn repr_mermaid(
        logical_plan_builder: &LogicalPlanBuilder,
        cfg: Arc<DaftExecutionConfig>,
        options: MermaidDisplayOptions,
    ) -> String {
        let logical_plan = logical_plan_builder.build();
        let (physical_plan, _) = translate(&logical_plan, &HashMap::new()).unwrap();
        let ctx = BuilderContext::new();
        let (pipeline_node, _) =
            translate_physical_plan_to_pipeline(&physical_plan, &cfg, &ctx).unwrap();

        let display_type = if options.simple {
            DisplayLevel::Compact
        } else {
            DisplayLevel::Default
        };
        viz_pipeline_mermaid(
            pipeline_node.as_ref(),
            display_type,
            options.bottom_up,
            options.subgraph_options,
        )
    }
}

impl Drop for NativeExecutor {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(conn) = &mut self.shuffle_server_connection {
            let _ = conn.shutdown();
        }
    }
}

pub struct ExecutionEngineResult {
    receiver: ResultReceiver,
    cancel: CancellationToken,
    exhausted: bool,
}

impl ExecutionEngineResult {
    async fn next(&mut self) -> Option<ExecutionEngineResultItem> {
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => None,
            item = self.receiver.recv() => {
                self.exhausted = item.is_none();
                item
            }
        }
    }

    /// Consume all pipeline output for this input_id until EOF, returning any
    /// emitted `MicroPartition`s. `FlightPartitionRef` items are skipped (they
    /// are only relevant when shuffles are enabled). Intended for tests that
    /// exercise `NativeExecutor` end-to-end and need the pipeline to finish
    /// producing output before `try_finish` is called — mirroring what the
    /// production Python `__anext__` loop does.
    pub async fn collect_partitions_for_testing(mut self) -> Vec<MicroPartition> {
        let mut out = Vec::new();
        while let Some(item) = self.next().await {
            if let ExecutionEngineResultItem::Partition(p) = item {
                out.push(p);
            }
        }
        out
    }
}

impl Drop for ExecutionEngineResult {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Also owns registration while the asynchronous enqueue is pending. Dropping
/// that future must not leave a plan/input in the executor registry.
#[cfg(any(feature = "python", test))]
struct InputRegistration {
    executor: Arc<Mutex<NativeExecutor>>,
    fingerprint: u64,
    input_id: InputId,
    active: AtomicBool,
}

#[cfg(any(feature = "python", test))]
impl Drop for InputRegistration {
    fn drop(&mut self) {
        if self.active.swap(false, Ordering::AcqRel)
            && let Ok(mut executor) = self.executor.lock()
        {
            let cleanup = executor.cancel_input(self.fingerprint, self.input_id);
            get_global_runtime().spawn(cleanup);
        }
    }
}

#[cfg(any(feature = "python", test))]
struct ResultHandle {
    result: tokio::sync::Mutex<Option<ExecutionEngineResult>>,
    cancel: CancellationToken,
    registration: InputRegistration,
}

#[cfg(any(feature = "python", test))]
impl ResultHandle {
    fn new(result: ExecutionEngineResult, registration: InputRegistration) -> Self {
        Self {
            cancel: result.cancel.clone(),
            result: tokio::sync::Mutex::new(Some(result)),
            registration,
        }
    }

    async fn next(&self) -> Option<ExecutionEngineResultItem> {
        let mut result = self.result.lock().await;
        match result.as_mut() {
            Some(result) => result.next().await,
            None => None,
        }
    }

    async fn finish(&self, abandon: bool) -> DaftResult<ExecutionStats> {
        // Both finish and close stop consumption. Wake any pending next before
        // acquiring its mutex; the recorded EOF distinguishes normal finish.
        self.cancel.cancel();
        let mut result_guard = self.result.lock().await;
        let Some(result) = result_guard.take() else {
            return Ok(ExecutionStats::new(QueryID::from(""), vec![]));
        };
        let exhausted = result.exhausted;
        drop(result);
        self.registration.active.store(false, Ordering::Release);
        let InputRegistration {
            executor,
            fingerprint,
            input_id,
            ..
        } = &self.registration;
        if abandon || !exhausted {
            let cleanup = executor
                .lock()
                .unwrap()
                .cancel_input(*fingerprint, *input_id);
            cleanup.await;
            Ok(ExecutionStats::new(QueryID::from(""), vec![]))
        } else {
            let finish = executor
                .lock()
                .unwrap()
                .try_finish(*fingerprint, *input_id)?;
            finish.await
        }
    }
}

#[cfg(feature = "python")]
#[pyclass(module = "daft.daft", name = "PyResultReceiver", frozen)]
pub struct PyResultReceiver {
    handle: Arc<ResultHandle>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyResultReceiver {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, pyo3::PyAny>> {
        let handle = self.handle.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let part = handle.next().await;
            Python::attach(|py| {
                Ok(match part {
                    None => py.None(),
                    Some(ExecutionEngineResultItem::Partition(partition)) => {
                        PyMicroPartition::from(partition)
                            .into_pyobject(py)?
                            .unbind()
                            .into_any()
                    }
                    Some(ExecutionEngineResultItem::FlightPartitionRef(partition_ref)) => {
                        PyFlightPartitionRef::from(partition_ref)
                            .into_pyobject(py)?
                            .unbind()
                            .into_any()
                    }
                })
            })
        })
    }

    fn try_finish<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stats = handle.finish(false).await?;
            Ok(PyExecutionStats::from(stats))
        })
    }

    fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        handle.cancel.cancel();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle.finish(true).await?;
            Ok(())
        })
    }
}

fn dispatch_task_start_event(subscribers: &[Arc<dyn Subscriber>], event: &Event) {
    for subscriber in subscribers {
        if let Err(e) = subscriber.on_event(event.clone()) {
            log::debug!("Failed to dispatch task start event: {}", e);
        }
    }
}

#[cfg(test)]
#[path = "run_tests.rs"]
mod tests;
