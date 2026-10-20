//! Exercise the production result router and executor without timing sleeps.

use std::time::Duration;

use arrow_array::Int64Array;
use daft_core::prelude::{DataType, Field, Schema};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::stats::StatsState;
use futures::poll;

use super::*;

fn partition(rows: usize) -> MicroPartition {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64)]));
    MicroPartition::from_arrow(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(0..rows as i64))],
    )
    .unwrap()
}

fn plan() -> LocalPhysicalPlanRef {
    LocalPhysicalPlan::in_memory_scan(
        0,
        partition(1).schema(),
        8,
        StatsState::NotMaterialized,
        LocalNodeContext::default(),
    )
}

fn input(rows: usize) -> HashMap<SourceId, Input> {
    HashMap::from([(0, Input::InMemory(vec![Arc::new(partition(rows))]))])
}

async fn enqueue(
    executor: &Arc<Mutex<NativeExecutor>>,
    input_id: InputId,
    inputs: HashMap<SourceId, Input>,
    capacity: Option<usize>,
) -> ResultHandle {
    let (fingerprint, future) = executor
        .lock()
        .unwrap()
        .run_with_result_buffer(
            &plan(),
            Arc::new(DaftExecutionConfig::default()),
            vec![],
            Some(HashMap::from([("plan_fingerprint".into(), "42".into())])),
            inputs,
            input_id,
            true,
            capacity,
        )
        .unwrap();
    let registration = InputRegistration {
        executor: executor.clone(),
        fingerprint,
        input_id,
        active: AtomicBool::new(true),
    };
    ResultHandle::new(future.await.unwrap(), registration)
}

fn morsel(rows: usize) -> PipelineMessage {
    PipelineMessage::Morsel {
        input_id: 0,
        partition: partition(rows),
    }
}

#[tokio::test]
async fn result_buffer_enforces_capacity_and_preserves_order() {
    for capacity in [1, 3] {
        let (output, mut result) = create_result_channel(Some(capacity));
        let mut router = MessageRouter::new();
        router.insert_output_sender(0, output);
        for rows in 1..=capacity {
            router.route_message(morsel(rows)).unwrap().await;
        }
        let mut blocked = router.route_message(morsel(capacity + 1)).unwrap();
        assert!(
            poll!(&mut blocked).is_pending(),
            "the next result must wait for capacity"
        );
        let Some(ExecutionEngineResultItem::Partition(first)) = result.next().await else {
            panic!()
        };
        assert_eq!(first.len(), 1);
        blocked.await;
        assert!(router.route_message(PipelineMessage::Flush(0)).is_none());
        for rows in 2..=capacity + 1 {
            let Some(ExecutionEngineResultItem::Partition(partition)) = result.next().await else {
                panic!()
            };
            assert_eq!(partition.len(), rows);
        }
        assert!(result.next().await.is_none());
        assert!(result.exhausted);
    }
}

#[tokio::test]
async fn abandoning_full_result_buffer_releases_blocked_router() {
    let (output, result) = create_result_channel(Some(1));
    let mut router = MessageRouter::new();
    router.insert_output_sender(0, output);
    router.route_message(morsel(1)).unwrap().await;
    let mut blocked = router.route_message(morsel(2)).unwrap();
    assert!(poll!(&mut blocked).is_pending());
    drop(result);
    assert!(
        poll!(&mut blocked).is_ready(),
        "abandonment must not require draining a full queue"
    );
}

#[tokio::test]
async fn closing_pending_next_is_idempotent_and_removes_last_plan() {
    let executor = Arc::new(Mutex::new(NativeExecutor::new(false, "")));
    // No source input arrives: next remains blocked until close cancels it.
    let handle = enqueue(&executor, 0, HashMap::new(), None).await;
    let mut pending_next = Box::pin(handle.next());
    assert!(poll!(&mut pending_next).is_pending());
    let (next, closed, also_closed) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(pending_next, handle.finish(true), handle.finish(true))
    })
    .await
    .unwrap();
    assert!(next.is_none());
    closed.unwrap();
    also_closed.unwrap();
    handle.finish(true).await.unwrap();
    assert!(handle.next().await.is_none());
    assert!(executor.lock().unwrap().plans.is_empty());
}

#[tokio::test]
async fn closing_one_shared_input_preserves_other_input_results_and_stats() {
    let executor = Arc::new(Mutex::new(NativeExecutor::new(false, "")));
    let abandoned_inputs = HashMap::from([(
        0,
        Input::InMemory((0..32).map(|_| Arc::new(partition(1))).collect()),
    )]);
    let abandoned = enqueue(&executor, 0, abandoned_inputs, Some(1)).await;
    let retained = enqueue(&executor, 1, input(5), Some(1)).await;
    abandoned.finish(true).await.unwrap();
    {
        let executor = executor.lock().unwrap();
        let state = &executor.plans[&42];
        assert_eq!(state.active_input_ids, HashSet::from([1]));
        assert!(!state.cancel.is_cancelled());
    }
    // Old queued data and Flush messages must never be routed to a new
    // consumer that reuses an abandoned id on this still-active plan.
    let reused = executor.lock().unwrap().run_with_result_buffer(
        &plan(),
        Arc::new(DaftExecutionConfig::default()),
        vec![],
        Some(HashMap::from([("plan_fingerprint".into(), "42".into())])),
        input(100),
        0,
        true,
        Some(1),
    );
    assert!(
        matches!(reused, Err(common_error::DaftError::ValueError(message)) if message.contains("abandoned"))
    );
    let mut rows = 0;
    while let Some(item) = tokio::time::timeout(Duration::from_secs(10), retained.next())
        .await
        .unwrap()
    {
        if let ExecutionEngineResultItem::Partition(partition) = item {
            rows += partition.len();
        }
    }
    assert_eq!(rows, 5);
    let stats = retained.finish(false).await.unwrap();
    assert!(
        !stats.nodes.is_empty(),
        "EOF must retain execution statistics"
    );
    assert!(executor.lock().unwrap().plans.is_empty());
    retained.finish(true).await.unwrap();
}

#[tokio::test]
async fn local_default_result_buffer_is_one() {
    assert_eq!(result_buffer_capacity(false, None).unwrap(), Some(1));
    assert_eq!(result_buffer_capacity(true, None).unwrap(), None);
    assert_eq!(result_buffer_capacity(false, Some(3)).unwrap(), Some(3));
    assert_eq!(result_buffer_capacity(true, Some(3)).unwrap(), Some(3));
    let executor = Arc::new(Mutex::new(NativeExecutor::new(false, "")));
    let handle = enqueue(&executor, 0, HashMap::new(), None).await;
    let guard = handle.result.lock().await;
    let result = guard.as_ref().unwrap();
    assert!(matches!(result.receiver, ResultReceiver::Bounded(_)));
    drop(guard);
    handle.finish(true).await.unwrap();
    let (output, mut result) = create_result_channel(result_buffer_capacity(false, None).unwrap());
    output
        .clone()
        .send(ExecutionEngineResultItem::Partition(partition(1)))
        .await;
    let mut second = Box::pin(output.send(ExecutionEngineResultItem::Partition(partition(2))));
    assert!(poll!(&mut second).is_pending());
    assert!(result.next().await.is_some());
    second.await;
}

#[tokio::test]
async fn normally_finished_input_id_can_be_reused_in_shared_plan() {
    let executor = Arc::new(Mutex::new(NativeExecutor::new(false, "")));
    let held_open = enqueue(&executor, 1, HashMap::new(), Some(1)).await;
    for expected_rows in [3, 5] {
        let result = enqueue(&executor, 0, input(expected_rows), Some(1)).await;
        let mut rows = 0;
        while let Some(item) = tokio::time::timeout(Duration::from_secs(10), result.next())
            .await
            .unwrap()
        {
            if let ExecutionEngineResultItem::Partition(partition) = item {
                rows += partition.len();
            }
        }
        assert_eq!(rows, expected_rows);
        result.finish(false).await.unwrap();
        assert_eq!(
            executor.lock().unwrap().plans[&42].active_input_ids,
            HashSet::from([1])
        );
    }
    held_open.finish(true).await.unwrap();
    assert!(executor.lock().unwrap().plans.is_empty());
}

#[test]
fn invalid_result_capacity_is_rejected_before_starting_plan() {
    let mut executor = NativeExecutor::new(false, "");
    for capacity in [0, tokio::sync::Semaphore::MAX_PERMITS + 1, usize::MAX] {
        let result = executor.run_with_result_buffer(
            &plan(),
            Arc::new(DaftExecutionConfig::default()),
            vec![],
            None,
            input(1),
            0,
            true,
            Some(capacity),
        );
        assert!(matches!(
            result,
            Err(common_error::DaftError::ValueError(_))
        ));
        assert!(executor.plans.is_empty());
    }
}

#[test]
fn dropping_pending_enqueue_registration_cancels_its_plan() {
    let executor = Arc::new(Mutex::new(NativeExecutor::new(false, "")));
    let (fingerprint, pending_enqueue) = executor
        .lock()
        .unwrap()
        .run(
            &plan(),
            Arc::new(DaftExecutionConfig::default()),
            vec![],
            None,
            HashMap::new(),
            0,
            true,
        )
        .unwrap();
    let cancel = executor.lock().unwrap().plans[&fingerprint].cancel.clone();
    let registration = InputRegistration {
        executor: executor.clone(),
        fingerprint,
        input_id: 0,
        active: AtomicBool::new(true),
    };
    // The same guard is captured by the Python enqueue future before it polls.
    drop(pending_enqueue);
    drop(registration);
    assert!(executor.lock().unwrap().plans.is_empty());
    assert!(cancel.is_cancelled());
}

fn runtime_context() -> (ExecutionRuntimeContext, RuntimeStatsManager) {
    let (pipeline, _) = translate_physical_plan_to_pipeline(
        &plan(),
        &Arc::new(DaftExecutionConfig::default()),
        &BuilderContext::new(),
    )
    .unwrap();
    let stats = RuntimeStatsManager::try_new(
        &Handle::current(),
        &pipeline,
        vec![],
        QueryID::from("completion-test"),
        true,
    )
    .unwrap();
    let runtime =
        ExecutionRuntimeContext::new(get_or_init_memory_manager().clone(), stats.handle());
    (runtime, stats)
}

#[tokio::test]
async fn output_closure_without_flush_preserves_delayed_worker_error() {
    let (mut runtime, stats) = runtime_context();
    let (pipeline_tx, mut pipeline_rx) = create_channel::<PipelineMessage>(1);
    let (release_error, wait_for_release) = tokio::sync::oneshot::channel();
    runtime.spawn(
        async move {
            // Reproduce a nested worker losing its last output sender before
            // its error reaches the outer execution task.
            drop(pipeline_tx);
            wait_for_release.await.unwrap();
            Err(common_error::DaftError::ValueError(
                "delayed producer failure".into(),
            ))
        },
        "controlled failing source",
    );
    assert!(pipeline_rx.recv().await.is_none());
    let (output, _consumer) = create_result_channel(Some(1));
    let mut router = MessageRouter::new();
    router.insert_output_sender(0, output);
    let cancel = CancellationToken::new();
    let mut finish = Box::pin(finish_runtime_tasks(
        &mut runtime,
        &cancel,
        router.has_unfinished_consumers(),
        Ok(()),
        QueryEndState::Finished,
    ));
    assert!(poll!(&mut finish).is_pending());
    release_error.send(()).unwrap();
    let (result, status) = tokio::time::timeout(Duration::from_secs(10), finish)
        .await
        .unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("delayed producer failure")
    );
    assert!(matches!(status, QueryEndState::Failed));
    stats.finish(status).await;
}

#[tokio::test]
async fn unfinished_output_drain_remains_cancellable() {
    let (mut runtime, stats) = runtime_context();
    runtime.spawn(futures::future::pending(), "blocked source");
    let cancel = CancellationToken::new();
    let mut finish = Box::pin(finish_runtime_tasks(
        &mut runtime,
        &cancel,
        true,
        Ok(()),
        QueryEndState::Finished,
    ));
    assert!(poll!(&mut finish).is_pending());
    cancel.cancel();
    let (result, status) = tokio::time::timeout(Duration::from_secs(10), finish)
        .await
        .unwrap();
    result.unwrap();
    assert!(matches!(status, QueryEndState::Canceled));
    stats.finish(status).await;
}

#[tokio::test]
async fn flushed_result_does_not_wait_for_abandoned_shared_upstream() {
    let (mut runtime, stats) = runtime_context();
    runtime.spawn(futures::future::pending(), "blocked abandoned source");
    let (abandoned, consumer) = create_result_channel(Some(1));
    let (retained, _retained_consumer) = create_result_channel(Some(1));
    let mut router = MessageRouter::new();
    router.insert_output_sender(0, abandoned);
    router.insert_output_sender(1, retained);
    drop(consumer);
    router.route_message(PipelineMessage::Flush(1));
    assert!(!router.has_unfinished_consumers());
    let (result, status) = tokio::time::timeout(
        Duration::from_secs(10),
        finish_runtime_tasks(
            &mut runtime,
            &CancellationToken::new(),
            router.has_unfinished_consumers(),
            Ok(()),
            QueryEndState::Finished,
        ),
    )
    .await
    .unwrap();
    result.unwrap();
    assert!(matches!(status, QueryEndState::Finished));
    stats.finish(status).await;
}
