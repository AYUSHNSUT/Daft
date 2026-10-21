//! Bounded, opt-in query profiling on one native monotonic clock.
//!
//! Durations are elapsed wall time; overlapping operations are not additive CPU time.
//! Disabled instrumentation performs only an atomic load and does not read the clock.

use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use common_error::{DaftError, DaftResult};

pub const MAX_EVENTS: usize = 50_000;
const MAX_ACCUMULATED_OPERATIONS: usize = 8;

#[derive(Clone, Copy)]
#[repr(u8)]
pub enum Phase {
    Schema,
    PlanningMetadata,
    ExecutionOpen,
    Data,
    Execution,
    Io,
    Parquet,
    Engine,
}

impl Phase {
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Schema,
            1 => Self::PlanningMetadata,
            2 => Self::ExecutionOpen,
            4 => Self::Execution,
            5 => Self::Io,
            6 => Self::Parquet,
            7 => Self::Engine,
            _ => Self::Data,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Schema => "schema",
            Self::PlanningMetadata => "planning_metadata",
            Self::ExecutionOpen => "execution_open",
            Self::Data => "data",
            Self::Execution => "execution",
            Self::Io => "io",
            Self::Parquet => "parquet",
            Self::Engine => "engine",
        }
    }
}

#[derive(Clone, Copy)]
pub struct AccumulatedWork {
    pub operation: &'static str,
    pub calls: u64,
    /// Sum of disjoint synchronous call durations, including thread preemption.
    /// These durations have no exported interval locations and are not CPU time.
    pub wall_ns: u64,
}

pub struct Record {
    pub span_id: u64,
    /// Numeric identity of the thread starting the span; async spans may migrate.
    pub thread_id: u64,
    pub file_id: Option<u64>,
    pub phase: Phase,
    pub operation: &'static str,
    pub started_ns: u64,
    pub duration_ns: u64,
    /// Sum of time inside this future's poll calls, not total CPU across spawned tasks.
    pub active_poll_ns: Option<u64>,
    pub poll_count: Option<u64>,
    /// Bounded repeated work inside this envelope; never treat sums as intervals.
    pub accumulated_work: Vec<AccumulatedWork>,
    pub offset: Option<u64>,
    pub requested_bytes: Option<usize>,
    pub returned_bytes: Option<usize>,
    pub request_wait_ns: Option<u64>,
    pub byte_wait_ns: Option<u64>,
    pub header_ns: Option<u64>,
    pub body_ns: Option<u64>,
    pub error: bool,
    pub cancelled: bool,
}

struct Session {
    generation: u64,
    started: Instant,
    started_events: usize,
    dropped_events: usize,
    // URIs stay private and are dropped at finish. Never include this map in reports.
    files: HashMap<String, u64>,
    threads: HashMap<std::thread::ThreadId, u64>,
    events: Vec<Record>,
}

struct State {
    next_generation: u64,
    session: Option<Session>,
}

static ENABLED: AtomicBool = AtomicBool::new(false);
static STATE: Mutex<State> = Mutex::new(State {
    next_generation: 0,
    session: None,
});

pub struct Snapshot {
    pub elapsed_ns: u64,
    pub file_count: usize,
    pub dropped_events: usize,
    pub unfinished_events: usize,
    pub events: Vec<Record>,
}

fn nanos(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

pub fn start() -> DaftResult<()> {
    let mut state = STATE.lock().unwrap();
    if state.session.is_some() {
        return Err(DaftError::ValueError(
            "Vortex diagnostics already have an active collector".into(),
        ));
    }
    state.next_generation = state.next_generation.wrapping_add(1);
    state.session = Some(Session {
        generation: state.next_generation,
        started: Instant::now(),
        started_events: 0,
        dropped_events: 0,
        files: HashMap::new(),
        threads: HashMap::new(),
        events: Vec::new(),
    });
    ENABLED.store(true, Ordering::Release);
    Ok(())
}

pub fn finish() -> DaftResult<Snapshot> {
    let mut state = STATE.lock().unwrap();
    let session = state.session.take().ok_or_else(|| {
        DaftError::ValueError("Vortex diagnostics have no active collector".into())
    })?;
    ENABLED.store(false, Ordering::Release);
    Ok(Snapshot {
        elapsed_ns: nanos(session.started.elapsed()),
        file_count: session.files.len(),
        dropped_events: session.dropped_events,
        unfinished_events: session.started_events.saturating_sub(session.events.len()),
        events: session.events,
    })
}

pub struct Span {
    generation: u64,
    started: Instant,
    record: Option<Record>,
    checkpoint_ns: u64,
}

/// Current capture time. Python callers use this clock for cross-language intervals.
/// Returns `None` without reading a clock when profiling is disabled.
pub fn instant_ns() -> Option<u64> {
    if !ENABLED.load(Ordering::Acquire) {
        return None;
    }
    STATE
        .lock()
        .unwrap()
        .session
        .as_ref()
        .map(|session| nanos(session.started.elapsed()))
}

pub fn begin(phase: Phase, operation: &'static str, uri: Option<&str>) -> Option<Span> {
    if !ENABLED.load(Ordering::Acquire) {
        return None;
    }
    let mut state = STATE.lock().unwrap();
    let session = state.session.as_mut()?;
    if session.started_events >= MAX_EVENTS {
        session.dropped_events = session.dropped_events.saturating_add(1);
        return None;
    }
    session.started_events += 1;
    let next_id = session.files.len() as u64 + 1;
    let file_id = uri.map(|uri| *session.files.entry(uri.to_string()).or_insert(next_id));
    let next_thread_id = session.threads.len() as u64 + 1;
    let thread_id = *session
        .threads
        .entry(std::thread::current().id())
        .or_insert(next_thread_id);
    let started = Instant::now();
    Some(Span {
        generation: session.generation,
        started,
        checkpoint_ns: 0,
        record: Some(Record {
            span_id: session.started_events as u64,
            thread_id,
            file_id,
            phase,
            operation,
            started_ns: nanos(started.duration_since(session.started)),
            duration_ns: 0,
            active_poll_ns: None,
            poll_count: None,
            accumulated_work: Vec::new(),
            offset: None,
            requested_bytes: None,
            returned_bytes: None,
            request_wait_ns: None,
            byte_wait_ns: None,
            header_ns: None,
            body_ns: None,
            error: false,
            cancelled: false,
        }),
    })
}

impl Span {
    /// Record synchronous wall duration inside one poll, excluding suspension.
    pub fn polled(&mut self, elapsed: std::time::Duration) {
        let record = self.record.as_mut().unwrap();
        *record.active_poll_ns.get_or_insert(0) += nanos(elapsed);
        *record.poll_count.get_or_insert(0) += 1;
    }

    pub fn range(&mut self, offset: u64, length: usize) {
        let record = self.record.as_mut().unwrap();
        record.offset = Some(offset);
        record.requested_bytes = Some(length);
    }

    pub fn returned(&mut self, length: usize) {
        self.record.as_mut().unwrap().returned_bytes = Some(length);
    }

    pub fn request_wait(&mut self) {
        let now = nanos(self.started.elapsed());
        self.record.as_mut().unwrap().request_wait_ns = Some(now);
        self.checkpoint_ns = now;
    }

    pub fn byte_wait(&mut self) {
        let now = nanos(self.started.elapsed());
        self.record.as_mut().unwrap().byte_wait_ns = Some(now.saturating_sub(self.checkpoint_ns));
        self.checkpoint_ns = now;
    }

    pub fn headers(&mut self) {
        let now = nanos(self.started.elapsed());
        self.record.as_mut().unwrap().header_ns = Some(now.saturating_sub(self.checkpoint_ns));
        self.checkpoint_ns = now;
    }

    pub fn body(&mut self) {
        let now = nanos(self.started.elapsed());
        self.record.as_mut().unwrap().body_ns = Some(now.saturating_sub(self.checkpoint_ns));
        self.checkpoint_ns = now;
    }

    pub fn finish(mut self, error: bool) {
        self.record(error, false);
    }

    fn record(&mut self, error: bool, cancelled: bool) {
        let Some(mut record) = self.record.take() else {
            return;
        };
        record.duration_ns = nanos(self.started.elapsed());
        record.error = error;
        record.cancelled = cancelled;
        let mut state = STATE.lock().unwrap();
        if let Some(session) = &mut state.session
            && session.generation == self.generation
        {
            session.events.push(record);
        }
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        self.record(false, true);
    }
}

pub fn complete(span: Option<Span>, error: bool) {
    if let Some(span) = span {
        span.finish(error);
    }
}

/// Accumulate frequent synchronous calls without emitting an event per call.
///
/// The span is a real lifetime envelope, including gaps and awaited work. Its
/// fixed counters contain only measured call durations and have no interval
/// locations, so their sums must not be used to construct a wall-time union.
pub struct AccumulatedSpan<const N: usize> {
    span: Option<Span>,
    work: [AccumulatedWork; N],
}

impl<const N: usize> AccumulatedSpan<N> {
    pub fn new(
        phase: Phase,
        operation: &'static str,
        uri: Option<&str>,
        operations: [&'static str; N],
    ) -> Self {
        // Instrumentation labels are compile-time call-site constants; bound
        // their storage independently of the number of measured calls.
        const { assert!(N <= MAX_ACCUMULATED_OPERATIONS) };
        Self {
            span: begin(phase, operation, uri),
            work: operations.map(|operation| AccumulatedWork {
                operation,
                calls: 0,
                wall_ns: 0,
            }),
        }
    }

    /// Measure one synchronous call. Disabled captures do not read the clock.
    /// Errors are returned unchanged and still count as attempted work.
    pub fn measure<T>(&mut self, index: usize, f: impl FnOnce() -> T) -> T {
        if self.span.is_none() {
            return f();
        }
        let work = &mut self.work[index];
        let started = Instant::now();
        let result = f();
        work.wall_ns = work.wall_ns.saturating_add(nanos(started.elapsed()));
        work.calls = work.calls.saturating_add(1);
        result
    }

    pub fn finish(mut self, error: bool) {
        self.attach();
        complete(self.span.take(), error);
    }

    fn attach(&mut self) {
        if let Some(span) = &mut self.span {
            span.record.as_mut().unwrap().accumulated_work = self.work.to_vec();
        }
    }
}

impl<const N: usize> Drop for AccumulatedSpan<N> {
    fn drop(&mut self) {
        // Preserve work already performed if the enclosing future is cancelled.
        self.attach();
    }
}

/// Measure a synchronous operation's wall time, including thread descheduling.
pub fn measure<T, E>(
    phase: Phase,
    operation: &'static str,
    uri: Option<&str>,
    f: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let span = begin(phase, operation, uri);
    let result = f();
    complete(span, result.is_err());
    result
}

/// Measure an awaited operation. This includes async wait and scheduler delay.
pub fn measure_async<T, E, F>(
    phase: Phase,
    operation: &'static str,
    uri: Option<&str>,
    future: F,
) -> impl std::future::Future<Output = Result<T, E>> + Send + use<T, E, F>
where
    F: std::future::Future<Output = Result<T, E>> + Send,
{
    measure_future(begin(phase, operation, uri), future)
}

pin_project_lite::pin_project! {
    /// Pin the wrapped future in place without boxing or duplicating its state.
    struct ProfiledFuture<F> {
        #[pin]
        future: F,
        span: Option<Span>,
    }
}

impl<T, E, F> std::future::Future for ProfiledFuture<F>
where
    F: std::future::Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();
        let result = if let Some(span) = this.span.as_mut() {
            let started = Instant::now();
            let result = this.future.as_mut().poll(cx);
            span.polled(started.elapsed());
            result
        } else {
            this.future.as_mut().poll(cx)
        };
        if let std::task::Poll::Ready(result) = &result {
            complete(this.span.take(), result.is_err());
        }
        result
    }
}

/// Like `measure_async`, using an already-created span so URI borrows end before polling.
/// The explicit Send bound is an opaque auto-trait boundary for nested I/O futures.
pub fn measure_future<T, E, F>(
    span: Option<Span>,
    future: F,
) -> impl std::future::Future<Output = Result<T, E>> + Send
where
    F: std::future::Future<Output = Result<T, E>> + Send,
{
    ProfiledFuture { future, span }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_is_bounded_scoped_and_does_not_export_file_names() {
        assert!(finish().is_err());
        assert!(instant_ns().is_none());
        // The profiling adapter must not duplicate a large future's inline state.
        // An async wrapper that moves F into a pinned local inflated nested I/O
        // futures enough to exceed the workspace's stack-size lint.
        let large = async {
            let bytes = [0_u8; 4096];
            std::future::pending::<()>().await;
            Ok::<_, ()>(bytes)
        };
        let size = std::mem::size_of_val(&large);
        let measured = measure_future(None, large);
        assert!(
            std::mem::size_of_val(&measured) <= size + std::mem::size_of::<Option<Span>>() + 16
        );
        drop(measured);
        start().unwrap();
        assert!(instant_ns().is_some());
        assert!(start().is_err());
        let old = begin(
            Phase::Schema,
            "old",
            Some("https://secret:password@host/data?token=private"),
        );
        let had_old = old.is_some();
        let first = finish().unwrap();
        assert!(first.unfinished_events >= usize::from(had_old));
        start().unwrap();
        complete(old, false);
        for _ in 0..=MAX_EVENTS {
            let mut span = begin(Phase::Data, "fixture", Some("s3://bucket/file"));
            if let Some(span) = &mut span {
                span.range(3, 8);
                span.returned(8);
            }
            complete(span, false);
        }
        let snapshot = finish().unwrap();
        assert!(snapshot.events.len() <= MAX_EVENTS);
        assert!(snapshot.dropped_events >= 1);
        assert!(!snapshot.events.iter().any(|event| event.operation == "old"));
        assert!(
            snapshot
                .events
                .iter()
                .all(|event| event.span_id > 0 && event.thread_id > 0)
        );
        for event in snapshot
            .events
            .iter()
            .filter(|event| event.operation == "fixture")
        {
            assert_eq!(event.requested_bytes, Some(8));
            assert_eq!(event.returned_bytes, Some(8));
        }
        assert!(begin(Phase::Data, "disabled", None).is_none());
        assert!(finish().is_err());

        start().unwrap();
        let before = instant_ns().unwrap();
        assert_eq!(
            measure(Phase::Parquet, "sync_ok", None, || Ok::<_, ()>(7)),
            Ok(7)
        );
        assert_eq!(
            measure(Phase::Io, "sync_error", None, || Err::<(), _>(9)),
            Err(9)
        );
        futures::executor::block_on(async {
            let mut first = true;
            let result = measure_async(
                Phase::Io,
                "poll_fixture",
                None,
                std::future::poll_fn(|cx| {
                    if std::mem::replace(&mut first, false) {
                        cx.waker().wake_by_ref();
                        std::task::Poll::Pending
                    } else {
                        std::task::Poll::Ready(Ok::<_, ()>(11))
                    }
                }),
            )
            .await;
            assert_eq!(result, Ok(11));
            let mut cancelled = Box::pin(measure_async(
                Phase::Io,
                "cancel_fixture",
                None,
                std::future::pending::<Result<(), ()>>(),
            ));
            assert!(futures::poll!(cancelled.as_mut()).is_pending());
            drop(cancelled);
        });
        let after = instant_ns().unwrap();
        let snapshot = finish().unwrap();
        assert_eq!(snapshot.unfinished_events, 0);
        assert_eq!(snapshot.events.len(), 4);
        assert!(
            snapshot
                .events
                .iter()
                .all(|e| e.started_ns >= before && e.started_ns + e.duration_ns <= after)
        );
        let polled = snapshot
            .events
            .iter()
            .find(|e| e.operation == "poll_fixture")
            .unwrap();
        assert_eq!(polled.poll_count, Some(2));
        assert!(polled.active_poll_ns.unwrap() <= polled.duration_ns);
        assert!(
            snapshot
                .events
                .iter()
                .find(|e| e.operation == "sync_error")
                .unwrap()
                .error
        );
        assert!(
            snapshot
                .events
                .iter()
                .find(|e| e.operation == "cancel_fixture")
                .unwrap()
                .cancelled
        );

        // Tiny repeated calls must not exhaust the event budget. Preserve both
        // attempted error calls and work completed before task cancellation.
        start().unwrap();
        let mut accumulated =
            AccumulatedSpan::new(Phase::Parquet, "repeated_fixture", None, ["decode", "skip"]);
        for _ in 0..=MAX_EVENTS {
            assert_eq!(accumulated.measure(0, || std::hint::black_box(7)), 7);
        }
        assert_eq!(accumulated.measure(1, || Err::<(), _>(9)), Err(9));
        accumulated.finish(true);
        let mut cancelled =
            AccumulatedSpan::new(Phase::Parquet, "repeated_cancel", None, ["decode"]);
        cancelled.measure(0, || std::hint::black_box(1));
        drop(cancelled);
        let snapshot = finish().unwrap();
        assert_eq!(snapshot.dropped_events, 0);
        assert_eq!(snapshot.unfinished_events, 0);
        assert_eq!(snapshot.events.len(), 2);
        let completed = &snapshot.events[0];
        assert!(completed.error);
        assert_eq!(completed.accumulated_work.len(), 2);
        assert_eq!(completed.accumulated_work[0].calls, MAX_EVENTS as u64 + 1);
        assert_eq!(completed.accumulated_work[1].calls, 1);
        assert!(
            completed
                .accumulated_work
                .iter()
                .map(|work| work.wall_ns)
                .sum::<u64>()
                <= completed.duration_ns
        );
        assert!(snapshot.events[1].cancelled);
        assert_eq!(snapshot.events[1].accumulated_work[0].calls, 1);
        let mut disabled =
            AccumulatedSpan::new(Phase::Parquet, "disabled_repeat", None, ["decode"]);
        assert_eq!(disabled.measure(0, || 11), 11);
        assert_eq!(disabled.work[0].calls, 0);
        assert_eq!(disabled.work[0].wall_ns, 0);
    }
}
