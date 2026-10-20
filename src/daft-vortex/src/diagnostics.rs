//! Opt-in, process-local benchmark diagnostics. Disabled reads do not start timers.

use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use common_error::{DaftError, DaftResult};

pub(crate) const MAX_EVENTS: usize = 50_000;

#[derive(Clone, Copy)]
#[repr(u8)]
pub(crate) enum Phase {
    Schema,
    PlanningMetadata,
    ExecutionOpen,
    Data,
    Execution,
}

impl Phase {
    pub(crate) fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Schema,
            1 => Self::PlanningMetadata,
            2 => Self::ExecutionOpen,
            4 => Self::Execution,
            _ => Self::Data,
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Schema => "schema",
            Self::PlanningMetadata => "planning_metadata",
            Self::ExecutionOpen => "execution_open",
            Self::Data => "data",
            Self::Execution => "execution",
        }
    }
}

pub(crate) struct Record {
    pub file_id: Option<u64>,
    pub phase: Phase,
    pub operation: &'static str,
    pub started_ns: u64,
    pub duration_ns: u64,
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

pub(crate) struct Snapshot {
    pub elapsed_ns: u64,
    pub file_count: usize,
    pub dropped_events: usize,
    pub unfinished_events: usize,
    pub events: Vec<Record>,
}

fn nanos(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

pub(crate) fn start() -> DaftResult<()> {
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
        events: Vec::new(),
    });
    ENABLED.store(true, Ordering::Release);
    Ok(())
}

pub(crate) fn finish() -> DaftResult<Snapshot> {
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

pub(crate) struct Span {
    generation: u64,
    started: Instant,
    record: Option<Record>,
    checkpoint_ns: u64,
}

pub(crate) fn begin(phase: Phase, operation: &'static str, uri: Option<&str>) -> Option<Span> {
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
    Some(Span {
        generation: session.generation,
        started: Instant::now(),
        checkpoint_ns: 0,
        record: Some(Record {
            file_id,
            phase,
            operation,
            started_ns: nanos(session.started.elapsed()),
            duration_ns: 0,
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
    pub(crate) fn range(&mut self, offset: u64, length: usize) {
        let record = self.record.as_mut().unwrap();
        record.offset = Some(offset);
        record.requested_bytes = Some(length);
    }

    pub(crate) fn returned(&mut self, length: usize) {
        self.record.as_mut().unwrap().returned_bytes = Some(length);
    }

    pub(crate) fn request_wait(&mut self) {
        let now = nanos(self.started.elapsed());
        self.record.as_mut().unwrap().request_wait_ns = Some(now);
        self.checkpoint_ns = now;
    }

    pub(crate) fn byte_wait(&mut self) {
        let now = nanos(self.started.elapsed());
        self.record.as_mut().unwrap().byte_wait_ns = Some(now.saturating_sub(self.checkpoint_ns));
        self.checkpoint_ns = now;
    }

    pub(crate) fn headers(&mut self) {
        let now = nanos(self.started.elapsed());
        self.record.as_mut().unwrap().header_ns = Some(now.saturating_sub(self.checkpoint_ns));
        self.checkpoint_ns = now;
    }

    pub(crate) fn body(&mut self) {
        let now = nanos(self.started.elapsed());
        self.record.as_mut().unwrap().body_ns = Some(now.saturating_sub(self.checkpoint_ns));
        self.checkpoint_ns = now;
    }

    pub(crate) fn finish(mut self, error: bool) {
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

pub(crate) fn complete(span: Option<Span>, error: bool) {
    if let Some(span) = span {
        span.finish(error);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_is_bounded_scoped_and_does_not_export_file_names() {
        assert!(finish().is_err());
        start().unwrap();
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
    }
}
