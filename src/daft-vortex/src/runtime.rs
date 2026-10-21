//! Diagnostic envelopes around the pinned SDK's existing Tokio executor.
//!
//! Scheduling and abort behavior are delegated unchanged. CPU closures have a
//! synchronous wall envelope; async tasks report poll wall time and suspension.
//! Neither is exclusive decoder CPU. Uninstrumented sessions use `with_tokio`.

use std::sync::{Arc, LazyLock};

use futures::{FutureExt, future::BoxFuture};
use vortex::io::runtime::{AbortHandleRef, Executor, Handle};

use crate::diagnostics::{self, Phase};

struct ProfiledTokio;

fn wrap_future(task: BoxFuture<'static, ()>, operation: &'static str) -> BoxFuture<'static, ()> {
    let queued = diagnostics::begin(Phase::Execution, "sdk_task_queue", None);
    async move {
        diagnostics::complete(queued, false);
        diagnostics::measure_async(Phase::Execution, operation, None, async move {
            task.await;
            Ok::<_, common_error::DaftError>(())
        })
        .await
        .expect("diagnostic wrapper cannot fail");
    }
    .boxed()
}

fn wrap_sync(task: Box<dyn FnOnce() + Send>, operation: &'static str) -> Box<dyn FnOnce() + Send> {
    let queued = diagnostics::begin(Phase::Execution, "sdk_task_queue", None);
    Box::new(move || {
        diagnostics::complete(queued, false);
        let work = diagnostics::begin(Phase::Execution, operation, None);
        task();
        diagnostics::complete(work, false);
    })
}

impl Executor for ProfiledTokio {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> AbortHandleRef {
        Executor::spawn(
            &tokio::runtime::Handle::current(),
            wrap_future(task, "sdk_task"),
        )
    }

    fn spawn_io(&self, task: BoxFuture<'static, ()>) -> AbortHandleRef {
        Executor::spawn_io(
            &tokio::runtime::Handle::current(),
            wrap_future(task, "sdk_io_task"),
        )
    }

    fn spawn_cpu(&self, task: Box<dyn FnOnce() + Send>) -> AbortHandleRef {
        Executor::spawn_cpu(
            &tokio::runtime::Handle::current(),
            wrap_sync(task, "sdk_cpu_task"),
        )
    }

    fn spawn_blocking_io(&self, task: Box<dyn FnOnce() + Send>) -> AbortHandleRef {
        Executor::spawn_blocking_io(
            &tokio::runtime::Handle::current(),
            wrap_sync(task, "sdk_blocking_io_task"),
        )
    }
}

pub(crate) fn handle() -> Handle {
    // This is a stateless executor adapter, not a session or metadata cache.
    static EXECUTOR: LazyLock<Arc<dyn Executor>> = LazyLock::new(|| Arc::new(ProfiledTokio));
    Handle::new(Arc::downgrade(&EXECUTOR))
}
