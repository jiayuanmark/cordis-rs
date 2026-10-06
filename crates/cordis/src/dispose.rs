//! The undo list of one fiber run: what the runtime reverts when the fiber is disposed.

use std::any::Any;
use std::future::Future;
use std::ops::Deref;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::FutureExt;
use tokio::task::JoinHandle;

use crate::erased::BoxFuture;
use crate::{Guard, Guards};

/// How long an undo may run before the runtime warns that it is still running.
const SLOW_UNDO: Duration = Duration::from_secs(5);

/// Something a fiber undoes when it is disposed. Pass it to [`Cx::on_dispose`](crate::Cx::on_dispose).
///
/// Implemented for:
///
/// - [`Guard`]: dropped, which runs its inverse; [`Guards`] likewise, newest first.
/// - Tokio's [`JoinHandle`]: the task is aborted, then awaited until it has stopped, so
///   everything it owns is dropped before the next undo starts.
/// - Async closures, `FnOnce() -> impl Future<Output = ()>`: called, and the future awaited.
///
/// Implement it for a type with an asynchronous close, such as a connection.
pub trait Dispose: Send + 'static {
    /// Starts undoing the effect. Code that runs before the returned future is first polled
    /// runs as soon as disposal reaches this entry; the future then finishes the undo.
    fn dispose(self) -> impl Future<Output = ()> + Send + 'static;
}

impl Dispose for Guard {
    fn dispose(self) -> impl Future<Output = ()> + Send + 'static {
        drop(self);
        std::future::ready(())
    }
}

impl Dispose for Guards {
    fn dispose(self) -> impl Future<Output = ()> + Send + 'static {
        drop(self);
        std::future::ready(())
    }
}

impl<T: Send + 'static> Dispose for JoinHandle<T> {
    fn dispose(self) -> impl Future<Output = ()> + Send + 'static {
        self.abort();
        async move {
            // `Err` means the task was cancelled or panicked; either way it has stopped.
            let _ = self.await;
        }
    }
}

impl<F, Fut> Dispose for F
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn dispose(self) -> impl Future<Output = ()> + Send + 'static {
        self()
    }
}

/// One undo: calling it starts the undo and returns the rest of it.
type Entry = Box<dyn FnOnce() -> BoxFuture<()> + Send>;

/// The undo list of one fiber run. Clones share one list.
///
/// Every `Cx` of the run adds to it: [`Cx::on_dispose`](crate::Cx::on_dispose) entries,
/// listeners registered with `cx.on`, and children instantiated with `cx.plugin`. The runtime
/// runs it once, when the run unloads, fails, or is cancelled, and closes it first. An entry
/// added after that is undone at once, so nothing a run registers can outlive it, even when a
/// listener or a stored component keeps a clone of its `Cx`.
#[derive(Clone)]
pub(crate) struct Undo(Arc<List>);

/// `None` once the list has been closed.
struct List(Mutex<Option<Vec<Entry>>>);

impl Deref for List {
    type Target = Mutex<Option<Vec<Entry>>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for List {
    /// A list dropped without being run, because the app was dropped while the run was live,
    /// undoes its entries newest first as late entries: the synchronous parts in order now, and
    /// the rest concurrently on the runtime.
    fn drop(&mut self) {
        let entries = self.0.get_mut().unwrap_or_else(PoisonError::into_inner).take().unwrap_or_default();
        for entry in entries.into_iter().rev() {
            undo_late(entry);
        }
    }
}

impl Undo {
    pub(crate) fn new() -> Self {
        Self(Arc::new(List(Mutex::new(Some(Vec::new())))))
    }

    /// Adds `undo` to run before every entry already in the list, or undoes it now if the list
    /// has been closed.
    pub(crate) fn push(&self, undo: impl Dispose) {
        let entry: Entry = Box::new(move || Box::pin(undo.dispose()));
        let mut list = self.0.lock().unwrap();
        match list.as_mut() {
            Some(list) => list.push(entry),
            None => {
                drop(list);
                undo_late(entry);
            }
        }
    }

    pub(crate) fn is_open(&self) -> bool {
        self.0.lock().unwrap().is_some()
    }

    /// Closes the list and runs it newest first, each undo finishing before the next starts.
    ///
    /// A panicking undo is logged, and the rest still run.
    pub(crate) async fn run(&self) {
        let entries = self.0.lock().unwrap().take().unwrap_or_default();
        for entry in entries.into_iter().rev() {
            undo(entry).await;
        }
    }
}

async fn undo(entry: Entry) {
    let future = match catch_unwind(AssertUnwindSafe(entry)) {
        Ok(future) => future,
        Err(panic) => {
            tracing::error!(panic = panic_message(&*panic), "an undo panicked");
            return;
        }
    };
    let mut future = AssertUnwindSafe(future).catch_unwind();
    let mut waited = Duration::ZERO;
    loop {
        tokio::select! {
            result = &mut future => {
                if let Err(panic) = result {
                    tracing::error!(panic = panic_message(&*panic), "an undo panicked");
                }
                return;
            }
            () = tokio::time::sleep(SLOW_UNDO) => {
                waited += SLOW_UNDO;
                tracing::warn!(seconds = waited.as_secs(), "an undo is still running; the fiber's providers wait for it");
            }
        }
    }
}

/// Undoes an entry added after its run ended: its synchronous part now, the rest on the
/// current Tokio runtime if there is one.
fn undo_late(entry: Entry) {
    let future = match catch_unwind(AssertUnwindSafe(entry)) {
        Ok(future) => future,
        Err(panic) => {
            tracing::error!(panic = panic_message(&*panic), "an undo panicked");
            return;
        }
    };
    let mut future = AssertUnwindSafe(future).catch_unwind();
    let result = match (&mut future).now_or_never() {
        Some(result) => result,
        None => match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    if let Err(panic) = future.await {
                        tracing::error!(panic = panic_message(&*panic), "an undo panicked");
                    }
                });
                return;
            }
            Err(_) => {
                tracing::warn!(
                    "an undo was added after its fiber ended, outside a Tokio runtime; the rest of it is skipped"
                );
                return;
            }
        },
    };
    if let Err(panic) = result {
        tracing::error!(panic = panic_message(&*panic), "an undo panicked");
    }
}

pub(crate) fn panic_message(panic: &(dyn Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panic with a non-string payload".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log() -> (Arc<Mutex<Vec<&'static str>>>, impl Fn(&'static str) -> Guard) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let entries = log.clone();
        (log, move |name| {
            let entries = entries.clone();
            Guard::new(move || entries.lock().unwrap().push(name))
        })
    }

    #[tokio::test]
    async fn entries_run_newest_first_and_late_entries_run_at_once() {
        let (log, guard) = log();
        let undo = Undo::new();
        undo.push(guard("first"));
        undo.push(guard("second"));
        assert!(undo.is_open());

        undo.run().await;
        assert!(!undo.is_open());
        assert_eq!(*log.lock().unwrap(), ["second", "first"]);
        undo.push(guard("late"));
        assert_eq!(*log.lock().unwrap(), ["second", "first", "late"]);
    }

    #[test]
    fn a_list_dropped_without_running_undoes_its_entries() {
        let (log, guard) = log();
        let undo = Undo::new();
        undo.push(guard("first"));
        undo.push(guard("second"));
        drop(undo);
        assert_eq!(*log.lock().unwrap(), ["second", "first"]);
    }

    #[test]
    fn a_late_entry_without_a_runtime_runs_its_synchronous_part() {
        let (log, guard) = log();
        let undo = Undo::new();
        *undo.0.lock().unwrap() = None;
        undo.push(guard("late"));
        assert_eq!(*log.lock().unwrap(), ["late"]);
    }
}
