//! The undo list of one fiber run: what the runtime reverts when the fiber is disposed.

use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use futures::FutureExt;
use tokio::task::JoinHandle;

use crate::erased::BoxFuture;
use crate::{Guard, Guards};

/// Something a fiber undoes when it is disposed. Pass it to [`Cx::on_dispose`](crate::Cx::on_dispose).
///
/// Implemented for:
///
/// - [`Guard`]: dropped, which runs its inverse; [`Guards`] likewise, newest first.
/// - Tokio's [`JoinHandle`]: the task is aborted, then awaited until it has stopped, so
///   everything it owns is dropped before the next undo starts. A `spawn_blocking` task cannot
///   be aborted once it has started, so disposal waits until its closure returns.
/// - Async closures, `FnOnce() -> impl Future<Output = ()>`: called, and the future awaited.
///
/// Implement it for a type with an asynchronous close, such as a connection.
pub trait Dispose: Send + 'static {
    /// Starts undoing the effect. Code that runs before the returned future is first polled
    /// runs as soon as disposal reaches this entry; the future then finishes the undo.
    fn dispose(self) -> impl Future<Output = ()> + Send + 'static;

    /// Returns whether the effect has already ended on its own, such as a task that has
    /// finished. The fiber then drops the value without calling [`dispose`](Dispose::dispose),
    /// so a run that keeps adding short-lived effects doesn't accumulate them until teardown.
    ///
    /// Called while the run's list is locked: keep it cheap, and don't add to the list from it.
    /// Defaults to `false`.
    fn is_finished(&self) -> bool {
        false
    }
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

    fn is_finished(&self) -> bool {
        JoinHandle::is_finished(self)
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

/// One entry of the list: a [`Dispose`] value with its type erased.
trait Entry: Send {
    /// Starts the undo and returns the rest of it.
    fn start(self: Box<Self>) -> BoxFuture<()>;
    fn is_finished(&self) -> bool;
}

impl<D: Dispose> Entry for D {
    fn start(self: Box<Self>) -> BoxFuture<()> {
        Box::pin((*self).dispose())
    }

    fn is_finished(&self) -> bool {
        Dispose::is_finished(self)
    }
}

/// The list drops finished entries when it reaches this length, and then at twice the length
/// that remains, so pushing stays cheap.
const PRUNE_AT: usize = 32;

/// The undo list of one fiber run, as its owners hold it: the activation task, then the
/// fiber's instance.
///
/// Every `Cx` of the run adds to it through an [`UndoHandle`]: [`Cx::on_dispose`](crate::Cx::on_dispose)
/// entries, listeners registered with `cx.on`, and children instantiated with `cx.plugin`. The
/// runtime runs it once, when the run unloads, fails, or is cancelled, and closes it first. An
/// entry added after that is undone at once, so nothing a run registers can outlive it.
#[derive(Clone)]
pub(crate) struct Undo(Arc<List>);

/// A handle to a run's undo list, as every `Cx` of the run holds it.
///
/// It does not keep the list alive, so a `Cx` captured by a listener or by an undo cannot keep
/// the run's effects in place: when the owners drop the list without running it, because the
/// app was dropped, the list undoes its entries anyway.
#[derive(Clone)]
pub(crate) struct UndoHandle(Weak<List>);

/// `None` once the list has been closed.
struct List(Mutex<Option<Open>>);

struct Open {
    entries: Vec<Box<dyn Entry>>,
    prune_at: usize,
}

impl Open {
    fn push(&mut self, entry: Box<dyn Entry>) {
        if self.entries.len() >= self.prune_at {
            self.entries.retain(|entry| !entry.is_finished());
            self.prune_at = (2 * self.entries.len()).max(PRUNE_AT);
        }
        self.entries.push(entry);
    }
}

impl List {
    /// Locks the list. A panic while it was locked (in an `is_finished`) leaves it consistent,
    /// so poisoning is ignored.
    fn lock(&self) -> MutexGuard<'_, Option<Open>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn push(&self, entry: Box<dyn Entry>) {
        let mut list = self.lock();
        match list.as_mut() {
            Some(open) => open.push(entry),
            None => {
                drop(list);
                undo_late(entry);
            }
        }
    }

    /// Closes the list and returns its entries, newest first.
    fn close(&self) -> impl Iterator<Item = Box<dyn Entry>> + use<> {
        let open = self.lock().take();
        open.map(|open| open.entries).unwrap_or_default().into_iter().rev()
    }
}

impl Drop for List {
    /// A list dropped without being run, because the app was dropped while the run was live,
    /// undoes its entries newest first as late entries: the synchronous parts in order now, and
    /// the rest concurrently on the runtime.
    fn drop(&mut self) {
        for entry in self.close() {
            undo_late(entry);
        }
    }
}

impl Undo {
    pub(crate) fn new() -> Self {
        Self(Arc::new(List(Mutex::new(Some(Open { entries: Vec::new(), prune_at: PRUNE_AT })))))
    }

    pub(crate) fn handle(&self) -> UndoHandle {
        UndoHandle(Arc::downgrade(&self.0))
    }

    /// Closes the list and runs it newest first, each undo finishing before the next starts.
    ///
    /// A panicking undo is logged, and the rest still run.
    pub(crate) async fn run(&self) {
        for entry in self.0.close() {
            undo(entry).await;
        }
    }
}

impl UndoHandle {
    /// Returns the list while one of its owners still holds it.
    pub(crate) fn upgrade(&self) -> Option<Undo> {
        self.0.upgrade().map(Undo)
    }

    /// Adds `undo` to run before every entry already in the list, or undoes it now if the list
    /// has been closed.
    pub(crate) fn push(&self, undo: impl Dispose) {
        let entry: Box<dyn Entry> = Box::new(undo);
        match self.0.upgrade() {
            Some(list) => list.push(entry),
            None => undo_late(entry),
        }
    }

    pub(crate) fn is_open(&self) -> bool {
        self.0.upgrade().is_some_and(|list| list.lock().is_some())
    }
}

type Started = futures::future::CatchUnwind<AssertUnwindSafe<BoxFuture<()>>>;

/// Calls `entry`, returning the rest of its undo, or `None` if the call panicked.
fn start(entry: Box<dyn Entry>) -> Option<Started> {
    match catch_unwind(AssertUnwindSafe(move || entry.start())) {
        Ok(future) => Some(AssertUnwindSafe(future).catch_unwind()),
        Err(panic) => {
            report(Err(panic));
            None
        }
    }
}

fn report(result: std::thread::Result<()>) {
    if let Err(panic) = result {
        tracing::error!(panic = panic_message(&*panic), "an undo panicked");
    }
}

async fn undo(entry: Box<dyn Entry>) {
    if let Some(future) = start(entry) {
        report(future.await);
    }
}

/// Undoes an entry added after its run ended: its synchronous part now, the rest on the
/// current Tokio runtime if there is one.
fn undo_late(entry: Box<dyn Entry>) {
    let Some(mut future) = start(entry) else { return };
    if let Some(result) = (&mut future).now_or_never() {
        return report(result);
    }
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => {
            runtime.spawn(async move { report(future.await) });
        }
        Err(_) => tracing::warn!(
            "an undo was added after its fiber ended, outside a Tokio runtime; the rest of it is skipped"
        ),
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
        let handle = undo.handle();
        handle.push(guard("first"));
        handle.push(guard("second"));
        assert!(handle.is_open());

        undo.run().await;
        assert!(!handle.is_open());
        assert_eq!(*log.lock().unwrap(), ["second", "first"]);
        handle.push(guard("late"));
        assert_eq!(*log.lock().unwrap(), ["second", "first", "late"]);
    }

    #[test]
    fn a_list_dropped_without_running_undoes_its_entries() {
        let (log, guard) = log();
        let undo = Undo::new();
        let handle = undo.handle();
        handle.push(guard("first"));
        handle.push(guard("second"));
        drop(undo);
        assert_eq!(*log.lock().unwrap(), ["second", "first"]);
        assert!(!handle.is_open());
        handle.push(guard("late"));
        assert_eq!(*log.lock().unwrap(), ["second", "first", "late"]);
    }

    #[tokio::test]
    async fn finished_tasks_do_not_pile_up() {
        let undo = Undo::new();
        for _ in 0..1000 {
            let task = tokio::spawn(async {});
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
            undo.handle().push(task);
        }
        let len = undo.0.lock().as_ref().unwrap().entries.len();
        assert!(len <= PRUNE_AT, "{len} entries kept");
    }

    #[test]
    fn a_late_entry_without_a_runtime_runs_its_synchronous_part() {
        let (log, guard) = log();
        let undo = Undo::new();
        undo.0.close().for_each(drop);
        undo.handle().push(guard("late"));
        assert_eq!(*log.lock().unwrap(), ["late"]);
    }
}
