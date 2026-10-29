//! Deterministic fault injection (ADR-0014, required before step 6b): a
//! test arms named points, each with a count; the n-th time the store reaches
//! that point, the operation stops there with an injected I/O error, as if
//! the process died right after the steps before it. Everything before the
//! point has happened; nothing after it. Disarmed, a point costs one
//! thread-local read. One store per thread (compio's model), so the armed
//! points belong to the test's thread.

use std::cell::RefCell;
use std::io;

/// Where a fault can be injected.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultPoint {
    /// A checkpoint wrote and synced its object table.
    CheckpointObjectTable,
    /// ... its relocation run.
    CheckpointRelocationRun,
    /// ... its block table.
    CheckpointBlockTable,
    /// ... its block relocation run.
    CheckpointBlockRun,
    /// ... its id run.
    CheckpointIdRun,
    /// ... its dictionary file.
    CheckpointDictionary,
    /// It synced the directory after its new files (before reading them
    /// back).
    CheckpointFilesSynced,
    /// It wrote and synced the tmp manifest.
    CheckpointManifestTmp,
    /// It renamed the tmp manifest (before the directory sync).
    CheckpointRenamed,
    /// It synced the directory after the rename.
    CheckpointRenameSynced,
    /// Not a stop: the manifest's read-back is treated as a mismatch, so
    /// the write-time quarantine runs.
    CheckpointReadBackMismatch,
    /// It deleted the previous manifest (before the directory sync).
    CheckpointPreviousDeleted,
    /// It synced the directory after deleting the previous manifest.
    CheckpointPreviousDeleteSynced,
    /// It deleted the id runs a merge superseded and synced the directory.
    CheckpointMergedRunsDeleted,
    /// A quarantine created its directory (before syncing the store
    /// directory).
    QuarantineDirCreated,
    /// A quarantine moved one file (counted across files).
    QuarantineMoved,
    /// A quarantine synced both directories after a group of moves.
    QuarantineGroupSynced,
    /// Open made every change and is about to return its report.
    OpenBeforeReport,
}

thread_local! {
    static ARMED: RefCell<Vec<(FaultPoint, u32)>> = const { RefCell::new(Vec::new()) };
}

/// Arms `point` to fire the `nth` time (from 1) it is reached on this
/// thread; other armed points stay armed (a point is armed once).
#[doc(hidden)]
pub fn arm(point: FaultPoint, nth: u32) {
    ARMED.with(|a| {
        let mut a = a.borrow_mut();
        a.retain(|(p, _)| *p != point);
        a.push((point, nth.max(1)));
    });
}

/// Disarms every point; returns whether any armed point had not fired.
#[doc(hidden)]
pub fn disarm() -> bool {
    ARMED.with(|a| {
        let mut a = a.borrow_mut();
        let unfired = !a.is_empty();
        a.clear();
        unfired
    })
}

/// Whether `point` is armed and this is its n-th arrival (disarming it
/// then).
pub(crate) fn hit(point: FaultPoint) -> bool {
    ARMED.with(|a| {
        let mut a = a.borrow_mut();
        let Some(i) = a.iter().position(|(p, _)| *p == point) else {
            return false;
        };
        if a[i].1 == 1 {
            a.remove(i);
            true
        } else {
            a[i].1 -= 1;
            false
        }
    })
}

/// `Err` with an injected I/O error at the armed point's n-th arrival.
pub(crate) fn check(point: FaultPoint) -> io::Result<()> {
    if hit(point) {
        return Err(io::Error::other(format!("injected fault at {point:?}")));
    }
    Ok(())
}
