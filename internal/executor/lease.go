package executor

// Lease protocol (implemented by scheduler on top of store.Store; this
// file documents the contract):
//
//   - A worker that picks a READY job performs one atomic Mutate:
//     READY -> RUNNING, LeaseOwner=<owner>, LeaseUntil=now+TTL.
//   - While the attempt runs, a renewer periodically extends LeaseUntil.
//   - Another worker can never claim a RUNNING job with an unexpired
//     lease, which prevents duplicate execution across concurrent workers
//     and across processes.
//   - If the owning process dies, renewals stop. The recoverer moves
//     expired RUNNING jobs back to READY, so no job is ever lost.
//   - When the attempt finishes (or the job is canceled/timed out at the
//     scheduler level), one terminal transition is committed. A late
//     attempt from an evicted owner is rejected by an owner+status guard
//     in that transition, so a retried-but-still-running old attempt can
//     never overwrite the new outcome.
//
// Together this gives at-least-once delivery with at-most-one concurrent
// execution (effectively exactly-once state transition); tasks with side
// effects outside the process should still be idempotent, as a crash after
// the process acted but before the terminal commit will cause a re-run.
