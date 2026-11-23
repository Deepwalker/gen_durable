// -*- mode: Bluespec; -*-
//
// gen_durable — attempts, max_attempts and the failure paths of ONE row.
//
// How a run of a step can end, and what happens to `attempt`:
//   ok        the step returns an outcome            → committed (done here; progress)
//   error     `{:error, _}` (job form)               → FSM.__retry__: attempt+1 < max ? retry : failed
//   exit      the Task dies (`exit/1`, kill, a crash outside invoke's rescue — ISSUES #10
//             keeps a bare exit on the crash path deliberately) → row stays executing →
//             Reaper: attempt + 1, runnable, eligible_at UNCHANGED (no backoff)
//   nodeDown  the whole node dies (OOM, NIF segfault, a step outliving the VM) → the
//             restarted scheduler's reclaim_orphans: runnable, attempt NOT bumped
//
// max_attempts is only ever consulted on the `error` path. The property: user code of one
// step is started at most MAX_ATTEMPTS times before the row makes progress or terminates.
module retries {

  const MAX_ATTEMPTS: int
  const CAP_CRASHES: bool     // fix D1: the executor refuses to run a step whose attempt ≥ max
  const RECLAIM_BUMPS: bool   // fix D2: reclaim_orphans bumps attempt like the reaper

  type Status = Runnable | Executing | Done | Failed

  var status: Status
  var attempt: int
  var taskAlive: bool         // a Task is running the step (vs crashed, row awaiting recovery)
  var runs: int               // ghost: user-code starts of this step since the last progress
  var lastAction: str

  action init = all {
    status' = Runnable, attempt' = 0, taskAlive' = false, runs' = 0, lastAction' = "init",
  }

  // claim + Executor.run. With D1, a row already out of attempts is failed without running
  // user code (one guarded :stop commit).
  action claim = all {
    status == Runnable,
    if (CAP_CRASHES and attempt >= MAX_ATTEMPTS) all {
      status' = Failed, attempt' = attempt, taskAlive' = false, runs' = runs,
      lastAction' = "claim:out-of-attempts",
    } else all {
      status' = Executing, attempt' = attempt, taskAlive' = true, runs' = runs + 1,
      lastAction' = "claim+run",
    },
  }

  action stepOk = all {
    status == Executing, taskAlive,
    status' = Done, attempt' = attempt, taskAlive' = false, runs' = 0, lastAction' = "ok",
  }

  action stepError = all {
    status == Executing, taskAlive,
    val retry = attempt + 1 < MAX_ATTEMPTS
    all {
      status' = if (retry) Runnable else Failed,
      attempt' = if (retry) attempt + 1 else attempt,
      taskAlive' = false, runs' = runs,
      lastAction' = if (retry) "error:retry" else "error:failed",
    }
  }

  action taskExit = all {
    status == Executing, taskAlive,
    status' = status, attempt' = attempt, taskAlive' = false, runs' = runs, lastAction' = "exit",
  }

  // Reaper: the lease of the dead task expired.
  action reap = all {
    status == Executing, not(taskAlive),
    status' = Runnable, attempt' = attempt + 1, taskAlive' = false, runs' = runs,
    lastAction' = "reap",
  }

  // The node died with the task; its successor's startup reclaim releases the row early.
  action nodeDownReclaim = all {
    status == Executing,
    status' = Runnable, attempt' = if (RECLAIM_BUMPS) attempt + 1 else attempt,
    taskAlive' = false, runs' = runs, lastAction' = "node-down+reclaim",
  }

  action idle = all {
    status' = status, attempt' = attempt, taskAlive' = taskAlive, runs' = runs, lastAction' = lastAction,
  }

  action step = any { idle, claim, stepOk, stepError, taskExit, reap, nodeDownReclaim }

  // a poison pill is retried a bounded number of times
  val BoundedRetries = runs <= MAX_ATTEMPTS
  val AttemptSane = attempt >= 0
  val Safety = BoundedRetries and AttemptSane
}

module retries_shipped {
  import retries(MAX_ATTEMPTS = 3, CAP_CRASHES = false, RECLAIM_BUMPS = false).*
}
module retries_cap_only {      // D1 alone: the reclaim path still never bumps attempt
  import retries(MAX_ATTEMPTS = 3, CAP_CRASHES = true, RECLAIM_BUMPS = false).*
}
module retries_fixed {
  import retries(MAX_ATTEMPTS = 3, CAP_CRASHES = true, RECLAIM_BUMPS = true).*
}
