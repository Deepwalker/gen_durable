// -*- mode: Bluespec; -*-
//
// gen_durable — the claim / out-of-band admission / group-commit flush / credit / reaper /
// reconcile protocol for CONFIGURED concurrency gates, including inline run-ahead
// (inline_execution) with key changes between steps.
//
// Mapping to lib/ (details in formal/README.md):
//   claim          Queries @claim_sql            (runnable → executing, provisional shard)
//   admitOk/Deny   Limiter.Postgres @admit_sql   (debit + UNGUARDED stamp) / release_claims
//   flush          Queries.flush/2               (guarded: locked_by = worker ∧ executing)
//   credit         Limiter.Postgres @credit_sql  (LEAST(capacity, available + n)), run by the
//                  Flusher AFTER the flush commits / by Executor.finish_inline
//   inline*        Executor.attempt_inline / finish_inline / admit_inline
//   reap           Queries.reap/1                (does NOT clear concurrency_shard)
//   reconcile      Queries.reconcile_concurrency (available = cap − held(executing ∧ shard))
//
// Abstraction:
//   * One SQL statement / transaction = one atomic action. Row locks + SKIP LOCKED only
//     serialize statements, so atomic statements are sound for these safety properties.
//   * A worker id is a SCHEDULER INCARNATION (Scheduler.init: prefix <> unique_integer),
//     not a per-claim token — the crux of the ABA finding.
//   * One shard per key (shards only spread contention; they add no new interleavings to the
//     properties below). Every key is its own bucket of CAP.
//   * Time is abstract: a lease expires when its task crashed, or — with PAUSES — at any
//     moment (BEAM pause / VM suspend / partition > lease_ttl, incarnation survives).
//   * Ghost variables (epoch, claims, staleCommit, stepRegress, lastAction) are model-only.
module gen_durable {

  // ---- parameters --------------------------------------------------------------------
  const ROWS: Set[int]
  const WORKERS: Set[str]
  const KEYS: Set[str]      // configured gate keys, each its own bucket of CAP
  const INIT_KEY: str       // every row starts on this key ("" = no key)
  const CAP: int
  const MAX_STEP: int       // a row is done after MAX_STEP committed steps
  const MAX_CLAIMS: int     // bound on total claims (finite state space)
  const PAUSES: bool        // a live task's lease may expire
  const INLINE: bool        // inline_execution continuations enabled
  const RECONCILE: bool     // the GC reconciler runs (off: isolate the other mechanisms)
  const TRACE: bool         // keep the lastAction label (readable traces; ~multiplies states)
  const BACKEND: str        // "pg" (gen_durable_buckets counters) | "redis" (lease ZSET members)
  // fixes under test
  const CREDIT_IN_TXN: bool // credit freed slots inside the committing flush transaction
  const FENCED: bool        // per-claim token in the ownership guard (flush, admit, release)

  // ---- types -------------------------------------------------------------------------
  type Status = Runnable | Executing | Done
  type Shard = SNone | SProv | SReal       // NULL | provisional 0 (claim/continue) | stamped
  type Phase =
    | PAdmit          // claimed, waiting for Limiter.admit
    | PRun            // admitted (or ungated): user step running
    | PFlush          // outcome built, waiting for the flush
    | PCredit         // committed; post-commit credit of `credit` pending, then done
    | PInlineAdmit    // continue committed (row still executing); admit for the new key
    | PInlineCredit   // inline: credit of the old slot pending, then run the next step
    | PRequeue        // inline admit denied: apply_outcome (:next requeue flush) pending

  // Redis slot member (gd:conc:<key> ZSET). Shipped: the row id; FENCED: the claim token.
  type Member = { row: int, epoch: int }

  type Row = { status: Status, owner: str, step: int, key: str, shard: Shard, epoch: int }

  type Task = {
    row: int,
    worker: str,
    epoch: int,     // the claim this task works under
    step: int,      // the step it is executing
    key: str,       // the key it is admitted on / being admitted on
    phase: Phase,
    slot: str,      // key of the gate slot it holds ("" = none) — job.slot
    credit: str,    // key of a slot credit it still owes ("" = none)
  }

  // ---- state -------------------------------------------------------------------------
  var rows: int -> Row
  var tasks: Set[Task]
  var avail: str -> int     // gen_durable_buckets.available per key          (pg)
  var members: str -> Set[Member] // gd:conc:<key> holders, unexpired         (redis)
  var claims: int           // ghost: claim counter = epoch generator
  var staleCommit: bool     // ghost: an outcome of a claim that is not the row's claim committed
  var stepRegress: bool     // ghost: an outcome for a step that is not the row's step committed
  var lastAction: str       // ghost: trace readability

  // ---- helpers -----------------------------------------------------------------------
  def lbl(a: str): str = if (TRACE) a else ""
  pure def max(a: int, b: int): int = if (a > b) a else b
  pure def min(a: int, b: int): int = if (a < b) a else b

  // credit one slot of key k (LEAST(capacity, available + 1)); "" = nothing
  pure def creditOne(av: str -> int, k: str): str -> int =
    if (k == "") av else av.set(k, min(CAP, av.get(k) + 1))

  // reconcile's notion of "holding a slot of k"
  def heldInDb(k: str): Set[int] =
    ROWS.filter(r => rows.get(r).status == Executing and rows.get(r).key == k and rows.get(r).shard != SNone)

  def stampedExecuting(k: str): Set[int] =
    ROWS.filter(r => rows.get(r).status == Executing and rows.get(r).key == k and rows.get(r).shard == SReal)

  // user steps running right now under a slot of k
  def runningOn(k: str): Set[Task] =
    tasks.filter(t => t.slot == k and (t.phase == PRun or t.phase == PFlush))

  // the ownership guard: locked_by = $worker AND status = 'executing' (+ claim token if FENCED)
  def owns(t: Task): bool = {
    val row = rows.get(t.row)
    row.status == Executing and row.owner == t.worker and (not(FENCED) or row.epoch == t.epoch)
  }

  def isPg: bool = BACKEND == "pg"
  // credit inside the flush transaction — only possible when the counter lives in the same DB
  def inTxn: bool = CREDIT_IN_TXN and isPg
  pure def memberOf(row: int, epoch: int): Member = { row: row, epoch: if (FENCED) epoch else 0 }
  def mOf(t: Task): Member = memberOf(t.row, t.epoch)
  def freeSlots(k: str): int = if (isPg) avail.get(k) else CAP - members.get(k).size()

  def replace(t: Task, t2: Task): Set[Task] = tasks.exclude(Set(t)).union(Set(t2))
  def drop(t: Task): Set[Task] = tasks.exclude(Set(t))

  // ---- slot accounting, per backend ------------------------------------------------------
  //   pg:    take = available − 1 (+ stamp the shard on the row); give = LEAST(cap, available + 1)
  //   redis: take = ZADD member (idempotent for an existing member!); give = ZREM member
  def takeAvail(k: str): str -> int = if (isPg) avail.set(k, avail.get(k) - 1) else avail
  def takeMembers(k: str, m: Member): str -> Set[Member] =
    if (isPg) members else members.set(k, members.get(k).union(Set(m)))
  def giveAvail(k: str): str -> int = if (isPg) creditOne(avail, k) else avail
  def giveMembers(k: str, m: Member): str -> Set[Member] =
    if (isPg or k == "") members else members.set(k, members.get(k).exclude(Set(m)))
  // The PG admit stamps the drawn shard on the row; Redis does not touch the row. Shipped:
  // the stamp is unguarded. FENCED: the stamp UPDATE carries `locked_by = token AND
  // executing`, so it lands only on the task's own claim. The admission math itself is NOT
  // fenced (joining it to gen_durable collapses the planner's row estimates — PERFORMANCE.md):
  // an admit for a claim that was taken back still debits, unstamped — a leak in the safe
  // direction (the slot is held by nobody the reconciler counts; it heals it).
  def stampFor(t: Task): int -> Row =
    if (isPg and (not(FENCED) or owns(t))) rows.set(t.row, { ...rows.get(t.row), shard: SReal })
    else rows

  // ---- actions -----------------------------------------------------------------------

  action init = all {
    rows' = ROWS.mapBy(_ => { status: Runnable, owner: "", step: 0, key: INIT_KEY, shard: SNone, epoch: 0 }),
    tasks' = Set(),
    avail' = KEYS.mapBy(_ => CAP),
    members' = KEYS.mapBy(_ => Set()),
    claims' = 0,
    staleCommit' = false,
    stepRegress' = false,
    lastAction' = lbl("init"),
  }

  // @claim_sql. A gated key gets the provisional shard and goes to admission; an ungated
  // row runs straight away.
  action claim(w: str, r: int): bool = all {
    claims < MAX_CLAIMS,
    val row = rows.get(r)
    val e = claims + 1
    val gated = row.key != ""
    all {
      row.status == Runnable,
      rows' = rows.set(r, { ...row, status: Executing, owner: w, epoch: e,
                            shard: if (gated) SProv else SNone }),
      tasks' = tasks.union(Set({ row: r, worker: w, epoch: e, step: row.step, key: row.key,
                                 phase: if (gated) PAdmit else PRun, slot: "", credit: "" })),
      avail' = avail, members' = members,
      claims' = e,
      staleCommit' = staleCommit, stepRegress' = stepRegress,
      lastAction' = lbl("claim"),
    }
  }

  // Limiter.admit on the pick path.
  action admitOk(t: Task): bool = all {
    t.phase == PAdmit,
    freeSlots(t.key) > 0,
    rows' = stampFor(t),
    tasks' = replace(t, { ...t, phase: PRun, slot: t.key }),
    avail' = takeAvail(t.key),
    members' = takeMembers(t.key, mOf(t)),
    claims' = claims,
    staleCommit' = staleCommit, stepRegress' = stepRegress,
    lastAction' = lbl("admitOk"),
  }

  // denied → release_claims (guarded) back to runnable, shard NULL
  action admitDeny(t: Task): bool = all {
    t.phase == PAdmit,
    freeSlots(t.key) <= 0,
    rows' = if (owns(t)) rows.set(t.row, { ...rows.get(t.row), status: Runnable, owner: "", shard: SNone })
            else rows,
    tasks' = drop(t),
    avail' = avail, members' = members,
    claims' = claims,
    staleCommit' = staleCommit, stepRegress' = stepRegress,
    lastAction' = lbl("admitDeny"),
  }

  action execute(t: Task): bool = all {
    t.phase == PRun,
    rows' = rows,
    tasks' = replace(t, { ...t, phase: PFlush }),
    avail' = avail, members' = members,
    claims' = claims,
    staleCommit' = staleCommit, stepRegress' = stepRegress,
    lastAction' = lbl("execute"),
  }

  // The step's outcome commits through the flush. `nk` = the next step's concurrency_key
  // ("" = release), `inl` = run it inline (continue entry, keep_lock) instead of requeuing.
  //   done / :next requeue — row leaves executing, its slot is freed (credited after commit,
  //                          or inside the transaction with CREDIT_IN_TXN on pg).
  //   inline, same key     — keep everything, run the next step (no admission, no credit).
  //   inline, new key/none — continue commit (row stays executing; provisional shard for a
  //                          new configured key); the new key is admitted out-of-band, the
  //                          old slot credited after that admit (Executor.finish_inline).
  action flush(t: Task, nk: str, inl: bool): bool = all {
    t.phase == PFlush,
    val row = rows.get(t.row)
    val nstep = t.step + 1
    val done = nstep >= MAX_STEP
    val stale2 = (staleCommit or row.epoch != t.epoch)
    val regress2 = (stepRegress or row.step != t.step)
    if (not(owns(t))) all {
      rows' = rows, tasks' = drop(t), avail' = avail, members' = members, claims' = claims,
      staleCommit' = staleCommit, stepRegress' = stepRegress, lastAction' = lbl("flush:stale"),
    } else if (done or not(INLINE) or not(inl)) all {
      // terminal or requeue: leave executing
      rows' = rows.set(t.row, { ...row, status: if (done) Done else Runnable, owner: "",
                                step: nstep, key: if (done) row.key else nk, shard: SNone }),
      tasks' = if (t.slot != "" and not(inTxn)) replace(t, { ...t, phase: PCredit, credit: t.slot })
               else drop(t),
      avail' = if (inTxn) giveAvail(t.slot) else avail,
      members' = members,
      claims' = claims,
      staleCommit' = stale2, stepRegress' = regress2,
      lastAction' = lbl(if (done) "flush:done" else "flush:next"),
    } else if (nk == t.key) all {
      // inline, :keep — same key, same shard, same slot
      rows' = rows.set(t.row, { ...row, step: nstep }),
      tasks' = replace(t, { ...t, phase: PRun, step: nstep }),
      avail' = avail, members' = members, claims' = claims,
      staleCommit' = stale2, stepRegress' = regress2,
      lastAction' = lbl("flush:inline-keep"),
    } else all {
      // inline continue to a new key (or none): row stays executing under the same claim
      rows' = rows.set(t.row, { ...row, step: nstep, key: nk,
                                shard: if (nk == "") SNone else SProv }),
      // The old slot is returned by THIS commit: inside the transaction (fix A, pg) or right
      // after it, BEFORE the new key's admit (Executor since 0.2.16: the continue entry
      // carries the old slot). Then the new key is admitted, if any.
      tasks' = replace(t, { ...t, step: nstep, key: nk, slot: "",
                            phase: if (t.slot != "" and not(inTxn)) PInlineCredit
                                   else if (nk == "") PRun else PInlineAdmit,
                            credit: if (inTxn) "" else t.slot }),
      avail' = if (inTxn) giveAvail(t.slot) else avail,
      members' = members,
      claims' = claims,
      staleCommit' = stale2, stepRegress' = regress2,
      lastAction' = lbl("flush:inline-rekey"),
    }
  }

  // finish_inline → admit_inline for the configured new key (the old slot is already back).
  action inlineAdmitOk(t: Task): bool = all {
    t.phase == PInlineAdmit,
    freeSlots(t.key) > 0,
    rows' = stampFor(t),
    tasks' = replace(t, { ...t, slot: t.key, phase: PRun }),
    avail' = takeAvail(t.key),
    members' = takeMembers(t.key, mOf(t)),
    claims' = claims,
    staleCommit' = staleCommit, stepRegress' = stepRegress,
    lastAction' = lbl("inlineAdmitOk"),
  }

  // Denied: apply_outcome — a :next requeue flush of the same outcome; the old slot was
  // already returned by the continue commit, so the requeue credits nothing.
  action inlineAdmitDeny(t: Task): bool = all {
    t.phase == PInlineAdmit,
    freeSlots(t.key) <= 0,
    rows' = rows,
    tasks' = replace(t, { ...t, phase: PRequeue }),
    avail' = avail, members' = members,
    claims' = claims,
    staleCommit' = staleCommit, stepRegress' = stepRegress,
    lastAction' = lbl("inlineAdmitDeny"),
  }

  action requeue(t: Task): bool = all {
    t.phase == PRequeue,
    val row = rows.get(t.row)
    if (owns(t)) all {
      rows' = rows.set(t.row, { ...row, status: Runnable, owner: "", shard: SNone }),
      tasks' = if (t.credit != "") replace(t, { ...t, phase: PCredit, slot: "" }) else drop(t),
      avail' = avail, members' = members, claims' = claims,
      staleCommit' = (staleCommit or row.epoch != t.epoch),
      stepRegress' = stepRegress,
      lastAction' = lbl("requeue"),
    } else all {
      rows' = rows, tasks' = drop(t), avail' = avail, members' = members, claims' = claims,
      staleCommit' = staleCommit, stepRegress' = stepRegress, lastAction' = lbl("requeue:stale"),
    }
  }

  // An owed credit lands (pg: LEAST(capacity, available + 1); redis: ZREM member).
  action credit(t: Task): bool = all {
    t.phase == PCredit or t.phase == PInlineCredit,
    rows' = rows,
    // PInlineCredit: the continue's old slot is back; admit the new key next (if any)
    tasks' = if (t.phase == PCredit) drop(t)
             else replace(t, { ...t, phase: if (t.key == "") PRun else PInlineAdmit, credit: "" }),
    avail' = giveAvail(t.credit),
    members' = giveMembers(t.credit, mOf(t)),
    claims' = claims,
    staleCommit' = staleCommit, stepRegress' = stepRegress,
    lastAction' = lbl("credit"),
  }

  action crash(t: Task): bool = all {
    rows' = rows, tasks' = drop(t), avail' = avail, members' = members, claims' = claims,
    staleCommit' = staleCommit, stepRegress' = stepRegress, lastAction' = lbl("crash"),
  }

  // Queries.reap/1 — concurrency_shard NOT cleared.
  action reap(r: int): bool = all {
    val row = rows.get(r)
    all {
      row.status == Executing,
      PAUSES or not(tasks.exists(t => t.row == r and t.epoch == row.epoch)),
      rows' = rows.set(r, { ...row, status: Runnable, owner: "" }),
      tasks' = tasks, avail' = avail, members' = members, claims' = claims,
      staleCommit' = staleCommit, stepRegress' = stepRegress, lastAction' = lbl("reap"),
    }
  }

  // pg: reconcile_concurrency — per key, available = GREATEST(0, cap − held).
  action reconcile: bool = all {
    RECONCILE, isPg,
    rows' = rows, tasks' = tasks,
    avail' = KEYS.mapBy(k => max(0, CAP - heldInDb(k).size())),
    members' = members, claims' = claims,
    staleCommit' = staleCommit, stepRegress' = stepRegress, lastAction' = lbl("reconcile"),
  }

  // redis: a member whose lease is no longer renewed is pruned by the next admit
  // (ZREMRANGEBYSCORE … now). Renewed while a live task holds or still owes it — unless PAUSES.
  action prune(k: str, m: Member): bool = all {
    not(isPg),
    members.get(k).contains(m),
    PAUSES or not(tasks.exists(t => mOf(t) == m and (t.slot == k or t.credit == k))),
    rows' = rows, tasks' = tasks, avail' = avail,
    members' = members.set(k, members.get(k).exclude(Set(m))),
    claims' = claims,
    staleCommit' = staleCommit, stepRegress' = stepRegress, lastAction' = lbl("prune"),
  }

  // explicit stutter: terminal / exhausted states are not deadlocks (Apalache checks those)
  action idle: bool = all {
    rows' = rows, tasks' = tasks, avail' = avail, members' = members, claims' = claims,
    staleCommit' = staleCommit, stepRegress' = stepRegress, lastAction' = lbl("idle"),
  }

  action step = any {
    idle,
    nondet w = WORKERS.oneOf()
    nondet r = ROWS.oneOf()
    claim(w, r),
    nondet r = ROWS.oneOf()
    reap(r),
    reconcile,
    all {
      not(isPg),
      nondet k = KEYS.oneOf()
      all {
        members.get(k) != Set(),
        nondet m = members.get(k).oneOf()
        prune(k, m),
      },
    },
    all {
      tasks != Set(),
      nondet t = tasks.oneOf()
      nondet nk = KEYS.union(Set("")).oneOf()
      nondet inl = Set(true, false).oneOf()
      any {
        admitOk(t), admitDeny(t), execute(t), flush(t, nk, inl),
        inlineAdmitOk(t), inlineAdmitDeny(t), requeue(t), credit(t), crash(t),
      },
    },
  }

  // ---- properties --------------------------------------------------------------------

  // the gate's promise: at most CAP admitted steps of a key run at once (crash-only world)
  val GateCap = KEYS.forall(k => runningOn(k).size() <= CAP)
  // DB level (pg): at most CAP executing rows hold a stamped slot of a key
  val GateCapDb = not(isPg) or KEYS.forall(k => stampedExecuting(k).size() <= CAP)
  // the ownership guard admits only the current claim's outcome
  val NoStaleCommit = not(staleCommit)
  // ... and never an outcome for a step the row has already moved past
  val NoStepRegression = not(stepRegress)
  val AvailInRange = not(isPg) or KEYS.forall(k => avail.get(k) >= 0 and avail.get(k) <= CAP)

  val Safety = GateCap and GateCapDb and NoStaleCommit and NoStepRegression and AvailInRange
  // with pauses zombies run by design; the DB and the commit protocol must still be sound
  val PauseSafety = GateCapDb and NoStaleCommit and NoStepRegression and AvailInRange
}

// ---- experiments -----------------------------------------------------------------------
// small: 2 rows, 2 incarnations, 1 key (pick path only)  |  inline: 2 keys + run-ahead

module shipped {
  import gen_durable(ROWS = Set(1, 2), WORKERS = Set("w1", "w2"), KEYS = Set("a"), INIT_KEY = "a",
    CAP = 1, MAX_STEP = 2, MAX_CLAIMS = 4, RECONCILE = true, TRACE = true, BACKEND = "pg", PAUSES = false, INLINE = false,
    CREDIT_IN_TXN = false, FENCED = false).*
}
module shipped_pauses {
  import gen_durable(ROWS = Set(1, 2), WORKERS = Set("w1", "w2"), KEYS = Set("a"), INIT_KEY = "a",
    CAP = 1, MAX_STEP = 3, MAX_CLAIMS = 4, RECONCILE = true, TRACE = true, BACKEND = "pg", PAUSES = true, INLINE = false,
    CREDIT_IN_TXN = false, FENCED = false).*
}
module shipped_inline {
  import gen_durable(ROWS = Set(1, 2), WORKERS = Set("w1"), KEYS = Set("a", "b"), INIT_KEY = "a",
    CAP = 1, MAX_STEP = 3, MAX_CLAIMS = 3, RECONCILE = true, TRACE = true, BACKEND = "pg", PAUSES = false, INLINE = true,
    CREDIT_IN_TXN = false, FENCED = false).*
}
module fixed_inline {
  import gen_durable(ROWS = Set(1, 2), WORKERS = Set("w1"), KEYS = Set("a", "b"), INIT_KEY = "a",
    CAP = 1, MAX_STEP = 3, MAX_CLAIMS = 3, RECONCILE = true, TRACE = false, BACKEND = "pg", PAUSES = false, INLINE = true,
    CREDIT_IN_TXN = true, FENCED = true).*
}
module fixed {
  import gen_durable(ROWS = Set(1, 2), WORKERS = Set("w1", "w2"), KEYS = Set("a"), INIT_KEY = "a",
    CAP = 1, MAX_STEP = 3, MAX_CLAIMS = 4, RECONCILE = true, TRACE = false, BACKEND = "pg", PAUSES = true, INLINE = false,
    CREDIT_IN_TXN = true, FENCED = true).*
}
module fixed_inline_pauses {
  import gen_durable(ROWS = Set(1, 2), WORKERS = Set("w1"), KEYS = Set("a", "b"), INIT_KEY = "a",
    CAP = 1, MAX_STEP = 3, MAX_CLAIMS = 3, RECONCILE = true, TRACE = false, BACKEND = "pg", PAUSES = true, INLINE = true,
    CREDIT_IN_TXN = true, FENCED = true).*
}
module fixed_big {
  import gen_durable(ROWS = Set(1, 2, 3), WORKERS = Set("w1", "w2"), KEYS = Set("a", "b"), INIT_KEY = "a",
    CAP = 2, MAX_STEP = 3, MAX_CLAIMS = 5, RECONCILE = true, TRACE = false, BACKEND = "pg", PAUSES = false, INLINE = true,
    CREDIT_IN_TXN = true, FENCED = true).*
}

// the inline path with the reconciler off: does anything ELSE break?
module shipped_inline_norec {
  import gen_durable(ROWS = Set(1, 2), WORKERS = Set("w1", "w2"), KEYS = Set("a", "b"), INIT_KEY = "a",
    CAP = 1, MAX_STEP = 3, MAX_CLAIMS = 4, RECONCILE = false, TRACE = false, BACKEND = "pg", PAUSES = false, INLINE = true,
    CREDIT_IN_TXN = false, FENCED = false).*
}
module shipped_inline_norec_pauses {
  import gen_durable(ROWS = Set(1, 2), WORKERS = Set("w1", "w2"), KEYS = Set("a", "b"), INIT_KEY = "a",
    CAP = 1, MAX_STEP = 3, MAX_CLAIMS = 4, RECONCILE = false, TRACE = true, BACKEND = "pg", PAUSES = true, INLINE = true,
    CREDIT_IN_TXN = false, FENCED = false).*
}
// fix 1 alone under pauses: is the fence really needed for the DB invariants, or only for commits?
module fix_credit_inline_pauses {
  import gen_durable(ROWS = Set(1, 2), WORKERS = Set("w1", "w2"), KEYS = Set("a", "b"), INIT_KEY = "a",
    CAP = 1, MAX_STEP = 3, MAX_CLAIMS = 4, RECONCILE = true, TRACE = true, BACKEND = "pg", PAUSES = true, INLINE = true,
    CREDIT_IN_TXN = true, FENCED = false).*
}

// ---- Redis limiter backend (lease-scored ZSET; members = row ids, shipped) -------------
module redis_shipped {
  import gen_durable(ROWS = Set(1, 2, 3), WORKERS = Set("w1"), KEYS = Set("a"), INIT_KEY = "a",
    CAP = 2, MAX_STEP = 2, MAX_CLAIMS = 4, RECONCILE = false, TRACE = true, BACKEND = "redis", PAUSES = false,
    INLINE = false, CREDIT_IN_TXN = false, FENCED = false).*
}
module redis_fixed {     // members = claim tokens
  import gen_durable(ROWS = Set(1, 2, 3), WORKERS = Set("w1"), KEYS = Set("a"), INIT_KEY = "a",
    CAP = 2, MAX_STEP = 2, MAX_CLAIMS = 5, RECONCILE = false, TRACE = false, BACKEND = "redis", PAUSES = false,
    INLINE = false, CREDIT_IN_TXN = false, FENCED = true).*
}
module redis_fixed_inline {
  import gen_durable(ROWS = Set(1, 2), WORKERS = Set("w1"), KEYS = Set("a", "b"), INIT_KEY = "a",
    CAP = 1, MAX_STEP = 3, MAX_CLAIMS = 4, RECONCILE = false, TRACE = false, BACKEND = "redis", PAUSES = false,
    INLINE = true, CREDIT_IN_TXN = false, FENCED = true).*
}
module redis_fixed_pauses {
  import gen_durable(ROWS = Set(1, 2), WORKERS = Set("w1"), KEYS = Set("a", "b"), INIT_KEY = "a",
    CAP = 1, MAX_STEP = 3, MAX_CLAIMS = 3, RECONCILE = false, TRACE = false, BACKEND = "redis", PAUSES = true,
    INLINE = true, CREDIT_IN_TXN = false, FENCED = true).*
}

// fix A alone, crash-only, with inline: enough when no lease expires under a live task
module fix_credit {
  import gen_durable(ROWS = Set(1, 2), WORKERS = Set("w1", "w2"), KEYS = Set("a", "b"), INIT_KEY = "a",
    CAP = 1, MAX_STEP = 3, MAX_CLAIMS = 4, RECONCILE = true, TRACE = false, BACKEND = "pg", PAUSES = false,
    INLINE = true, CREDIT_IN_TXN = true, FENCED = false).*
}
