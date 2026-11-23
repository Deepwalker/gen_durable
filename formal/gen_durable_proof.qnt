// -*- mode: Bluespec; -*-
//
// Inductive invariant for the FIXED protocol (fixes A + B; pg backend; crash-only; pick path,
// no inline). Checked by Apalache with `quint verify --inductive-invariant=IndInv`:
//   [1] init ⇒ IndInv      [2] IndInv ∧ step ⇒ IndInv'      [3] IndInv ⇒ Safety
// Unlike TLC's exhaustive search (all states reachable within MAX_CLAIMS claims), [2] is a
// one-step argument: it holds for traces of ANY length, and it scales to larger scopes.
//
// Why the gate holds (the argument IndInv encodes):
//   * every live task owns its row under its own claim token (crash-only: the reaper only takes
//     a row whose claim has no live task; the fence rejects any other claim's commit);
//   * a task running under a slot of k ⇔ its row is executing with a stamped shard of k;
//   * the counter never over-promises: available(k) + |stamped executing rows of k| ≤ CAP —
//     admit moves one unit from `available` to a stamp, the in-transaction credit moves it back
//     atomically with the row leaving `executing`, a crashed holder or a provisional row only
//     ever makes `available` smaller, and reconcile recomputes it from a superset of the stamps.
module gen_durable_proof {
  import gen_durable(ROWS = Set(1, 2), WORKERS = Set("w1"), KEYS = Set("a"), INIT_KEY = "a",
    CAP = 1, MAX_STEP = 2, MAX_CLAIMS = 3, RECONCILE = true, TRACE = true, BACKEND = "pg", PAUSES = false,
    INLINE = false, CREDIT_IN_TXN = true, FENCED = true).* from "./gen_durable"

  val OptKeys = KEYS.union(Set(""))
  val RowSpace =
    tuples(Set(Runnable, Executing, Done), WORKERS.union(Set("")), 0.to(MAX_STEP), OptKeys,
           Set(SNone, SProv, SReal), 0.to(MAX_CLAIMS))
      .map(p => { status: p._1, owner: p._2, step: p._3, key: p._4, shard: p._5, epoch: p._6 })
  val TaskSpace =
    tuples(ROWS, WORKERS, 1.to(MAX_CLAIMS), 0.to(MAX_STEP - 1), OptKeys, Set(PAdmit, PRun, PFlush), OptKeys)
      .map(p => { row: p._1, worker: p._2, epoch: p._3, step: p._4, key: p._5, phase: p._6, slot: p._7, credit: "" })
  val Labels = Set("init", "claim", "admitOk", "admitDeny", "execute", "flush:stale", "flush:done",
                   "flush:next", "flush:inline-keep", "flush:inline-rekey", "inlineAdmitOk",
                   "inlineAdmitDeny", "requeue", "requeue:stale", "credit", "crash", "reap",
                   "reconcile", "prune", "idle")

  // the shape of a state (doubles as Apalache's generator for phase [2])
  val Shape = and {
    rows.in(setOfMaps(ROWS, RowSpace)),
    tasks.in(powerset(TaskSpace)),
    avail.in(setOfMaps(KEYS, 0.to(CAP))),
    members.in(Set(KEYS.mapBy(_ => Set()))),
    claims.in(0.to(MAX_CLAIMS)),
    staleCommit.in(Set(false)),
    stepRegress.in(Set(false)),
    lastAction.in(Labels),
  }

  val RowsWellFormed = ROWS.forall(r => and {
    val row = rows.get(r)
    and {
      row.epoch <= claims,
      (row.status == Executing) == (row.owner != ""),
      row.status != Done implies row.step < MAX_STEP,
    }
  })

  // each claim spawns one task; a live task is the current claim of its row
  val TasksBound = tasks.forall(t => and {
    val row = rows.get(t.row)
    and {
      t.epoch <= claims,
      tasks.forall(u => u.epoch == t.epoch implies u == t),
      row.status == Executing, row.owner == t.worker, row.epoch == t.epoch,
      row.step == t.step, row.key == t.key,
    }
  })

  // phase ↔ shard ↔ slot
  val PhasesConsistent = tasks.forall(t => and {
    val sh = rows.get(t.row).shard
    and {
      t.phase == PAdmit implies (t.slot == "" and t.key != "" and sh == SProv),
      (t.phase == PRun or t.phase == PFlush) implies
        ((t.slot == "" and t.key == "" and sh == SNone) or (t.slot == t.key and t.key != "" and sh == SReal)),
    }
  })

  // the counter never over-promises
  val CounterSound = KEYS.forall(k => avail.get(k) + stampedExecuting(k).size() <= CAP)

  // the reachable-state part (cheap to evaluate; what the simulator / TLC check)
  val IndInvCore = and { RowsWellFormed, TasksBound, PhasesConsistent, CounterSound,
                         not(staleCommit), not(stepRegress) }
  val IndInv = and { Shape, IndInvCore }
}

// mutation check: the same invariant over the SHIPPED credit path (no fix A) must NOT be inductive
module gen_durable_proof_shipped {
  import gen_durable(ROWS = Set(1, 2), WORKERS = Set("w1"), KEYS = Set("a"), INIT_KEY = "a",
    CAP = 1, MAX_STEP = 2, MAX_CLAIMS = 3, RECONCILE = true, TRACE = true, BACKEND = "pg", PAUSES = false,
    INLINE = false, CREDIT_IN_TXN = false, FENCED = true).* from "./gen_durable"

  val OptKeys = KEYS.union(Set(""))
  val RowSpace =
    tuples(Set(Runnable, Executing, Done), WORKERS.union(Set("")), 0.to(MAX_STEP), OptKeys,
           Set(SNone, SProv, SReal), 0.to(MAX_CLAIMS))
      .map(p => { status: p._1, owner: p._2, step: p._3, key: p._4, shard: p._5, epoch: p._6 })
  val TaskSpace =
    tuples(ROWS, WORKERS, 1.to(MAX_CLAIMS), 0.to(MAX_STEP - 1), OptKeys, Set(PAdmit, PRun, PFlush), OptKeys)
      .map(p => { row: p._1, worker: p._2, epoch: p._3, step: p._4, key: p._5, phase: p._6, slot: p._7, credit: "" })
      .union(tuples(ROWS, WORKERS, 1.to(MAX_CLAIMS), 0.to(MAX_STEP - 1), KEYS)
      .map(p => { row: p._1, worker: p._2, epoch: p._3, step: p._4, key: p._5, phase: PCredit, slot: p._5, credit: p._5 }))
  val Labels = Set("init", "claim", "admitOk", "admitDeny", "execute", "flush:stale", "flush:done",
                   "flush:next", "flush:inline-keep", "flush:inline-rekey", "inlineAdmitOk",
                   "inlineAdmitDeny", "requeue", "requeue:stale", "credit", "crash", "reap",
                   "reconcile", "prune", "idle")

  // the shape of a state (doubles as Apalache's generator for phase [2])
  val Shape = and {
    rows.in(setOfMaps(ROWS, RowSpace)),
    tasks.in(powerset(TaskSpace)),
    avail.in(setOfMaps(KEYS, 0.to(CAP))),
    members.in(Set(KEYS.mapBy(_ => Set()))),
    claims.in(0.to(MAX_CLAIMS)),
    staleCommit.in(Set(false)),
    stepRegress.in(Set(false)),
    lastAction.in(Labels),
  }

  val RowsWellFormed = ROWS.forall(r => and {
    val row = rows.get(r)
    and {
      row.epoch <= claims,
      (row.status == Executing) == (row.owner != ""),
      row.status != Done implies row.step < MAX_STEP,
    }
  })

  // each claim spawns one task; a live task is the current claim of its row
  val EpochsUnique = tasks.forall(t => t.epoch <= claims and tasks.forall(u => u.epoch == t.epoch implies u == t))
  // a task owing a credit has left its row: the row is no longer under that claim
  val OwedDetached = tasks.filter(t => t.phase == PCredit).forall(t => rows.get(t.row).epoch >= t.epoch)
  val TasksBound = tasks.filter(t => t.phase != PCredit).forall(t => and {
    val row = rows.get(t.row)
    and {
      t.epoch <= claims,
      tasks.forall(u => u.epoch == t.epoch implies u == t),
      row.status == Executing, row.owner == t.worker, row.epoch == t.epoch,
      row.step == t.step, row.key == t.key,
    }
  })

  // phase ↔ shard ↔ slot
  val PhasesConsistent = tasks.filter(t => t.phase != PCredit).forall(t => and {
    val sh = rows.get(t.row).shard
    and {
      t.phase == PAdmit implies (t.slot == "" and t.key != "" and sh == SProv),
      (t.phase == PRun or t.phase == PFlush) implies
        ((t.slot == "" and t.key == "" and sh == SNone) or (t.slot == t.key and t.key != "" and sh == SReal)),
    }
  })

  // the counter never over-promises
  // without fix A a freed slot is "in flight" until its credit lands: count the debts
  val CounterSound = KEYS.forall(k =>
    avail.get(k) + stampedExecuting(k).size() + tasks.filter(t => t.phase == PCredit and t.credit == k).size() <= CAP)

  // the reachable-state part (cheap to evaluate; what the simulator / TLC check)
  val IndInvCore = and { RowsWellFormed, EpochsUnique, OwedDetached, TasksBound, PhasesConsistent, CounterSound,
                         not(staleCommit), not(stepRegress) }
  val IndInv = and { Shape, IndInvCore }
}
