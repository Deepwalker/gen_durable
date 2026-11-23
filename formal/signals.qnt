// -*- mode: Bluespec; -*-
//
// gen_durable — signals & await: the lost-wakeup protocol.
//
//   deliver   Queries @signal_sql_body — one statement: inbox INSERT + wake UPDATE whose flip
//             lives in a CASE (not the WHERE), so it always locks the row and re-evaluates
//             against the committed parked row.
//   claim     @claim_sql + enrich — the step's inbox snapshot is taken at PICK time.
//   park      Queries.flush :await — park + recheck in ONE transaction; the recheck flips the
//             row back to runnable if a matching signal the step did NOT see is in the inbox
//             (the per-row `presented` exclusion).
//   next      a progressing outcome — consumes exactly the awaited ids the step saw.
//   finish    a terminal outcome — consumes the whole inbox.
//   expire    expire_awaits — a timed-out park wakes (awaits kept).
//   reap      lease expiry of a crashed task.
//
// Statements are atomic actions (row locks serialize them; the park's transaction holds the
// row lock from the park UPDATE to commit, so a delivery either commits before the park or
// re-evaluates after it — both orders are actions here).
module signals {

  const SIGNALS: Set[int]     // signal ids (all carry the awaited name)
  const MAX_CLAIMS: int
  const RECHECK: bool         // the park's recheck (off = mutant: must lose a wakeup)

  type Status = Runnable | Executing | Awaiting | Done

  var status: Status
  var inbox: Set[int]         // committed, unconsumed signals
  var sent: Set[int]          // ghost: delivered so far
  var seen: Set[int]          // the running step's inbox snapshot (ctx.awaited)
  var parkedSeen: Set[int]    // ghost: what the step that parked had seen
  var consumed: Set[int]      // ghost: consumed by a committed outcome that saw them
  var dropped: Set[int]       // ghost: dropped unseen by a terminal outcome
  var claims: int
  var lastAction: str

  action init = all {
    status' = Runnable, inbox' = Set(), sent' = Set(), seen' = Set(), parkedSeen' = Set(),
    consumed' = Set(), dropped' = Set(), claims' = 0, lastAction' = "init",
  }

  action deliver(s: int): bool = all {
    not(sent.contains(s)),
    status != Done,                       // target CTE: status NOT IN ('done','failed')
    inbox' = inbox.union(Set(s)),
    sent' = sent.union(Set(s)),
    status' = if (status == Awaiting) Runnable else status,
    seen' = seen, parkedSeen' = parkedSeen, consumed' = consumed, dropped' = dropped,
    claims' = claims, lastAction' = "deliver",
  }

  action claim: bool = all {
    claims < MAX_CLAIMS,
    status == Runnable,
    status' = Executing,
    seen' = inbox,                        // enrich at pick
    inbox' = inbox, sent' = sent, parkedSeen' = parkedSeen, consumed' = consumed,
    dropped' = dropped, claims' = claims + 1, lastAction' = "claim",
  }

  // :await — park + recheck, one transaction
  action park: bool = all {
    status == Executing,
    val unseen = if (RECHECK) inbox.exclude(seen) else Set()
    all {
      status' = if (unseen != Set()) Runnable else Awaiting,
      parkedSeen' = seen,
      inbox' = inbox, sent' = sent, seen' = Set(), consumed' = consumed, dropped' = dropped,
      claims' = claims, lastAction' = if (unseen != Set()) "park+recheck-wake" else "park",
    }
  }

  // :next consuming exactly what the step saw
  action progress: bool = all {
    status == Executing,
    status' = Runnable,
    inbox' = inbox.exclude(seen),
    consumed' = consumed.union(seen),
    sent' = sent, seen' = Set(), parkedSeen' = parkedSeen, dropped' = dropped,
    claims' = claims, lastAction' = "next",
  }

  // :done — whole inbox consumed (the unseen part is dropped unseen, by contract)
  action finish: bool = all {
    status == Executing,
    status' = Done,
    inbox' = Set(),
    consumed' = consumed.union(seen),
    dropped' = dropped.union(inbox.exclude(seen)),
    sent' = sent, seen' = Set(), parkedSeen' = parkedSeen,
    claims' = claims, lastAction' = "finish",
  }

  action expire: bool = all {
    status == Awaiting,
    status' = Runnable,
    inbox' = inbox, sent' = sent, seen' = seen, parkedSeen' = parkedSeen, consumed' = consumed,
    dropped' = dropped, claims' = claims, lastAction' = "expire",
  }

  action reap: bool = all {
    status == Executing,
    status' = Runnable,
    seen' = Set(),
    inbox' = inbox, sent' = sent, parkedSeen' = parkedSeen, consumed' = consumed,
    dropped' = dropped, claims' = claims, lastAction' = "reap",
  }

  action idle = all {
    status' = status, inbox' = inbox, sent' = sent, seen' = seen, parkedSeen' = parkedSeen,
    consumed' = consumed, dropped' = dropped, claims' = claims, lastAction' = lastAction,
  }

  action step = any {
    idle,
    nondet s = SIGNALS.oneOf()
    deliver(s),
    claim, park, progress, finish, expire, reap,
  }

  // No lost wakeup: a parked row has no signal in its inbox that the parking step did not see.
  val NoLostWakeup = status == Awaiting implies inbox.subseteq(parkedSeen)
  // No signal is consumed by an outcome that did not see it, and none vanishes otherwise:
  // every delivered signal is in the inbox, consumed after being seen, or dropped by a
  // terminal outcome (the documented contract of :done/:stop).
  val NoSignalLost = sent == inbox.union(consumed).union(dropped)
  val ConsumedWereSeen = consumed.intersect(inbox) == Set()
  val Safety = NoLostWakeup and NoSignalLost and ConsumedWereSeen
}

module signals_small {
  import signals(SIGNALS = Set(1, 2, 3), MAX_CLAIMS = 6, RECHECK = true).*
}
// mutation check: without the recheck the model must find a lost wakeup
module signals_no_recheck {
  import signals(SIGNALS = Set(1, 2, 3), MAX_CLAIMS = 6, RECHECK = false).*
}
