#!/usr/bin/env bash
# The model-checking matrix: every experiment with its EXPECTED outcome. Exits non-zero if any
# result differs — so a fix that stops reproducing a bug, or a regression that breaks a fixed
# instance, both show up. Runs inside the formal container (`make formal`).
#
#   ./run.sh            the full matrix (TLC exhaustive + Apalache bounded)
#   ./run.sh tlc        TLC only
#   ./run.sh apalache   Apalache only (bounded + inductive)
#   JOB_TIMEOUT=secs    per-job cap (default 1200) — a job that hits it reports got=timeout
#
# Jobs run strictly one at a time: quint drives a single Apalache server (port 8822) for both
# backends, and TLC's heap is capped by tlc.json to leave room in the VM.
set -u
cd "$(dirname "$0")"
# Apalache's default 4G heap is not enough for the inductive checks (the server dies with
# "Connection dropped"); quint starts the server, apalache-mc reads JVM_ARGS.
export JVM_ARGS="${JVM_ARGS:--Xmx6g}"
want=${1:-all}
fail=0
log=out/run-$(date +%Y%m%d-%H%M%S).log
mkdir -p out

# file  instance  invariant  expect(ok|violation)  note
TLC=(
  "gen_durable.qnt shipped                     Safety      violation #1 PG gate: reconcile x post-commit credit = double credit"
  "gen_durable.qnt shipped_pauses              PauseSafety violation #2 ABA: same-incarnation zombie passes the ownership guard"
  "gen_durable.qnt shipped_pauses         NoStepRegression violation #2 ABA: ... and commits an outcome for a step the row already left"
  "gen_durable.qnt redis_shipped               Safety      violation #3 Redis gate: ZSET member = row id; a late ZREM evicts a newer holding"
  "gen_durable.qnt fix_credit_inline_pauses    PauseSafety violation fix A alone is not enough under pauses (needs the fence)"
  "gen_durable.qnt shipped_inline_norec        Safety      ok        inline run-ahead logic is sound once #1 is out of the way"
  "gen_durable.qnt fix_credit                  Safety      ok        fix A alone suffices while no lease expires under a live task"
  "gen_durable.qnt fixed                       PauseSafety ok        fixes A+B, pauses"
  "gen_durable.qnt fixed_inline                Safety      ok        fixes A+B, inline, crash-only"
  "gen_durable.qnt fixed_inline_pauses         PauseSafety ok        fixes A+B, inline, pauses"
  "gen_durable.qnt redis_fixed                 Safety      ok        fix C (member = claim token)"
  "gen_durable.qnt redis_fixed_inline          Safety      ok        fix C, inline"
  "gen_durable.qnt redis_fixed_pauses          PauseSafety ok        fix C, inline, pauses"
  "retries.qnt     retries_shipped             Safety      violation #4 crash path ignores max_attempts; node-down reclaim doesn't bump attempt"
  "retries.qnt     retries_cap_only            Safety      violation fix D1 alone: reclaim never bumps attempt"
  "retries.qnt     retries_fixed               Safety      ok        fixes D1+D2: a poison pill runs at most max_attempts times"
  "signals.qnt     signals_small               Safety      ok        no lost wakeup; signals consumed only when seen"
  "signals.qnt     signals_no_recheck          Safety      violation mutation check: drop the park recheck -> lost wakeup"
)
INDUCTIVE=(
  "gen_durable_proof.qnt gen_durable_proof          Safety ok        IndInv  fixes A+B: inductive => holds for traces of ANY length"
  "gen_durable_proof.qnt gen_durable_proof_shipped  Safety violation IndInv  mutation check: same invariant w/o fix A breaks at reconcile (#1)"
)
APALACHE=(
  "gen_durable.qnt shipped                     Safety      violation 10 #1, symbolically"
  "gen_durable.qnt fixed_inline                Safety      ok        6  fixes A+B, bounded (cost grows ~5x per 2 steps)"
  "retries.qnt     retries_shipped             Safety      violation 10 #4, symbolically"
  "retries.qnt     retries_fixed               Safety      ok        30 fixes D1+D2, bounded"
  "signals.qnt     signals_small               Safety      ok        10 signals, bounded"
  "signals.qnt     signals_no_recheck          Safety      violation 8  mutation check"
)

run() { # backend file inst inv expect [steps] -- note
  local backend=$1 file=$2 inst=$3 inv=$4 expect=$5 steps=${6:-}
  local args=(verify "$file" --main="$inst" --invariant="$inv")
  case "$backend" in
    tlc)       args+=(--backend=tlc --tlc-config=tlc.json) ;;
    inductive) args+=(--inductive-invariant="$steps") ;;
    *)         args+=(--max-steps="$steps") ;;
  esac
  local t0=$SECONDS out rc got
  out=$(timeout "${JOB_TIMEOUT:-1200}" quint "${args[@]}" 2>&1); rc=$?
  if   grep -q "No violation found" <<<"$out"; then got=ok
  elif grep -q "violation\]" <<<"$out";       then got=violation
  elif grep -q "reached a deadlock" <<<"$out"; then got=deadlock
  elif [ $rc -eq 124 ]; then got=timeout
  else got=error; fi
  local stats; stats=$( { grep -oE "^[0-9]+ states generated, [0-9]+ distinct states found" <<<"$out" | tail -1 | sed -E 's/.*, ([0-9]+) distinct.*/\1 states/'; grep -oE "depth of the complete state graph search is [0-9]+" <<<"$out" | tail -1 | sed -E 's/.* ([0-9]+)$/depth \1/'; } | tr '\n' ' ')
  local mark="PASS"; [ "$got" = "$expect" ] || { mark="FAIL"; fail=1; }
  printf '%-4s %-8s %-28s %-12s expect=%-9s got=%-9s %4ss %s\n' "$mark" "$backend" "$inst" "$inv" "$expect" "$got" $((SECONDS - t0)) "$stats" | tee -a "$log"
  { echo "----- $backend $file $inst $inv"; echo "$out"; } >> "$log.full"
  if [ "$got" = violation ] && [ "$backend" = tlc ]; then python3 scripts/trace.py <<<"$out" | sed 's/^/        /' | tee -a "$log"; fi
  return 0
}

if [ "$want" = all ] || [ "$want" = tlc ]; then
  for row in "${TLC[@]}"; do set -- $row; run tlc "$1" "$2" "$3" "$4"; done
fi
if [ "$want" = all ] || [ "$want" = apalache ] || [ "$want" = inductive ]; then
  for row in "${INDUCTIVE[@]}"; do set -- $row; run inductive "$1" "$2" "$3" "$4" "$5"; done
fi
if [ "$want" = all ] || [ "$want" = apalache ]; then
  for row in "${APALACHE[@]}"; do set -- $row; run apalache "$1" "$2" "$3" "$4" "$5"; done
fi
echo "log: formal/$log (full output: formal/$log.full)"
exit $fail
