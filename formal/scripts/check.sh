#!/usr/bin/env bash
# Usage: scripts/check.sh <instance> <invariant> [tlc|apalache] [max-steps]
# Runs one model-checking job inside the formal container; prints a compact trace on violation.
set -u
inst=$1; inv=$2; backend=${3:-tlc}; steps=${4:-20}
out=$(mktemp)
if [ "$backend" = tlc ]; then
  quint verify gen_durable.qnt --main="$inst" --invariant="$inv" --backend=tlc > "$out" 2>&1
else
  quint verify gen_durable.qnt --main="$inst" --invariant="$inv" --max-steps="$steps" > "$out" 2>&1
fi
rc=$?
grep -E "states generated|depth of the complete|No violation|violation\]|outcome|error:|Error" "$out" | grep -v "^ *$" | tail -6
if grep -q "violation\]" "$out" && [ "$backend" = tlc ]; then python3 scripts/trace.py < "$out"; fi
if grep -q "violation\]" "$out" && [ "$backend" != tlc ]; then sed -n '/^\[State 0\]/,$p' "$out" | head -400; fi
rm -f "$out"; exit $rc
