#!/usr/bin/env python3
"""Compact a TLC counterexample printed by `quint verify --backend=tlc` (read from stdin):
one line per state — the action, the bucket counters, every row, every task."""
import re
import sys

ABBR = {"status": "", "owner": "", "step": "s", "key": "k", "shard": "", "epoch": "e",
        "row": "r", "worker": "", "phase": "", "slot": "slot=", "credit": "owe="}


def untag(s):
    s = re.sub(r"\[(\w+) \|-> \[tag \|-> \"UNIT\"\]\]", r"\1", s)
    return re.sub(r"\s+", " ", s)


def records(s):
    out = []
    for body in re.findall(r"\[([^\[\]]*)\]", s):
        out.append(dict(re.findall(r"(\w+) \|-> (\"[^\"]*\"|[\w-]+)", body)))
    return out


def fmt_row(i, r):
    st = r.get("status", "?")[:4]
    owner = r.get("owner", '""').strip('"') or "-"
    key = r.get("key", '""').strip('"') or "∅"
    shard = r.get("shard", "?").lstrip("S")
    return f"r{i}[{st} {owner} s{r.get('step')} k:{key} {shard} e{r.get('epoch')}]"


def fmt_task(t):
    slot = t.get("slot", '""').strip('"')
    owe = t.get("credit", '""').strip('"')
    key = t.get("key", '""').strip('"') or "∅"
    extra = (f" slot:{slot}" if slot else "") + (f" owe:{owe}" if owe else "")
    return f"T(e{t.get('epoch')} r{t.get('row')} {t.get('worker','').strip(chr(34))} s{t.get('step')} k:{key} {t.get('phase','').lstrip('P')}{extra})"


text = sys.stdin.read()
parts = re.split(r"\nState (\d+):", text)
for i in range(1, len(parts), 2):
    n, body = parts[i], untag(parts[i + 1].split("\n\n")[0])

    def var(name):
        m = re.search(r"_" + name + r" = (.*?)(?= /\\ |$)", body)
        return m.group(1) if m else ""

    act = var("lastAction").strip('"')
    avail = var("avail")
    av = " ".join(f"{k}={v}" for k, v in re.findall(r"\"(\w+)\" :> (-?\d+)", avail)) or avail
    mem = var("members")
    mem_s = " ".join(f"{k}:{{" + ",".join(f"r{r}" + (f".e{e}" if e != "0" else "") for e, r in re.findall(r"epoch \|-> (\d+), row \|-> (\d+)", v) or [(e2, r2) for r2, e2 in re.findall(r"row \|-> (\d+), epoch \|-> (\d+)", v)]) + "}"
                     for k, v in re.findall(r'"(\w+)" :> (\{[^}]*\})', mem))
    if mem_s and "{}" not in mem_s.replace(":{}", ""):
        av = av + " zset " + mem_s
    elif mem_s:
        av = av + " zset " + mem_s
    rows = records(var("rows"))
    tasks = sorted(records(var("tasks")), key=lambda t: int(t.get("epoch", 0)))
    flags = ""
    if "_staleCommit = TRUE" in body:
        flags += " STALE-COMMIT"
    if "_stepRegress = TRUE" in body:
        flags += " STEP-REGRESS"
    print(f"{n:>2} {act:<18} avail[{av}] | "
          + " ".join(fmt_row(j + 1, r) for j, r in enumerate(rows))
          + " | " + " ".join(fmt_task(t) for t in tasks) + flags)
