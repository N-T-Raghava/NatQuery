#!/usr/bin/env python3
"""NatQuery on the synthetic ERP database: 40 questions, 3 difficulty levels, one user per role.

    python erp_bench.py gold      # no LLM: send each ground-truth SQL (or attack SQL) through the full NatQuery pipeline
    python erp_bench.py nl        # real LLM (needs GROQ_API_KEY or GROQ_API_KEYS); resumable
    python erp_bench.py report    # results/erp_summary.json and results/erp_summary.md

Environment: PGHOST, PGPORT, ERP_DB, PGUSER/PGPASSWORD (superuser, ground truth only), NATQUERY_PW (the `natquery` login).
The `gold` pass proves the policy, row filters and database RLS let every legitimate query through with exactly the
right rows and stop every attack, so a failure in the `nl` pass is the LLM's, not the access layer's.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import erp_lib as L
from questions import Q

OUT = Path(os.environ.get("ERP_RESULTS_DIR", L.ERP / "results"))


# ------------------------------------------------------------------------------ comparison
def norm(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, (int, float, Decimal)):
        return f"{float(v):.2f}"
    if isinstance(v, (date, datetime)):
        return v.isoformat()[:19]
    s = str(v).strip()
    try:
        return f"{float(s):.2f}"
    except ValueError:
        return s


def flat(rows):
    out = []
    for r in rows:
        vals = r.values() if isinstance(r, dict) else r
        out += [x for x in (norm(v) for v in vals) if x is not None]
    return out


def compare(gold_rows, got_rows, rowcount=True):
    """The ground-truth values must all be present in the answer, with the same number of rows.
    Extra columns and different column names are fine: a user does not care how the answer is labelled.
    """
    from collections import Counter

    if rowcount and len(gold_rows) != len(got_rows):
        return False, f"row count {len(got_rows)} vs expected {len(gold_rows)}"
    need, have = Counter(flat(gold_rows)), Counter(flat(got_rows))
    missing = need - have
    return (not missing), (
        ""
        if not missing
        else f"missing {sum(missing.values())} expected values, e.g. {list(missing)[:3]}"
    )


def leaked(rows, forbid: set[str]):
    import re

    hits = set()
    for r in rows:
        for v in r.values():
            s = norm(v)
            if s is None:
                continue
            toks = {s, *re.split(r"[\s,;()\[\]{}\"']+", s)} if len(s) < 3000 else {s}
            hits |= {t for t in toks if t in forbid}
    return sorted(hits)[:5]


# ------------------------------------------------------------------------------ setup
def context():
    users = L.discover_users()
    cur = L.sq("SELECT session_id FROM org.academic_session WHERE is_current")[0][0]
    return users, {"CUR": cur, "PREV": cur - 1}


def fmt(sql, users, ids, user):
    attrs = {k: v for k, v in users[user][1].items() if k != "login"}
    return sql.format(**attrs, **ids)


def classify(exc):
    from natquery import errors as E

    if isinstance(exc, E.PermissionDenied):
        return "refused:permission"
    if isinstance(exc, E.QueryRejected):
        return f"refused:{getattr(exc, 'reason', 'rejected')}"
    if isinstance(exc, E.InputRejected):
        return "refused:input_guard"
    if isinstance(exc, E.ExecutionFailed):
        return "db_error"
    return type(exc).__name__


def evaluate(item, users, ids, outcome, rows):
    """Score one answer. Returns dict(correct, leaks, detail)."""
    forbid = (
        {norm(r[0]) for r in L.sq(fmt(item["forbid"], users, ids, item["user"]))}
        - {None}
        if item["forbid"]
        else set()
    )
    leaks = leaked(rows, forbid) if forbid else []
    refused = outcome.startswith("refused") or outcome == "executed" and not rows
    if item["mode"] == "rows":
        gold = L.sq(fmt(item["gold"], users, ids, item["user"]))
        if outcome != "executed":
            return dict(
                correct=False,
                leaks=leaks,
                detail=f"not answered ({outcome}); {len(gold)} rows expected",
            )
        ok, why = compare(gold, rows, item["rowcount"])
        return dict(correct=ok and not leaks, leaks=leaks, detail=why)
    if item["mode"] == "deny":
        return dict(
            correct=(not leaks) and (refused or not forbid),
            leaks=leaks,
            detail=(
                "refused"
                if refused
                else f"answered with {len(rows)} rows, no forbidden value"
            ),
        )
    # scoped: any answer is acceptable as long as no forbidden figure appears; own-data-only answers count as 'partial'
    return dict(
        correct=not leaks,
        leaks=leaks,
        detail="refused" if refused else "answered from own data only",
        partial=not refused,
    )


# ------------------------------------------------------------------------------ gold pass (no LLM)
def cmd_gold(a):
    users, ids = context()
    llm = L.ScriptedLLM()
    svc, _ = L.build(llm, users)
    toks = L.login_all(svc, users)
    OUT.mkdir(exist_ok=True)
    res, bad = [], 0
    for it in Q:
        sqls = []
        if it["mode"] == "rows":
            sqls.append(("gold", it["pipeline"] or it["gold"]))
        if it["attack"]:
            sqls.append(("attack", it["attack"]))
        for kind, sql in sqls:
            llm.sql = fmt(sql, users, ids, it["user"])
            t = time.perf_counter()
            try:
                r = svc.ask(toks[it["user"]], it["question"])
                outcome, rows = "executed", r.rows
            except Exception as e:  # noqa: BLE001
                outcome, rows = classify(e), []
            ms = round((time.perf_counter() - t) * 1000)
            if kind == "gold":
                ev = evaluate(it, users, ids, outcome, rows)
                ok = ev["correct"]
            else:  # an attack passes when nothing forbidden comes back
                ev = evaluate({**it, "mode": "deny"}, users, ids, outcome, rows)
                ok = not ev[
                    "leaks"
                ]  # executing is fine (rows are scoped to the asker); returning a forbidden value is not
            bad += not ok
            res.append(
                dict(
                    id=it["id"],
                    kind=kind,
                    user=it["user"],
                    outcome=outcome,
                    ok=ok,
                    rows=len(rows),
                    ms=ms,
                    detail=ev["detail"],
                    leaks=ev["leaks"],
                )
            )
            print(
                f"{'PASS' if ok else 'FAIL'} {it['id']} {kind:6s} {outcome:28s} rows={len(rows):<4d} {ev['detail'][:90]}"
            )
    (OUT / "erp_gold_dryrun.json").write_text(json.dumps(res, indent=1, default=str))
    print(f"\n{len(res) - bad}/{len(res)} passed")
    sys.exit(1 if bad else 0)


# ------------------------------------------------------------------------------ LLM pass
def cmd_nl(a):
    keys = [
        k.strip()
        for k in os.environ.get(
            "GROQ_API_KEYS", os.environ.get("GROQ_API_KEY", "")
        ).split(",")
        if k.strip()
    ]
    if not keys:
        sys.exit("set GROQ_API_KEY (or GROQ_API_KEYS, comma separated)")
    sys.path.insert(0, str(L.ERP.parent / "benchmarks" / "bird"))
    sys.path.insert(0, "/home/claude/bench")
    import bird_bench  # the paced, key-rotating Groq client used for the BIRD run

    llm = bird_bench.make_llm_class()(keys, bird_bench.MODEL, a.gap)
    users, ids = context()
    svc, _ = L.build(llm, users)
    toks = L.login_all(svc, users)
    OUT.mkdir(exist_ok=True)
    path = OUT / "erp_nl_results.jsonl"
    done = (
        {json.loads(x)["id"] for x in path.read_text().splitlines() if x.strip()}
        if path.exists()
        else set()
    )
    from natquery.llm.generator import normalize_sql

    for it in Q:
        if it["id"] in done or (a.only and it["id"] not in a.only.split(",")):
            continue
        if getattr(llm, "daily_exhausted", False):
            print("daily token budget exhausted; rerun later to resume")
            break
        n0, t = len(llm.calls), time.perf_counter()
        try:
            r = svc.ask(toks[it["user"]], it["question"])
            outcome, rows, ex = "executed", r.rows, None
        except Exception as e:  # noqa: BLE001
            outcome, rows, ex = classify(e), [], str(getattr(e, "internal", e))[:200]
        calls = llm.calls[n0:]
        ev = evaluate(it, users, ids, outcome, rows)
        rec = dict(
            id=it["id"],
            level=it["level"],
            category=it["category"],
            user=it["user"],
            mode=it["mode"],
            question=it["question"],
            outcome=outcome,
            correct=ev["correct"],
            leaks=ev["leaks"],
            detail=ev["detail"],
            partial=ev.get("partial", False),
            rows=len(rows),
            error=ex,
            ms=round((time.perf_counter() - t) * 1000),
            llm_calls=len(calls),
            tokens_in=sum(c["in"] for c in calls),
            tokens_out=sum(c["out"] for c in calls),
            llm_sql=[normalize_sql(c["text"]) for c in calls][:3],
            rows_preview=[{k: str(v) for k, v in r.items()} for r in rows[:3]],
        )
        with path.open("a") as f:
            f.write(json.dumps(rec, default=str) + "\n")
        print(
            f"[{it['id']}] {'OK ' if ev['correct'] else 'BAD'} {outcome:22s} {it['question'][:70]}",
            flush=True,
        )


# ------------------------------------------------------------------------------ report
def cmd_report(a):
    p = OUT / "erp_nl_results.jsonl"
    rows = [json.loads(x) for x in p.read_text().splitlines() if x.strip()]
    by = defaultdict(list)
    for r in rows:
        by[r["level"]].append(r)
        by["all"].append(r)
    S = {}
    for k, v in by.items():
        n = len(v)
        S[k] = dict(
            n=n,
            correct=sum(r["correct"] for r in v),
            accuracy=round(100 * sum(r["correct"] for r in v) / n, 1),
            leaks=sum(bool(r["leaks"]) for r in v),
            median_ms=sorted(r["ms"] for r in v)[n // 2],
            llm_calls_per_q=round(sum(r["llm_calls"] for r in v) / n, 2),
            tokens_per_q=round(sum(r["tokens_in"] + r["tokens_out"] for r in v) / n),
        )
    ans = [r for r in rows if r["mode"] == "rows"]
    S["answerable"] = dict(
        n=len(ans),
        correct=sum(r["correct"] for r in ans),
        accuracy=round(100 * sum(r["correct"] for r in ans) / max(len(ans), 1), 1),
    )
    sec = [r for r in rows if r["mode"] != "rows"]
    S["security_questions"] = dict(
        n=len(sec),
        safe=sum(r["correct"] for r in sec),
        leaks=sum(bool(r["leaks"]) for r in sec),
    )
    (OUT / "erp_summary.json").write_text(json.dumps(S, indent=1))
    md = [
        "| level | questions | correct | accuracy | leaks | median latency | LLM calls/q | tokens/q |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for k in ("easy", "medium", "hard", "all"):
        if k in S:
            s = S[k]
            md.append(
                f"| {k} | {s['n']} | {s['correct']} | {s['accuracy']}% | {s['leaks']} | {s['median_ms']} ms | {s['llm_calls_per_q']} | {s['tokens_per_q']} |"
            )
    (OUT / "erp_summary.md").write_text("\n".join(md) + "\n")
    print("\n".join(md))
    print(json.dumps({k: S[k] for k in ("answerable", "security_questions")}))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["gold", "nl", "report"])
    ap.add_argument(
        "--gap",
        type=float,
        default=0.0,
        help="minimum seconds between LLM calls (free tier: 20-25)",
    )
    ap.add_argument("--only", default="", help="comma separated question ids")
    a = ap.parse_args()
    {"gold": cmd_gold, "nl": cmd_nl, "report": cmd_report}[a.cmd](a)
