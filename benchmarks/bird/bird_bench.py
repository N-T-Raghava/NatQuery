#!/usr/bin/env python3
"""BIRD Mini-Dev benchmark for NatQuery (PostgreSQL version of the dataset).

    python bird_bench.py run    [--limit N] [--gap SECONDS]    # resumable; appends to results/results.jsonl
    python bird_bench.py report                                # results/summary.json, results.csv

For every question three things are measured, all from ONE set of LLM calls:

  raw       the first SQL the LLM wrote, executed directly. No NatQuery security layer.
  enforced  that same first SQL passed through NatQuery's enforcer, then executed.
            raw - enforced = accuracy the security layer costs.
  natquery  the full pipeline (enforcer + bounded LLM repair loop), exactly as a user would get it.

Scoring is BIRD's execution accuracy: the predicted rows must equal the gold rows as a set.
Secrets come from environment variables, never from this file:  GROQ_API_KEY, PGPASSWORD (optional).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import sys
import tempfile
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import psycopg2

HERE = Path(__file__).resolve().parent
sys.path.insert(
    0, str(HERE.parents[1] / "src")
)  # repo checkout; an installed natquery also works

DATA = Path(
    os.environ.get("BIRD_DATA", HERE / "data" / "MINIDEV")
)  # folder holding mini_dev_postgresql.json
OUT = Path(
    os.environ.get("NQ_RESULTS_DIR", HERE / "results")
)  # shipped results live here
RESULTS = OUT / "results.jsonl"

PG = dict(
    host=os.environ.get("PGHOST", "127.0.0.1"),
    port=int(os.environ.get("PGPORT", "54330")),
    dbname=os.environ.get("PGDATABASE", "bird"),
    user=os.environ.get("PGUSER", "postgres"),
    password=os.environ.get("PGPASSWORD", ""),
)
MODEL = os.environ.get("NQ_BENCH_MODEL", "openai/gpt-oss-120b")
# USD per million tokens (Groq published price for gpt-oss-120b, checked 2026-10-28)
PRICE_IN, PRICE_OUT = 0.15, 0.60
GOLD_TIMEOUT_MS = 60_000
OUT_BUDGET = 1200  # output cap per call; observed outputs (reasoning included) peak well below this
TPM_BUDGET = 7900  # Groq cap is 8000 tokens/minute for this model on this account

POLICY_YAML = """
version: 1
defaults: {max_rows: 100000, statement_timeout_ms: 60000, max_joins: 12}
roles:
  analyst:
    priority: 10
    description: Benchmark role. Reads every table; no row filters (isolates SQL quality from access control).
    permissions: {can_view_sql: true}
    tables:
      "*": {columns: "*"}
"""


# ----------------------------------------------------------------------------- scoring
def _norm(v):
    if isinstance(v, Decimal):
        v = float(v)
    if isinstance(v, float):
        return round(v, 6)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, memoryview):
        return bytes(v)
    if isinstance(v, (list, tuple)):
        return tuple(_norm(x) for x in v)
    if isinstance(v, dict):
        return json.dumps(v, sort_keys=True, default=str)
    return v


def run_plain(conn, sql: str):
    """Execute on a plain (tuple) cursor in a read-only txn. Returns (rows|None, ms, error|None)."""
    t = time.perf_counter()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = {GOLD_TIMEOUT_MS}")
            cur.execute(sql)
            rows = [tuple(_norm(x) for x in r) for r in cur.fetchall()]
        return rows, (time.perf_counter() - t) * 1000, None
    except Exception as e:
        return (
            None,
            (time.perf_counter() - t) * 1000,
            f"{type(e).__name__}: {str(e)[:160]}",
        )
    finally:
        conn.rollback()


def same(pred, gold) -> bool:
    return pred is not None and gold is not None and set(pred) == set(gold)


# ----------------------------------------------------------------------------- LLM
def make_llm_class():
    from groq import Groq, RateLimitError

    from natquery.errors import LLMUnavailable
    from natquery.llm.groq_provider import GroqClient

    class BenchGroq(GroqClient):
        """The library's own GroqClient call (same model, temperature, max_tokens), plus pacing,
        rate-limit backoff and token/latency recording."""

        def __init__(self, api_keys: list[str], model: str, gap: float):
            super().__init__(
                {"api_key": api_keys[0], "model": model, "min_tokens": OUT_BUDGET}
            )
            self._keys, self._ki = api_keys, 0
            self._client = Groq(api_key=api_keys[0], timeout=90.0, max_retries=0)
            self.daily_exhausted = False
            self.gap, self._last = gap, 0.0
            self.calls: list[dict] = []
            self._window: list[tuple[float, int]] = (
                []
            )  # (time, tokens) over the last 60 s
            self._est_prompt = 1900  # refined from history

        def complete(self, messages, *, temperature=0.0, max_tokens=800) -> str:
            for attempt in range(8):
                wait = self.gap - (time.time() - self._last)
                if wait > 0:
                    time.sleep(wait)
                # stay under Groq's tokens-per-minute cap (8000 on this account) instead of
                # hitting 429s: wait until the last 60 s of usage plus one more call fits
                while True:
                    now = time.time()
                    self._window = [(t, n) for t, n in self._window if now - t < 60]
                    need = self._est_prompt + max(max_tokens, self._min_tokens)
                    if (
                        not self._window
                        or sum(n for _, n in self._window) + need <= TPM_BUDGET
                    ):
                        break
                    time.sleep(max(1.0, 60 - (now - self._window[0][0]) + 0.5))
                self._last = time.time()
                t = time.perf_counter()
                try:
                    extra = {"reasoning_effort": self._effort} if self._effort else {}
                    r = self._client.chat.completions.create(
                        model=self._model,
                        messages=messages,
                        temperature=temperature,
                        max_tokens=max(max_tokens, self._min_tokens),
                        **extra,
                    )
                except RateLimitError as e:
                    if "tokens per day" in str(e) or "requests per day" in str(e):
                        # this key's daily allowance is gone: move to the next key, or stop cleanly
                        if self._ki + 1 < len(self._keys):
                            self._ki += 1
                            self._client = Groq(
                                api_key=self._keys[self._ki],
                                timeout=90.0,
                                max_retries=0,
                            )
                            self._window.clear()
                            print(
                                f"    daily limit reached, switching to key #{self._ki + 1}",
                                flush=True,
                            )
                            continue
                        self.daily_exhausted = True
                        raise LLMUnavailable(internal="DAILY_LIMIT") from e
                    delay = min(60, 5 * 2**attempt)
                    print(
                        f"    rate limited ({str(e)[:140]}), sleeping {delay}s",
                        flush=True,
                    )
                    time.sleep(delay)
                    self._last = time.time()
                    last = e
                    continue
                except Exception as e:
                    raise LLMUnavailable(
                        internal=f"{type(e).__name__}: {str(e)[:200]}"
                    ) from e
                u = r.usage
                # Groq counts prompt + the requested max_tokens against the per-minute cap
                self._window.append(
                    (time.time(), u.prompt_tokens + max(max_tokens, self._min_tokens))
                )
                self._est_prompt = int(
                    0.7 * self._est_prompt + 0.3 * u.prompt_tokens * 1.1
                )
                self.calls.append(
                    {
                        "ms": (time.perf_counter() - t) * 1000,
                        "in": u.prompt_tokens,
                        "out": u.completion_tokens,
                        "finish": r.choices[0].finish_reason,
                        "text": r.choices[0].message.content or "",
                    }
                )
                return r.choices[0].message.content or ""
            raise LLMUnavailable(internal=f"rate limited after retries: {last}")

    return BenchGroq


# ----------------------------------------------------------------------------- run
def load_questions():
    """The 500 Mini-Dev PostgreSQL questions."""
    return json.loads((DATA / "mini_dev_postgresql.json").read_text())


def build_services(llm, tmp: Path):
    from natquery.access.policy import load_policy
    from natquery.audit.log import AuditLog
    from natquery.auth import hashing
    from natquery.auth.local import LocalPasswordProvider
    from natquery.auth.session import SessionManager
    from natquery.auth.user_store import UserStore
    from natquery.backends.postgres import PostgresBackend
    from natquery.llm.generator import SQLGenerator
    from natquery.orchestration.pipeline import QueryService

    backend = PostgresBackend({"type": "params", **PG}, max_conn=4)
    full = backend.extract_schema()
    policy_path = tmp / "policy.yaml"
    policy_path.write_text(POLICY_YAML)
    policy = load_policy(policy_path)

    store = UserStore(tmp / "auth.db")
    provider = LocalPasswordProvider({"lock_threshold": 5}, store=store)
    pw = "Bench-Passw0rd!9"
    store.add("bench", hashing.hash_password(pw), ["analyst"], {})
    sec = {
        "rate_limit_per_minute": 100000,
        "llm_calls_per_hour": 1000000,
        "max_repair_attempts": 2,
        "enable_optimizer": False,
        "log_question_text": True,
        "session_idle_minutes": 600,
        "session_max_hours": 24,
    }
    sessions = SessionManager(provider, idle_seconds=36000, max_seconds=86400)
    audit = AuditLog(tmp / "audit.log")

    tables_json = json.loads((DATA / "dev_tables.json").read_text())
    services = {}
    for db in tables_json:
        names = {t.lower() for t in db["table_names_original"]}
        sub = {"tables": {n: m for n, m in full["tables"].items() if n in names}}
        svc = QueryService(
            backend=backend,
            policy=policy,
            schema=sub,
            generator=SQLGenerator(llm),
            audit=audit,
            sessions=sessions,
            security=sec,
            db_name=db["db_id"],
        )
        session = svc.login({"username": "bench", "password": pw})
        services[db["db_id"]] = (svc, session)
    return backend, services


def instrument_enforcer(svc, bucket: dict):
    """Record time spent in the enforcer (all calls for the current question)."""
    original = svc.enforcer.enforce

    def timed(*a, **kw):
        t = time.perf_counter()
        try:
            return original(*a, **kw)
        finally:
            bucket["enforcer_ms"] = (
                bucket.get("enforcer_ms", 0.0) + (time.perf_counter() - t) * 1000
            )

    svc.enforcer.enforce = timed
    return original


def cmd_run(args) -> None:
    from natquery.errors import NatQueryError
    from natquery.llm.generator import is_unanswerable, normalize_sql

    keys = [
        k.strip()
        for k in os.environ.get(
            "GROQ_API_KEYS", os.environ.get("GROQ_API_KEY", "")
        ).split(",")
        if k.strip()
    ]
    if not keys:
        sys.exit("set GROQ_API_KEYS (comma separated) or GROQ_API_KEY")
    OUT.mkdir(exist_ok=True)
    done = set()
    if RESULTS.exists():
        done = {
            json.loads(line)["question_id"]
            for line in RESULTS.read_text().splitlines()
            if line.strip()
        }

    questions = load_questions()
    if args.limit:
        questions = questions[: args.limit]
    todo = [q for q in questions if q["question_id"] not in done]
    print(f"{len(done)} done, {len(todo)} to run", flush=True)

    llm = make_llm_class()(keys, MODEL, args.gap)
    tmp = Path(tempfile.mkdtemp(prefix="nqbench_"))
    backend, services = build_services(llm, tmp)
    conn = psycopg2.connect(**PG)
    conn.set_session(readonly=True, autocommit=False)

    with RESULTS.open("a") as out:
        for i, q in enumerate(todo, 1):
            svc, session = services[q["db_id"]]
            text = q["question"] + (
                f"\nHint: {q['evidence']}" if q.get("evidence") else ""
            )
            rec = {k: q[k] for k in ("question_id", "db_id", "difficulty", "question")}
            rec["gold_sql"] = q["SQL"]

            gold, gold_ms, gold_err = run_plain(conn, q["SQL"])
            rec.update(
                gold_ms=gold_ms,
                gold_error=gold_err,
                gold_rows=None if gold is None else len(gold),
            )

            bucket: dict = {}
            original = instrument_enforcer(svc, bucket)
            llm.calls.clear()
            t0 = time.perf_counter()
            outcome, final_sql, resp, err = "ok", None, None, None
            try:
                resp = svc.ask(session.token, text)
                final_sql = resp.generated_sql
            except NatQueryError as e:
                outcome, err = (
                    "refused"
                    if e.reason in {"unanswerable"} or "denied" in (e.reason or "")
                    else "failed"
                ), f"{e.reason}"
            except Exception as e:  # harness must never die on one question
                outcome, err = "failed", f"{type(e).__name__}: {str(e)[:120]}"
            total_ms = (time.perf_counter() - t0) * 1000
            svc.enforcer.enforce = original
            if llm.daily_exhausted:
                print(
                    "All API keys hit their daily token limit. Results so far are saved; re-run to resume.",
                    flush=True,
                )
                break

            calls = list(llm.calls)
            first_sql = normalize_sql(calls[0]["text"]) if calls else None
            rec.update(
                outcome=outcome,
                error_reason=err,
                llm_calls=len(calls),
                repairs=max(0, len(calls) - 1),
                first_sql=first_sql,
                tokens_in=sum(c["in"] for c in calls),
                tokens_out=sum(c["out"] for c in calls),
                truncated_output=sum(1 for c in calls if c["finish"] == "length"),
                cost_usd=sum(c["in"] for c in calls) * PRICE_IN / 1e6
                + sum(c["out"] for c in calls) * PRICE_OUT / 1e6,
                llm_ms=sum(c["ms"] for c in calls),
                enforcer_ms=bucket.get("enforcer_ms", 0.0),
                db_ms=resp.execution_time_ms if resp else None,
                total_ms=total_ms,
                final_sql=final_sql,
                natquery_truncated=bool(resp and resp.truncated),
            )
            rec["natquery_ok"] = (
                bool(resp) and same(run_plain(conn, final_sql)[0], gold)
                if resp and gold is not None
                else False
            )

            # --- raw: first LLM SQL, no security layer
            if first_sql and not is_unanswerable(first_sql):
                raw_rows, raw_ms, raw_err = run_plain(conn, first_sql)
                rec.update(
                    raw_ms=raw_ms, raw_error=raw_err, raw_ok=same(raw_rows, gold)
                )
            else:
                rec.update(raw_ms=None, raw_error="unanswerable_or_empty", raw_ok=False)

            # --- enforced: first LLM SQL through the enforcer only (no repair)
            rec.update(enforced_ok=False, enforcer_verdict=None)
            if first_sql and not is_unanswerable(first_sql):
                try:
                    view = svc._view(svc.policy.pick_role(session.identity, None))
                    role = svc.policy.pick_role(session.identity, None)
                    t = time.perf_counter()
                    enf = svc.enforcer.enforce(first_sql, session.identity, role, view)
                    rec["enforcer_first_ms"] = (time.perf_counter() - t) * 1000
                    rows, _, _ = run_plain(conn, enf.sql)
                    rec.update(
                        enforcer_verdict="accepted", enforced_ok=same(rows, gold)
                    )
                except NatQueryError as e:
                    rec["enforcer_verdict"] = f"rejected:{e.reason}"
                except Exception as e:
                    rec["enforcer_verdict"] = f"error:{type(e).__name__}"

            out.write(json.dumps(rec) + "\n")
            out.flush()
            print(
                f"[{i}/{len(todo)}] q{rec['question_id']} {q['db_id']:<24} raw={int(rec['raw_ok'])} "
                f"enf={int(rec['enforced_ok'])} nq={int(rec['natquery_ok'])} calls={len(calls)} "
                f"{total_ms/1000:.1f}s  {err or ''}",
                flush=True,
            )
            time.sleep(args.gap)
    backend.close()


# ----------------------------------------------------------------------------- report
def pct(n, d):
    return round(100 * n / d, 1) if d else 0.0


def percentile(xs, p):
    xs = sorted(xs)
    if not xs:
        return None
    k = (len(xs) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return round(xs[lo] + (xs[hi] - xs[lo]) * (k - lo), 1)


def schema_recall(rows: list[dict]) -> dict[int, dict]:
    """No LLM involved: did NatQuery's schema retrieval put the tables the gold SQL needs into the
    prompt? Returns {question_id: {needed, shown, complete}}."""
    import sqlglot
    from sqlglot import exp

    from natquery.access.policy import load_policy
    from natquery.access.schema_view import build_view
    from natquery.backends.postgres import PostgresBackend
    from natquery.schema.retriever import retrieve_relevant_schema

    backend = PostgresBackend({"type": "params", **PG}, max_conn=2)
    full = backend.extract_schema()
    backend.close()
    pol = Path(tempfile.mkdtemp()) / "p.yaml"
    pol.write_text(POLICY_YAML)
    policy = load_policy(pol)

    class _Id:  # minimal identity for pick_role
        roles = ["analyst"]
        attributes: dict = {}
        user_id = "bench"
        username = "bench"

    from natquery.auth.models import Identity

    ident = Identity(
        user_id="bench",
        username="bench",
        roles=["analyst"],
        attributes={},
        auth_method="x",
    )
    role = policy.pick_role(ident, None)
    names_by_db = {
        d["db_id"]: {t.lower() for t in d["table_names_original"]}
        for d in json.loads((DATA / "dev_tables.json").read_text())
    }
    qs = {
        q["question_id"]: q
        for q in json.loads((DATA / "mini_dev_postgresql.json").read_text())
    }
    views: dict = {}
    out = {}
    for r in rows:
        q = qs[r["question_id"]]
        db = q["db_id"]
        if db not in views:
            sub = {
                "tables": {
                    n: m for n, m in full["tables"].items() if n in names_by_db[db]
                }
            }
            views[db] = build_view(sub, policy, role)
        text = q["question"] + (f"\nHint: {q['evidence']}" if q.get("evidence") else "")
        shown = set(
            retrieve_relevant_schema(
                text, views[db].prompt_schema, full_if_columns=250
            )["tables"]
        )
        try:
            needed = {
                t.name.lower()
                for t in sqlglot.parse_one(q["SQL"], dialect="postgres").find_all(
                    exp.Table
                )
            } & names_by_db[db]
        except Exception:
            continue
        out[r["question_id"]] = {
            "needed": sorted(needed),
            "shown": sorted(shown),
            "complete": needed <= shown,
        }
    return out


def cmd_report(_args) -> None:
    rows = [
        json.loads(line) for line in RESULTS.read_text().splitlines() if line.strip()
    ]
    valid = [r for r in rows if not r["gold_error"]]
    S: dict = {
        "questions_run": len(rows),
        "gold_failed_on_postgres": len(rows) - len(valid),
        "scored": len(valid),
        "model": MODEL,
    }

    def acc(key, subset):
        return pct(sum(1 for r in subset if r[key]), len(subset))

    S["accuracy"] = {k: acc(k, valid) for k in ("raw_ok", "enforced_ok", "natquery_ok")}
    S["by_difficulty"] = {
        d: {
            "n": len(sub),
            **{k: acc(k, sub) for k in ("raw_ok", "enforced_ok", "natquery_ok")},
        }
        for d in ("simple", "moderate", "challenging")
        if (sub := [r for r in valid if r["difficulty"] == d])
    }
    S["by_database"] = {
        db: {
            "n": len(sub),
            **{k: acc(k, sub) for k in ("raw_ok", "enforced_ok", "natquery_ok")},
        }
        for db in sorted({r["db_id"] for r in valid})
        if (sub := [r for r in valid if r["db_id"] == db])
    }
    accepted = [
        r for r in valid if (r["enforcer_verdict"] or "").startswith("accepted")
    ]
    rejected = [
        r for r in valid if (r["enforcer_verdict"] or "").startswith("rejected")
    ]
    reasons: dict = {}
    for r in rejected:
        reasons[r["enforcer_verdict"]] = reasons.get(r["enforcer_verdict"], 0) + 1
    S["enforcer"] = {
        "first_sql_accepted": len(accepted),
        "first_sql_rejected": len(rejected),
        "rejected_but_raw_correct": sum(1 for r in rejected if r["raw_ok"]),
        "accepted_and_raw_correct_but_enforced_wrong": sum(
            1 for r in accepted if r["raw_ok"] and not r["enforced_ok"]
        ),
        "accepted_raw_wrong_enforced_right": sum(
            1 for r in accepted if not r["raw_ok"] and r["enforced_ok"]
        ),
        "rejection_reasons": dict(sorted(reasons.items(), key=lambda kv: -kv[1])),
    }
    S["pipeline"] = {
        "outcomes": {
            o: sum(1 for r in valid if r["outcome"] == o)
            for o in sorted({r["outcome"] for r in valid})
        },
        "questions_with_repairs": sum(1 for r in valid if r["repairs"]),
        "repair_rescued": sum(1 for r in valid if r["repairs"] and r["natquery_ok"]),
        "unanswerable_replies": sum(
            1 for r in valid if r["error_reason"] == "unanswerable"
        ),
        "output_cut_by_max_tokens": sum(r["truncated_output"] for r in valid),
        "result_truncated_by_row_cap": sum(1 for r in valid if r["natquery_truncated"]),
    }
    try:
        rec = schema_recall(valid)
    except Exception as e:  # diagnostic only; never block the report
        rec, S["schema_recall_error"] = {}, f"{type(e).__name__}: {e}"
    if rec:
        got = [r for r in valid if r["question_id"] in rec]
        comp = [r for r in got if rec[r["question_id"]]["complete"]]
        miss = [r for r in got if not rec[r["question_id"]]["complete"]]
        S["schema_retrieval"] = {
            "questions": len(got),
            "all_needed_tables_in_prompt": len(comp),
            "pct_complete": pct(len(comp), len(got)),
            "natquery_accuracy_when_complete": acc("natquery_ok", comp),
            "natquery_accuracy_when_table_missing": acc("natquery_ok", miss),
            "n_missing": len(miss),
            "unanswerable_when_complete": sum(
                1 for r in comp if r["error_reason"] == "unanswerable"
            ),
            "unanswerable_when_table_missing": sum(
                1 for r in miss if r["error_reason"] == "unanswerable"
            ),
        }
    lat = {}
    for (
        r
    ) in (
        valid
    ):  # wall-clock total_ms also contains the benchmark's own rate-limit pacing waits
        r["net_ms"] = r["llm_ms"] + r["enforcer_ms"] + (r["db_ms"] or 0)
    for name, key in (
        ("end_to_end_excl_pacing", "net_ms"),
        ("llm", "llm_ms"),
        ("enforcer", "enforcer_ms"),
        ("database", "db_ms"),
    ):
        xs = [
            r[key]
            for r in valid
            if r.get(key) is not None and (key != "llm_ms" or r["llm_calls"])
        ]
        lat[name] = {
            "p50": percentile(xs, 50),
            "p95": percentile(xs, 95),
            "mean": round(statistics.fmean(xs), 1) if xs else None,
        }
    S["latency_ms"] = lat
    # efficiency: only where NatQuery and gold both right -> compare DB time of what was run vs gold
    both = [r for r in valid if r["natquery_ok"] and r["db_ms"] and r["gold_ms"]]
    ratios = [r["db_ms"] / r["gold_ms"] for r in both]
    S["efficiency"] = {
        "n_compared": len(both),
        "median_db_time_ratio_vs_gold": (
            round(statistics.median(ratios), 2) if ratios else None
        ),
        "p95_db_time_ratio_vs_gold": percentile(ratios, 95),
        "gold_ms_p50": percentile([r["gold_ms"] for r in valid], 50),
        "gold_ms_p95": percentile([r["gold_ms"] for r in valid], 95),
    }
    tot_cost = sum(r["cost_usd"] for r in valid)
    S["cost"] = {
        "tokens_in": sum(r["tokens_in"] for r in valid),
        "tokens_out": sum(r["tokens_out"] for r in valid),
        "usd_total": round(tot_cost, 4),
        "usd_per_question": round(tot_cost / len(valid), 5) if valid else 0,
        "mean_llm_calls": (
            round(statistics.fmean(r["llm_calls"] for r in valid), 2) if valid else 0
        ),
        "price_basis": f"${PRICE_IN}/M input, ${PRICE_OUT}/M output (Groq list price)",
    }
    (OUT / "summary.json").write_text(json.dumps(S, indent=2))
    cols = [
        "question_id",
        "db_id",
        "difficulty",
        "gold_error",
        "raw_ok",
        "enforcer_verdict",
        "enforced_ok",
        "outcome",
        "error_reason",
        "natquery_ok",
        "llm_calls",
        "repairs",
        "total_ms",
        "llm_ms",
        "enforcer_ms",
        "db_ms",
        "gold_ms",
        "tokens_in",
        "tokens_out",
        "cost_usd",
    ]
    with (OUT / "results.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r.get(c) for c in cols])
    print(json.dumps(S, indent=2))


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--limit", type=int, default=0)
    r.add_argument("--gap", type=float, default=3.0, help="seconds between LLM calls")
    sub.add_parser("report")
    args = ap.parse_args()
    {"run": cmd_run, "report": cmd_report}[args.cmd](args)


if __name__ == "__main__":
    main()
