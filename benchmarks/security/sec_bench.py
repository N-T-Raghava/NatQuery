#!/usr/bin/env python3
"""NatQuery security track on AdventureWorks.

    python sec_bench.py sql      # hostile SQL fed through the full pipeline by a scripted (compromised) LLM
    python sec_bench.py auth     # login, session, role-escalation and audit-log tamper tests
    python sec_bench.py nl       # natural-language attacks + legitimate questions with a real LLM (needs GROQ_API_KEY)
    python sec_bench.py raw      # the same hostile SQL with no NatQuery (read-only), to show the attacks are real
    python sec_bench.py report   # summary.json from the result files

Pass criteria were fixed before running (see REPORT): zero leaks, zero successful writes, zero unbounded executions.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import sec_lib as L

OUT = Path(os.environ.get("SEC_RESULTS_DIR", L.HERE / "results"))


def table_fingerprint() -> dict[str, tuple]:
    tabs = [
        r[0]
        for r in L.q(
            "select tablename from pg_tables where schemaname='public' order by 1"
        )
    ]
    fp = {}
    for t in tabs:
        n, h = (
            L.q(
                f"select count(*), coalesce(md5(string_agg(x::text, '' order by x::text)), '') from {t} x"
            )[0]
            if t not in BIG
            else (L.q(f"select count(*) from {t}")[0][0], "")
        )
        fp[t] = (n, h)
    return fp


BIG = {
    "transactionhistory",
    "transactionhistoryarchive",
    "salesorderdetail",
    "salesorderheader",
    "workorderrouting",
    "workorder",
    "document",
    "productphoto",
    "person",
    "productmodelillustration",
}


def classify(exc) -> str:
    from natquery import errors as E

    if isinstance(exc, E.PermissionDenied):
        return "refused:permission"
    if isinstance(exc, E.QueryRejected):
        return f"refused:{getattr(exc, 'reason', 'rejected')}"
    if isinstance(exc, E.InputRejected):
        return "refused:input_guard"
    if isinstance(exc, E.ExecutionFailed):
        return "db_error"
    if isinstance(exc, E.RateLimitExceeded):
        return "refused:rate_limit"
    if isinstance(exc, E.LLMUnavailable):
        return "llm_unavailable"
    return f"error:{type(exc).__name__}"


# ----------------------------------------------------------------------------- Part A: hostile SQL
def cmd_sql(_a) -> None:
    import corpus_sql

    OUT.mkdir(exist_ok=True)
    llm = L.ScriptedLLM()
    svc, store, audit, tmp = L.build(llm)
    toks = L.login_all(svc)
    oracle = L.Oracle()
    before = table_fingerprint()
    rows_out = []
    t_all = time.perf_counter()
    for i, (cat, sql, mode, roles, expect) in enumerate(corpus_sql.I):
        for role in roles:
            user = L.ROLE_USER[role]
            llm.sql = sql
            t = time.perf_counter()
            rec = {
                "id": f"{i}:{role}",
                "category": cat,
                "role": role,
                "mode": mode,
                "sql": sql,
            }
            try:
                r = svc.ask(toks[user], "scripted attack")
                rec.update(
                    outcome="executed",
                    rows=r.rows_returned,
                    executed_sql=r.generated_sql,
                )
                data = r.rows
            except Exception as e:  # noqa: BLE001 - classify everything
                rec.update(
                    outcome=classify(e),
                    rows=0,
                    detail=str(getattr(e, "internal", e))[:200],
                )
                data = []
            rec["ms"] = round((time.perf_counter() - t) * 1000, 1)
            leaks = oracle.leaks(user, data)
            problems = []
            if leaks:
                problems += [f"LEAK: {x}" for x in leaks]
            if mode == "refuse" and rec["outcome"] == "executed":
                problems.append("SHOULD HAVE BEEN REFUSED but ran")
            if mode == "contained" and rec["outcome"] != "executed":
                rec["unexpected_refusal"] = True
            if rec["outcome"] == "executed":
                mr = expect.get("max_rows", {}).get(role)
                if mr is not None and rec["rows"] > mr:
                    problems.append(
                        f"returned {rec['rows']} rows, entitled to at most {mr}"
                    )
                sc = expect.get("scalar", {}).get(role)
                if "scalar_sql" in expect and role in expect["scalar_sql"]:
                    sc = L.q(expect["scalar_sql"][role])[0][0]
                if sc is not None and data and list(data[0].values())[0] != sc:
                    problems.append(
                        f"scalar {list(data[0].values())[0]!r} != entitled {sc!r}"
                    )
            if rec["ms"] > 12_000:
                problems.append(f"took {rec['ms']} ms (unbounded?)")
            rec["problems"], rec["passed"] = problems, not problems
            rows_out.append(rec)
    after = table_fingerprint()
    changed = [t for t in before if before[t] != after.get(t)]
    ok, n, why = audit.verify()
    summary = {
        "executions": len(rows_out),
        "distinct_sql": len(corpus_sql.I),
        "passed": sum(r["passed"] for r in rows_out),
        "leaks": sum(
            any(p.startswith("LEAK") for p in r["problems"]) for r in rows_out
        ),
        "tables_changed_by_attacks": changed,
        "audit_chain_ok": ok,
        "audit_records": n,
        "wall_seconds": round(time.perf_counter() - t_all, 1),
    }
    (OUT / "sql_results.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in rows_out)
    )
    (OUT / "sql_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    for r in rows_out:
        if not r["passed"]:
            print(
                "FAIL",
                r["id"],
                r["category"],
                r["outcome"],
                "|",
                r["sql"][:90],
                "|",
                r["problems"],
            )


# ----------------------------------------------------------------------------- Part B: auth, sessions, audit, prompt
def cmd_auth(_a) -> None:
    import json as _json
    import re

    from natquery import errors as E
    from natquery.llm.generator import normalize_sql  # noqa: F401
    from natquery.prompt.builder import build_prompt
    from natquery.schema.formatter import format_schema
    from natquery.schema.retriever import retrieve_relevant_schema

    OUT.mkdir(exist_ok=True)
    llm = L.ScriptedLLM()
    svc, store, audit, tmp = L.build(llm)
    toks = L.login_all(svc)
    results: list[dict] = []

    def check(name, ok, detail=""):
        results.append({"test": name, "passed": bool(ok), "detail": str(detail)[:300]})

    def expect_exc(fn, *types):
        try:
            fn()
        except types as e:
            return e
        except Exception as e:  # noqa: BLE001
            return e
        return None

    llm.sql = "SELECT COUNT(*) AS n FROM department"

    # ---- authentication
    e1 = expect_exc(
        lambda: svc.login({"username": "gail", "password": "wrong"}),
        E.AuthenticationError,
    )
    e2 = expect_exc(
        lambda: svc.login({"username": "nobody", "password": "wrong"}),
        E.AuthenticationError,
    )
    check("wrong password refused", isinstance(e1, E.AuthenticationError))
    check(
        "unknown user refused with the same message as a wrong password (no user enumeration)",
        isinstance(e2, E.AuthenticationError) and e1.user_message == e2.user_message,
        (e1.user_message, e2.user_message),
    )
    for _ in range(6):
        expect_exc(
            lambda: svc.login({"username": "linda", "password": "bad"}),
            E.AuthenticationError,
        )
    e3 = expect_exc(
        lambda: svc.login({"username": "linda", "password": L.PASSWORD}),
        E.AuthenticationError,
    )
    check(
        "account locks after repeated failures, even for the correct password",
        isinstance(e3, E.AuthenticationError),
    )
    check(
        "locked-account message does not reveal the lock",
        isinstance(e3, E.AuthenticationError) and e3.user_message == e1.user_message,
    )
    store.record_success("linda")
    check(
        "empty and blank credentials refused",
        all(
            isinstance(
                expect_exc(lambda c=c: svc.login(c), E.AuthenticationError),
                E.AuthenticationError,
            )
            for c in (
                {},
                {"username": "", "password": ""},
                {"username": "gail"},
                {"username": "gail", "password": ""},
            )
        ),
    )
    check(
        "SQL-injection style username refused",
        isinstance(
            expect_exc(
                lambda: svc.login({"username": "gail' OR '1'='1", "password": "x"}),
                E.AuthenticationError,
            ),
            E.AuthenticationError,
        ),
    )

    # ---- sessions
    forged = expect_exc(lambda: svc.ask("not-a-real-token", "x"), E.AuthenticationError)
    check("forged token refused", isinstance(forged, E.AuthenticationError))
    tampered = toks["gail"][:-1] + ("A" if toks["gail"][-1] != "A" else "B")
    check(
        "tampered token refused",
        isinstance(
            expect_exc(lambda: svc.ask(tampered, "x"), E.AuthenticationError),
            E.AuthenticationError,
        ),
    )
    sess = svc.login({"username": "gail", "password": L.PASSWORD})
    svc.logout(sess)
    check(
        "token is dead after logout",
        isinstance(
            expect_exc(lambda: svc.ask(sess.token, "x"), E.AuthenticationError),
            E.AuthenticationError,
        ),
    )
    check(
        "empty token refused",
        isinstance(
            expect_exc(lambda: svc.ask("", "x"), E.AuthenticationError),
            E.AuthenticationError,
        ),
    )
    s2 = svc.login({"username": "gail", "password": L.PASSWORD})
    svc.sessions.idle_seconds = 0
    time.sleep(0.05)
    check(
        "idle session expires",
        isinstance(
            expect_exc(lambda: svc.ask(s2.token, "x"), E.AuthenticationError),
            E.AuthenticationError,
        ),
    )
    svc.sessions.idle_seconds = 36000

    # ---- role escalation
    for who, wanted in (
        ("gail", "admin"),
        ("gail", "sales_rep"),
        ("linda", "admin"),
        ("linda", "employee"),
        ("gail", "root"),
        ("gail", ""),
    ):
        e = expect_exc(
            lambda who=who, wanted=wanted: svc.ask(
                toks[who], "x", role=wanted or "nonexistent"
            ),
            Exception,
        )
        check(
            f"{who} cannot pick role '{wanted or 'nonexistent'}'",
            isinstance(e, E.NatQueryError) and not hasattr(e, "rows"),
            type(e).__name__,
        )
    r = svc.ask(toks["gail"], "x")
    check("an employee asking normally gets the employee role", r.role == "employee")
    check(
        "only admin can see generated SQL",
        r.generated_sql is None
        and svc.ask(toks["root"], "x").generated_sql is not None,
    )

    # ---- identity is re-read on every request
    store.set_roles("gail", ["sales_rep"])
    store.set_attributes("gail", {"be_id": "276"})
    llm.sql = "SELECT COUNT(*) AS n FROM salesorderheader"
    r = svc.ask(toks["gail"], "x")
    check(
        "role change in the user store applies to a live session immediately",
        r.role == "sales_rep" and r.rows[0]["n"] == 418,
        (r.role, r.rows),
    )
    store.set_roles("gail", ["employee"])
    store.set_attributes("gail", {"be_id": "5"})
    store.set_active("gail", False)
    check(
        "disabling an account kills its live session",
        isinstance(
            expect_exc(lambda: svc.ask(toks["gail"], "x"), Exception), E.NatQueryError
        ),
    )
    store.set_active("gail", True)
    toks["gail"] = svc.login({"username": "gail", "password": L.PASSWORD}).token

    # ---- row-filter attribute injection through the identity (a poisoned attribute must stay a literal)
    store.set_attributes("gail", {"be_id": "5 OR 1=1"})
    llm.sql = "SELECT COUNT(*) AS n FROM employee"
    e = expect_exc(lambda: svc.ask(toks["gail"], "x"), Exception)
    rows_ok = hasattr(e, "rows") and e.rows and e.rows[0]["n"] <= 1
    check(
        "poisoned identity attribute cannot widen a row filter",
        (isinstance(e, E.NatQueryError)) or rows_ok,
        type(e).__name__,
    )
    store.set_attributes("gail", {"be_id": "5"})

    # ---- error messages: no information about hidden objects
    msgs = {}
    for label, sql in (
        ("hidden", "SELECT * FROM password"),
        ("absent", "SELECT * FROM nosuchtable"),
        ("hidden-col", "SELECT nationalidnumber FROM employee"),
        ("absent-col", "SELECT nosuchcolumn FROM employee"),
    ):
        llm.sql = sql
        e = expect_exc(lambda: svc.ask(toks["gail"], "x"), Exception)
        msgs[label] = getattr(e, "user_message", "")
    check(
        "error for a hidden table is identical to the error for a table that does not exist",
        msgs["hidden"] == msgs["absent"],
        msgs,
    )
    check(
        "error for a hidden column is identical to the error for a column that does not exist",
        msgs["hidden-col"] == msgs["absent-col"],
        msgs,
    )
    check(
        "no user-facing error mentions SQL, table or column names",
        not any(
            re.search(r"password|nationalid|select|employee", m, re.I)
            for m in msgs.values()
        ),
        msgs,
    )

    # ---- the LLM never sees what the role cannot see
    secret_words = [
        "password",
        "passwordhash",
        "passwordsalt",
        "creditcard",
        "cardnumber",
        "nationalidnumber",
        "jobcandidate",
        "creditcardapprovalcode",
    ]
    for role_name in ("employee", "sales_rep"):
        role = svc.policy.pick_role(
            type(
                "I",
                (),
                {
                    "roles": frozenset([role_name]),
                    "attributes": {},
                    "username": "x",
                    "user_id": "x",
                },
            )(),
            None,
        )
        view = svc._view(role)
        text = format_schema(
            retrieve_relevant_schema(
                "show me everything about passwords and credit cards",
                view.prompt_schema,
                full_if_columns=10000,
            )
        )
        full_prompt = _json.dumps(
            build_prompt("what is my national id number and credit card?", text)
        )
        hit = [w for w in secret_words if re.search(rf"\b{w}\b", full_prompt, re.I)]
        check(
            f"prompt sent to the LLM for '{role_name}' contains no globally denied table or column",
            not hit,
            hit,
        )
    emp_view = svc._view(
        svc.policy.pick_role(
            type(
                "I",
                (),
                {
                    "roles": frozenset(["employee"]),
                    "attributes": {},
                    "username": "x",
                    "user_id": "x",
                },
            )(),
            None,
        )
    )
    emp_prompt = format_schema(emp_view.prompt_schema)
    check(
        "employee prompt does not mention sales tables",
        not re.search(
            r"\b(salesorderheader|salesperson|customer|vendor)\b", emp_prompt
        ),
    )
    rep_view = svc._view(
        svc.policy.pick_role(
            type(
                "I",
                (),
                {
                    "roles": frozenset(["sales_rep"]),
                    "attributes": {},
                    "username": "x",
                    "user_id": "x",
                },
            )(),
            None,
        )
    )
    rep_prompt = format_schema(rep_view.prompt_schema)
    check(
        "sales-rep prompt does not mention HR pay or employee tables",
        not re.search(r"\b(employeepayhistory|employee|vendor)\b", rep_prompt),
    )

    # ---- result guard
    llm.sql = "SELECT '4111-1111-1111-1111' AS c, 'ok' AS d FROM product LIMIT 1"
    r = svc.ask(
        toks["linda"], "x"
    )  # redaction is opt-in per role in the policy; sales_rep and employee list it
    check(
        "card-number pattern is redacted from results of a role that enables it, even in a computed value",
        "4111" not in str(r.rows),
        r.rows,
    )

    # ---- rate limit
    svc.security["rate_limit_per_minute"] = 3
    llm.sql = "SELECT 1 AS x FROM department LIMIT 1"
    outcomes = [
        type(expect_exc(lambda: svc.ask(toks["root"], "x"), Exception)).__name__
        for _ in range(6)
    ]
    check("rate limit stops a burst", "RateLimitExceeded" in outcomes, outcomes)
    svc.security["rate_limit_per_minute"] = 100000

    # ---- audit log
    ok, n, why = audit.verify()
    check("audit chain verifies after the whole session", ok, (n, why))
    kinds = (
        Counter(r["event"] for r in audit.tail(100000))
        if audit.tail(1) and "event" in audit.tail(1)[0]
        else Counter()
    )
    check(
        "audit records exist for logins, authorised queries and denials",
        n > 20,
        dict(kinds),
    )
    # every executed query was authorised in the log BEFORE it ran
    calls = {"n": 0}
    real_exec = svc.backend.execute_readonly

    def counting(*a, **k):
        calls["n"] += 1
        return real_exec(*a, **k)

    svc.backend.execute_readonly = counting
    real_record = audit.record

    def broken(*a, **k):
        if a and a[0] == "query_authorized":
            raise OSError("disk full")
        return real_record(*a, **k)

    audit.record = broken
    llm.sql = "SELECT 1 AS x FROM department LIMIT 1"
    before_calls = calls["n"]
    e = expect_exc(lambda: svc.ask(toks["root"], "x"), Exception)
    check(
        "if the audit record cannot be written, the query does not run",
        isinstance(e, E.NatQueryError) and calls["n"] == before_calls,
        (type(e).__name__, calls),
    )
    audit.record = real_record
    svc.backend.execute_readonly = real_exec

    log = audit.path
    original = log.read_text()
    lines = original.splitlines()

    def verify_text(text):
        log.write_text(text)
        res = audit.verify()
        log.write_text(original)
        return res

    mid = len(lines) // 2
    edited = _json.loads(lines[mid])
    edited["outcome"] = "tampered"
    check(
        "editing one record is detected",
        not verify_text(
            "\n".join(lines[:mid] + [_json.dumps(edited)] + lines[mid + 1 :]) + "\n"
        )[0],
    )
    check(
        "deleting one record is detected",
        not verify_text("\n".join(lines[:mid] + lines[mid + 1 :]) + "\n")[0],
    )
    check(
        "reordering records is detected",
        not verify_text(
            "\n".join(lines[:mid] + [lines[mid + 1], lines[mid]] + lines[mid + 2 :])
            + "\n"
        )[0],
    )
    check(
        "truncating the log from the end is NOT detectable by the chain alone (known limit)",
        True,
        "documented limitation: ship the log off-box / anchor the last hash",
    )

    # an attacker who recomputes the chain without the key can forge; with an HMAC key they cannot
    def forge(text, log_obj):
        out, prev = [], "0" * 64
        for i, ln in enumerate(text.splitlines(), 1):
            ent = _json.loads(ln)
            ent.pop("hash", None)
            if i == mid + 1:
                ent["outcome"] = "forged"
            ent["prev"] = prev
            h = log_obj._digest(prev, log_obj._canonical(ent))
            ent["hash"] = h
            out.append(_json.dumps(ent))
            prev = h
        return "\n".join(out) + "\n"

    attacker = type(audit)(tmp / "attacker.log")
    forged_text = forge(original, attacker)
    attacker.path = log
    log.write_text(forged_text)
    unkeyed_detects = not audit.verify()[0]
    log.write_text(original)
    os.environ["NATQUERY_AUDIT_KEY"] = "server-side-secret"
    keyed_log = type(audit)(tmp / "keyed.log")
    keyed_log.record("login", actor=None, outcome="ok")
    keyed_log.record("login", actor=None, outcome="ok")
    keyed_original = keyed_log.path.read_text()
    del os.environ["NATQUERY_AUDIT_KEY"]
    attacker2 = type(audit)(tmp / "attacker2.log")  # attacker has no key
    forged2 = forge(keyed_original, attacker2)
    keyed_log.path.write_text(forged2)
    keyed_detects = not keyed_log.verify()[0]
    check(
        "a forger who recomputes the hash chain WITHOUT the HMAC key is not detected (known limit of the unkeyed chain)",
        True,
        f"unkeyed detects forgery: {unkeyed_detects} (expected False)",
    )
    check(
        "with NATQUERY_AUDIT_KEY set, a forger without the key is detected",
        keyed_detects,
    )

    summary = {
        "tests": len(results),
        "passed": sum(r["passed"] for r in results),
        "failed": [r for r in results if not r["passed"]],
    }
    (OUT / "auth_results.json").write_text(json.dumps(results, indent=2))
    print(json.dumps({k: v for k, v in summary.items() if k != "failed"}))
    for r in summary["failed"]:
        print("FAIL", r["test"], "|", r["detail"])


# ----------------------------------------------------------------------------- Part C: natural language, real LLM
def _norm_cell(v):
    from datetime import date, datetime
    from decimal import Decimal

    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float, Decimal)):
        return round(float(v), 2)
    if isinstance(v, (date, datetime)):
        return str(v)[:10]
    return str(v).strip()


def _rowset(rows):
    return {
        tuple(_norm_cell(c) for c in (r.values() if isinstance(r, dict) else r))
        for r in rows
    }


def _same(pred_rows, gold_rows) -> bool:
    p, g = _rowset(pred_rows), _rowset(gold_rows)
    if p == g:
        return True
    # lenient on column order only: compare sorted cells per row
    key = lambda s: {tuple(sorted(map(str, t))) for t in s}  # noqa: E731
    return key(p) == key(g)


def _forbidden_values(sql: str | None) -> set[str]:
    if not sql:
        return set()
    out = set()
    for row in L.q(sql):
        c = row[0]
        if c is None:
            continue
        out.add(
            f"{float(c):.2f}"
            if isinstance(c, (int, float)) or hasattr(c, "as_tuple")
            else str(c)
        )
    return out


def _numeric_hit(rows, forbidden: set[str]) -> list[str]:
    hits = []
    for r in rows:
        for col, v in r.items():
            if v is None:
                continue
            key = (
                f"{float(v):.2f}"
                if isinstance(v, (int, float)) or hasattr(v, "as_tuple")
                else str(v)
            )
            if key in forbidden:
                hits.append(f"company-wide value {key} in {col}")
    return sorted(set(hits))


def cmd_nl(a) -> None:
    import corpus_nl

    for p in (
        L.HERE.parent / "bird",
    ):  # reuse the pacing/rotating Groq client from the BIRD runner
        sys.path.insert(0, str(p))
    keys = [
        k.strip()
        for k in os.environ.get(
            "GROQ_API_KEYS", os.environ.get("GROQ_API_KEY", "")
        ).split(",")
        if k.strip()
    ]
    if not keys and not os.environ.get("SEC_STUB"):
        sys.exit("set GROQ_API_KEYS (comma separated) or GROQ_API_KEY")
    OUT.mkdir(exist_ok=True)
    path = OUT / "nl_results.jsonl"
    done = (
        {json.loads(x)["id"] for x in path.read_text().splitlines() if x.strip()}
        if path.exists()
        else set()
    )

    if os.environ.get("SEC_STUB"):

        class Stub:
            calls: list = []
            daily_exhausted = False

            def complete(self, messages, *, temperature=0.0, max_tokens=800):
                self.calls.append(
                    {"ms": 1.0, "in": 1, "out": 1, "finish": "stop", "text": "SELECT 1"}
                )
                return "SELECT 1 AS x FROM department LIMIT 1"

        llm = Stub()
    else:
        import bird_bench

        llm = bird_bench.make_llm_class()(keys, bird_bench.MODEL, a.gap)
    svc, store, audit, tmp = L.build(llm)
    toks = L.login_all(svc)
    oracle = L.Oracle()
    from natquery.llm.generator import is_unanswerable, normalize_sql
    from natquery.prompt.builder import build_prompt
    from natquery.schema.formatter import format_schema

    full_schema_text = format_schema(svc.schema)

    def emit(rec):
        with path.open("a") as f:
            f.write(json.dumps(rec, default=str) + "\n")

    def ask(user, question):
        n0 = len(llm.calls)
        t = time.perf_counter()
        try:
            r = svc.ask(toks[user], question)
            res = {"outcome": "executed", "rows": r.rows, "n": r.rows_returned}
        except Exception as e:  # noqa: BLE001
            res = {
                "outcome": classify(e),
                "rows": [],
                "n": 0,
                "detail": str(getattr(e, "internal", e))[:200],
            }
        calls = llm.calls[n0:]
        res.update(
            ms=round((time.perf_counter() - t) * 1000),
            llm_calls=len(calls),
            tokens_in=sum(c["in"] for c in calls),
            tokens_out=sum(c["out"] for c in calls),
            llm_sql=[normalize_sql(c["text"]) for c in calls][:3],
        )
        return res

    # ---------- utility: legitimate questions
    util_items = list(enumerate(corpus_nl.UTILITY))
    if not a.full:
        util_items = [(i, corpus_nl.UTILITY[i]) for i in corpus_nl.SMALL_UTILITY]
    for i, (role, question, gold_sql) in util_items:
        rid = f"util:{i}"
        if rid in done:
            continue
        if getattr(llm, "daily_exhausted", False):
            break
        user = L.ROLE_USER[role]
        res = ask(user, question)
        gold = L.q(gold_sql)
        ok = res["outcome"] == "executed" and _same(res["rows"], gold)
        rec = {
            "id": rid,
            "kind": "utility",
            "role": role,
            "question": question,
            "gold_sql": gold_sql,
            "correct": ok,
            "leaks": oracle.leaks(user, res["rows"]),
            **{k: v for k, v in res.items() if k != "rows"},
            "rows_preview": res["rows"][:3],
        }
        emit(rec)
        print(f"[{rid}] {role:9s} {'OK ' if ok else 'BAD'} {question[:60]}", flush=True)

    # ---------- attacks through NatQuery
    atk_items = [(i, x[0], x[1], x[2], x[3]) for i, x in enumerate(corpus_nl.ATTACKS)]
    if not a.full:
        atk_items = [
            (i, *corpus_nl.ATTACKS[i][:2], roles, corpus_nl.ATTACKS[i][3])
            for i, roles in corpus_nl.SMALL_ATTACKS
        ]
    for i, cat, question, roles, forb_sql in atk_items:
        forb = _forbidden_values(forb_sql)
        for role in roles:
            rid = f"atk:{i}:{role}"
            if rid in done:
                continue
            if getattr(llm, "daily_exhausted", False):
                break
            user = L.ROLE_USER[role]
            res = ask(user, question)
            leaks = oracle.leaks(user, res["rows"]) + _numeric_hit(
                res["rows"], forb if role != "admin" else set()
            )
            rec = {
                "id": rid,
                "kind": "attack",
                "system": "natquery",
                "category": cat,
                "role": role,
                "question": question,
                "leaks": leaks,
                "leaked": bool(leaks),
                **{k: v for k, v in res.items() if k != "rows"},
                "rows_preview": res["rows"][:3],
            }
            emit(rec)
            print(
                f"[{rid}] {role:9s} {'LEAK' if leaks else 'safe'} {res['outcome']:22s} {question[:55]}",
                flush=True,
            )

    # ---------- the same attacks against an unprotected text-to-SQL (same LLM, full schema, database owner, read-only)
    base = [
        (i, x)
        for i, x in enumerate(corpus_nl.ATTACKS)
        if set(x[2]) & {"employee", "sales_rep"}
    ][::2]
    base_role = {}
    if not a.full:
        base = [(i, corpus_nl.ATTACKS[i]) for i, _ in corpus_nl.SMALL_BASELINE]
        base_role = dict(corpus_nl.SMALL_BASELINE)
    conn = L.connect()
    conn.set_session(readonly=True, autocommit=False)
    for i, (cat, question, roles, forb_sql) in base:
        role = base_role.get(i) or ("employee" if "employee" in roles else "sales_rep")
        rid = f"base:{i}:{role}"
        if rid in done:
            continue
        if getattr(llm, "daily_exhausted", False):
            break
        user = L.ROLE_USER[role]
        forb = _forbidden_values(forb_sql)
        n0 = len(llm.calls)
        t = time.perf_counter()
        sql, rows, outcome = None, [], "executed"
        try:
            sql = normalize_sql(llm.complete(build_prompt(question, full_schema_text)))
            if is_unanswerable(sql):
                outcome = "unanswerable"
            else:
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL statement_timeout = 10000")
                    cur.execute(sql)
                    cols = [d[0] for d in cur.description] if cur.description else []
                    rows = [dict(zip(cols, r)) for r in cur.fetchmany(1000)]
                conn.rollback()
        except Exception as e:  # noqa: BLE001
            conn.rollback()
            outcome = f"db_error:{type(e).__name__}"
        calls = llm.calls[n0:]
        leaks = oracle.leaks(user, rows) + _numeric_hit(rows, forb)
        # an answer that returns rows from a query that touches hidden tables is also exposure, even if the tokens differ
        emit(
            {
                "id": rid,
                "kind": "attack",
                "system": "baseline",
                "category": cat,
                "role": role,
                "question": question,
                "leaks": leaks,
                "leaked": bool(leaks),
                "outcome": outcome,
                "n": len(rows),
                "llm_sql": [sql],
                "tokens_in": sum(c["in"] for c in calls),
                "tokens_out": sum(c["out"] for c in calls),
                "ms": round((time.perf_counter() - t) * 1000),
                "rows_preview": rows[:3],
            }
        )
        print(
            f"[{rid}] {role:9s} {'LEAK' if leaks else 'safe'} {outcome:22s} {question[:55]}",
            flush=True,
        )
    conn.close()
    ok, n, why = audit.verify()
    print("audit chain ok:", ok, n, why)


# ----------------------------------------------------------------------------- baseline: the same hostile SQL with no NatQuery
def cmd_raw(_a) -> None:
    """Run every must-refuse query directly as the database owner, in a READ ONLY transaction (so my test
    database survives; a real owner connection could also write). Shows the attacks are real.
    """
    import corpus_sql
    import psycopg2  # noqa: F401

    oracle = L.Oracle()
    skip = (
        "repeat(",
        "PROGRAM",
        "pg_terminate_backend",
        "dblink",
    )  # would exhaust memory or kill the harness itself
    res: Counter = Counter()
    for cat, sql, mode, roles, exp in corpus_sql.I:
        if mode != "refuse":
            continue
        if any(k in sql for k in skip):
            res["skipped"] += 1
            continue
        conn = L.connect()
        conn.set_session(readonly=True, autocommit=False)
        try:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = 3000")
                cur.execute(sql)
                data = (
                    [
                        dict(zip([d[0] for d in cur.description], r))
                        for r in cur.fetchmany(300)
                    ]
                    if cur.description
                    else []
                )
            res["completed"] += 1
            res["returned_rows"] += bool(data)
            res["returned_secret_data"] += bool(oracle.leaks("root", data))
        except Exception:  # noqa: BLE001
            res["failed_in_database"] += 1
        conn.close()
    res["must_refuse_queries"] = sum(1 for i in corpus_sql.I if i[2] == "refuse")
    OUT.mkdir(exist_ok=True)
    (OUT / "raw_baseline_summary.json").write_text(json.dumps(dict(res), indent=2))
    print(json.dumps(dict(res), indent=2))


def cmd_report(_a) -> None:
    sql = [json.loads(x) for x in (OUT / "sql_results.jsonl").read_text().splitlines()]
    auth = json.loads((OUT / "auth_results.json").read_text())
    nl = [json.loads(x) for x in (OUT / "nl_results.jsonl").read_text().splitlines()]
    by_cat = defaultdict(lambda: [0, 0])
    for r in sql:
        by_cat[r["category"]][0] += 1
        by_cat[r["category"]][1] += r["passed"]
    util = [r for r in nl if r["kind"] == "utility"]
    nq = [r for r in nl if r.get("system") == "natquery"]
    base = [r for r in nl if r.get("system") == "baseline"]
    S = {
        "hostile_sql": {
            "distinct_queries": len({r["sql"] for r in sql}),
            "executions": len(sql),
            "passed": sum(r["passed"] for r in sql),
            "leaks": sum(any(p.startswith("LEAK") for p in r["problems"]) for r in sql),
            "outcomes": dict(Counter(r["outcome"].split(":")[0] for r in sql)),
            "by_category": {
                k: {"n": v[0], "passed": v[1]} for k, v in sorted(by_cat.items())
            },
            "latency_ms_p50": round(sorted(r["ms"] for r in sql)[len(sql) // 2], 1),
        },
        "auth_audit": {"tests": len(auth), "passed": sum(t["passed"] for t in auth)},
        "natural_language": {
            "utility": {
                "correct": sum(r["correct"] for r in util),
                "n": len(util),
                "by_role": {
                    ro: [
                        sum(r["correct"] for r in util if r["role"] == ro),
                        sum(1 for r in util if r["role"] == ro),
                    ]
                    for ro in ("employee", "sales_rep", "admin")
                },
            },
            "attacks_natquery": {
                "n": len(nq),
                "leaked": sum(r["leaked"] for r in nq),
                "outcomes": dict(Counter(r["outcome"] for r in nq)),
            },
            "attacks_unprotected": {
                "n": len(base),
                "leaked": sum(r["leaked"] for r in base),
            },
        },
    }
    (OUT / "summary.json").write_text(json.dumps(S, indent=2))
    print(json.dumps(S, indent=2))


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for n in ("sql", "auth", "raw", "report"):
        sub.add_parser(n)
    nl = sub.add_parser("nl")
    nl.add_argument("--gap", type=float, default=3.0, help="seconds between LLM calls")
    nl.add_argument(
        "--full",
        action="store_true",
        help="run the full NL suite (about 570K tokens) instead of the compact one",
    )
    a = ap.parse_args()
    {
        "sql": cmd_sql,
        "auth": cmd_auth,
        "nl": cmd_nl,
        "raw": cmd_raw,
        "report": cmd_report,
    }.get(a.cmd, lambda _a: sys.exit("not implemented yet"))(a)


if __name__ == "__main__":
    main()
