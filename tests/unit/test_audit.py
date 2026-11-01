import json
import multiprocessing
import stat
import threading
from types import SimpleNamespace

import pytest

from natquery.audit.log import GENESIS, AuditLog


@pytest.fixture
def log(tmp_path):
    return AuditLog(tmp_path / "audit.log")


def lines(log):
    return log.path.read_text().splitlines()


def test_empty_log_verifies_and_tails_empty(log):
    assert log.verify() == (True, 0, None) and log.tail() == []


def test_records_are_chained_from_genesis(log):
    log.record("a")
    log.record("b")
    recs = log.tail(10)
    assert recs[0]["prev"] == GENESIS and recs[1]["prev"] == recs[0]["hash"]
    assert [r["seq"] for r in recs] == [1, 2]
    assert log.verify() == (True, 2, None)


def test_file_permissions_private(log):
    log.record("a")
    assert stat.S_IMODE(log.path.stat().st_mode) == 0o600


def test_ref_is_returned_and_stored(log):
    assert log.record("a", ref="abc123") == "abc123"
    assert log.tail(1)[0]["id"] == "abc123"
    assert len(log.record("b")) == 12


def test_actor_forms(log):
    log.record(
        "a",
        actor=SimpleNamespace(user_id="1", username="u", roles=frozenset({"x", "a"})),
    )
    log.record("b", actor={"username": "raw"})
    log.record("c")
    a, b, c = log.tail(3)
    assert a["actor"] == {"user_id": "1", "username": "u", "roles": ["a", "x"]}
    assert b["actor"] == {"username": "raw"} and c["actor"] is None


def test_secrets_scrubbed_from_details(log):
    log.record("a", password="p", api_key="k", note="postgres://u:hunter2@h/db")
    blob = log.path.read_text()
    assert "hunter2" not in blob and '"p"' not in blob and '"k"' not in blob


def test_modification_detected(log):
    for i in range(3):
        log.record("e", n=i)
    rows = lines(log)
    rows[1] = rows[1].replace('"n":1', '"n":9').replace('"n": 1', '"n": 9')
    log.path.write_text("\n".join(rows) + "\n")
    ok, n, why = log.verify()
    assert not ok and n == 1 and "modified" in why


def test_deletion_of_middle_record_detected(log):
    for i in range(4):
        log.record("e", n=i)
    rows = lines(log)
    del rows[1]
    log.path.write_text("\n".join(rows) + "\n")
    ok, _, why = log.verify()
    assert not ok and "chain broken" in why


def test_reordering_detected(log):
    for i in range(3):
        log.record("e", n=i)
    rows = lines(log)
    rows[1], rows[2] = rows[2], rows[1]
    log.path.write_text("\n".join(rows) + "\n")
    assert not log.verify()[0]


def test_truncating_tail_is_not_detectable_by_chain_alone_but_head_removal_is(log):
    for i in range(3):
        log.record("e", n=i)
    rows = lines(log)
    log.path.write_text("\n".join(rows[1:]) + "\n")  # drop the first record
    assert not log.verify()[0]


def test_corrupt_json_detected(log):
    log.record("a")
    with open(log.path, "a") as f:
        f.write("{not json\n")
    ok, n, why = log.verify()
    assert not ok and n == 1 and "not valid JSON" in why


def test_hmac_key_prevents_recomputing_the_chain(tmp_path, monkeypatch):
    monkeypatch.setenv("NATQUERY_AUDIT_KEY", "k1")
    log = AuditLog(tmp_path / "a.log")
    log.record("a")
    assert log.verify()[0]
    monkeypatch.setenv("NATQUERY_AUDIT_KEY", "other")
    assert not AuditLog(tmp_path / "a.log").verify()[0]
    monkeypatch.delenv("NATQUERY_AUDIT_KEY")
    assert not AuditLog(tmp_path / "a.log").verify()[0]


def test_threads_cannot_fork_the_chain(log):
    def work():
        for _ in range(25):
            log.record("t")

    ts = [threading.Thread(target=work) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert log.verify() == (True, 200, None)


def _proc_work(path):
    log = AuditLog(path)
    for _ in range(20):
        log.record("p")


def test_processes_cannot_fork_the_chain(tmp_path):
    path = tmp_path / "mp.log"
    ps = [multiprocessing.Process(target=_proc_work, args=(path,)) for _ in range(4)]
    [p.start() for p in ps]
    [p.join() for p in ps]
    assert AuditLog(path).verify() == (True, 80, None)


def test_tail_limits_and_chain_continues_after_reopen(log, tmp_path):
    for i in range(30):
        log.record("e", n=i)
    assert len(log.tail(5)) == 5 and log.tail(5)[-1]["details"]["n"] == 29
    again = AuditLog(log.path)
    again.record("more")
    assert again.verify() == (True, 31, None)


def test_non_json_values_are_stringified(log):
    log.record("a", when=object(), s={1, 2})
    assert log.verify()[0]


def test_blank_lines_are_tolerated(log):
    log.record("a")
    with open(log.path, "a") as f:
        f.write("\n\n")
    log.record("b")
    assert log.verify() == (True, 2, None)
    assert [json.loads(x)["event"] for x in lines(log) if x.strip()] == ["a", "b"]


def test_sequence_gap_detected_even_if_hashes_are_recomputed(log):
    """An attacker without the HMAC key can recompute hashes; the sequence numbers still betray deletions."""
    log.record("a")
    log.record("b")
    log.record("c")
    recs = [json.loads(x) for x in lines(log)]
    # drop record 2 and re-chain record 3 onto record 1 with a freshly computed hash
    third = {k: v for k, v in recs[2].items() if k != "hash"}
    third["prev"] = recs[0]["hash"]
    third["hash"] = log._digest(third["prev"], log._canonical(third))
    log.path.write_text(json.dumps(recs[0]) + "\n" + json.dumps(third) + "\n")
    ok, n, why = log.verify()
    assert not ok and n == 1 and "sequence gap" in why
