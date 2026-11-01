import sqlite3
import stat
import threading
import time
from contextlib import closing

import pytest

from natquery.auth import hashing
from natquery.auth.base import AuthProvider, create_provider, register_provider
from natquery.auth.local import LocalPasswordProvider
from natquery.auth.models import CredentialField, Identity
from natquery.auth.session import InMemorySessionStore, SessionManager
from natquery.auth.user_store import UserStore
from natquery.errors import (
    AccountLockedError,
    AuthenticationError,
    ConfigError,
    SessionExpiredError,
)

PW = "Str0ngPass123!"


@pytest.fixture
def store(tmp_path):
    s = UserStore(tmp_path / "auth.db")
    s.add("alice", hashing.hash_password(PW), ["student"], {"roll_no": "1"})
    return s


@pytest.fixture
def provider(store):
    return LocalPasswordProvider({"lock_threshold": 3, "lock_minutes": 1}, store=store)


# ---------------------------------------------------------------- hashing
def test_hash_roundtrip_and_uniqueness():
    h1, h2 = hashing.hash_password("abc12345!x"), hashing.hash_password("abc12345!x")
    assert h1 != h2  # random salt
    assert hashing.verify_password("abc12345!x", h1)
    assert not hashing.verify_password("abc12345!y", h1)
    assert h1.startswith("scrypt$")


@pytest.mark.parametrize(
    "stored",
    [
        "",
        "garbage",
        "scrypt$1$2",
        "md5$1$1$1$a$b",
        "scrypt$x$y$z$a$b",
        "scrypt$1024$8$1$!!!$!!!",
    ],
)
def test_verify_rejects_malformed_hashes_without_raising(stored):
    assert hashing.verify_password("whatever", stored) is False


def test_unicode_and_long_passwords():
    pw = "pässwörd-密码-" + "x" * 500
    assert hashing.verify_password(pw, hashing.hash_password(pw))


def test_needs_rehash_detects_old_cost_and_garbage(monkeypatch):
    h = hashing.hash_password("abc12345!x")
    assert hashing.needs_rehash(h) is False
    monkeypatch.setattr(hashing, "_N", hashing._N * 2)
    assert hashing.needs_rehash(h) is True
    assert hashing.needs_rehash("nonsense") is True


@pytest.mark.parametrize(
    "pw,user,msg",
    [
        ("short1!", "", "at least"),
        ("alice-secret-99", "alice", "username"),
        ("password1", "", "at least"),
        ("changeme123", "", "common"),
        ("1234567890123", "", "mix"),
        ("abcdefghijkl", "", "mix"),
    ],
)
def test_password_strength_rules(pw, user, msg):
    with pytest.raises(ValueError, match=msg):
        hashing.check_password_strength(pw, user)


def test_password_strength_accepts_good_password():
    hashing.check_password_strength("c0rrect-horse", "alice")


# ---------------------------------------------------------------- user store
def test_user_store_file_is_private(tmp_path):
    UserStore(tmp_path / "x" / "auth.db")
    mode = stat.S_IMODE((tmp_path / "x" / "auth.db").stat().st_mode)
    assert mode == 0o600


def test_usernames_are_case_insensitive_and_unique(store):
    with pytest.raises(ValueError, match="already exists"):
        store.add("ALICE", "h", ["student"])
    assert store.get("ALICE").username == "alice"


def test_store_mutations_and_missing_user_errors(store):
    store.set_roles("alice", ["teacher", "student"])
    assert store.get("alice").roles == frozenset({"teacher", "student"})
    store.set_attributes("alice", {"emp_id": "F1"})
    assert store.get("alice").attributes == {"emp_id": "F1"}
    store.set_active("alice", False)
    assert store.get("alice").active is False
    for fn in (
        lambda: store.set_roles("ghost", []),
        lambda: store.set_active("ghost", True),
        lambda: store.set_password_hash("ghost", "x"),
    ):
        with pytest.raises(ValueError, match="No such user"):
            fn()
    assert store.count() == 1 and [u.username for u in store.list_users()] == ["alice"]


def test_sql_injection_in_username_is_just_a_string(store):
    assert store.get("alice' OR '1'='1") is None
    assert store.get("x'; DROP TABLE users;--") is None
    assert store.count() == 1
    store.add("o'brien", "h", [])
    assert store.get("o'brien")


def test_lockout_math(store):
    store.record_failure("alice", 3, 60)
    store.record_failure("alice", 3, 60)
    assert store.get("alice").locked_until == 0
    store.record_failure("alice", 3, 60)
    rec = store.get("alice")
    assert rec.locked_until > time.time() and rec.failed_attempts == 0
    store.record_success("alice")
    assert store.get("alice").locked_until == 0 and store.get("alice").last_login


# ---------------------------------------------------------------- local provider
def test_authenticate_success(provider):
    ident = provider.authenticate({"username": "alice", "password": PW})
    assert ident.username == "alice" and ident.roles == frozenset({"student"})
    assert ident.attributes == {"roll_no": "1"} and ident.auth_method == "local"


@pytest.mark.parametrize(
    "creds",
    [
        {"username": "alice", "password": "wrong"},
        {"username": "ghost", "password": PW},
        {"username": "", "password": PW},
        {"username": "alice"},
        {},
        {"username": "  ", "password": ""},
    ],
)
def test_bad_credentials_give_one_generic_message(provider, creds):
    with pytest.raises(AuthenticationError) as exc:
        provider.authenticate(creds)
    assert exc.value.user_message == AuthenticationError.default_message
    assert PW not in exc.value.internal


def test_lockout_blocks_even_the_correct_password(provider):
    for _ in range(3):
        with pytest.raises(AuthenticationError):
            provider.authenticate({"username": "alice", "password": "bad"})
    with pytest.raises(AccountLockedError) as exc:
        provider.authenticate({"username": "alice", "password": PW})
    assert (
        exc.value.user_message == AuthenticationError.default_message
    )  # no lock disclosure


def test_lockout_expires(provider, store):
    for _ in range(3):
        with pytest.raises(AuthenticationError):
            provider.authenticate({"username": "alice", "password": "bad"})
    store._update("alice", locked_until=time.time() - 1)
    assert provider.authenticate({"username": "alice", "password": PW})


def test_success_resets_failure_counter(provider, store):
    for _ in range(2):
        with pytest.raises(AuthenticationError):
            provider.authenticate({"username": "alice", "password": "bad"})
    provider.authenticate({"username": "alice", "password": PW})
    assert store.get("alice").failed_attempts == 0


def test_disabled_account_cannot_login(provider, store):
    store.set_active("alice", False)
    with pytest.raises(AuthenticationError):
        provider.authenticate({"username": "alice", "password": PW})


def test_login_upgrades_weak_hash(provider, store, monkeypatch):
    old = hashing.hash_password(PW)
    store.set_password_hash("alice", old)
    monkeypatch.setattr(hashing, "_N", hashing._N * 2)
    monkeypatch.setattr(hashing, "_MAXMEM", hashing._MAXMEM * 2)
    provider.authenticate({"username": "alice", "password": PW})
    new = store.get("alice").password_hash
    assert new != old and new.split("$")[1] == str(hashing._N)


def test_refresh_picks_up_changes_and_revokes(provider, store):
    ident = provider.authenticate({"username": "alice", "password": PW})
    store.set_roles("alice", ["teacher"])
    assert provider.refresh(ident).roles == frozenset({"teacher"})
    store.set_active("alice", False)
    with pytest.raises(AuthenticationError):
        provider.refresh(ident)


def test_refresh_rejects_recreated_account_with_same_name(provider, store):
    ident = provider.authenticate({"username": "alice", "password": PW})
    with closing(sqlite3.connect(store.path)) as db:
        db.execute("DELETE FROM users")
        db.commit()
    store.add("alice", hashing.hash_password(PW), ["admin"])
    with pytest.raises(AuthenticationError):
        provider.refresh(ident)  # same name, different user_id: stale session must die


def test_unknown_user_costs_a_hash_verification(provider, monkeypatch):
    calls = []
    real = hashing.verify_password
    monkeypatch.setattr(
        hashing, "verify_password", lambda p, h: calls.append(h) or real(p, h)
    )
    with pytest.raises(AuthenticationError):
        provider.authenticate({"username": "ghost", "password": "x"})
    assert calls == [hashing.DUMMY_HASH]  # timing equalised against user enumeration


# ---------------------------------------------------------------- registry / pluggability
def test_custom_provider_can_be_registered_and_used(tmp_path):
    class Token(AuthProvider):
        name = "token"

        def __init__(self, config):
            self.valid = config["valid"]

        def credential_fields(self):
            return [CredentialField("token", "Token", secret=True)]

        def authenticate(self, credentials):
            if credentials.get("token") != self.valid:
                raise AuthenticationError(internal="bad token")
            return Identity("t1", "svc", frozenset({"admin"}), auth_method="token")

    register_provider("token", Token)
    p = create_provider("token", {"valid": "s3cret"})
    assert p.authenticate({"token": "s3cret"}).roles == frozenset({"admin"})
    assert (
        p.refresh(p.authenticate({"token": "s3cret"})).username == "svc"
    )  # default refresh
    sm = SessionManager(p)
    assert (
        sm.validate(
            sm.create(p.authenticate({"token": "s3cret"})).token
        ).identity.username
        == "svc"
    )


def test_unknown_provider_is_a_config_error():
    with pytest.raises(ConfigError) as exc:
        create_provider("nope-xyz")
    assert "nope-xyz" in exc.value.internal and "local" in exc.value.internal


def test_entry_point_discovery_failure_never_breaks_startup(monkeypatch):
    import importlib.metadata as md

    monkeypatch.setattr(
        md, "entry_points", lambda **kw: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    with pytest.raises(ConfigError):
        create_provider("still-unknown")


def test_entry_point_provider_is_loaded(monkeypatch):
    import importlib.metadata as md
    from types import SimpleNamespace

    ep = SimpleNamespace(
        name="ep-prov", load=lambda: (lambda cfg: LocalPasswordProvider(cfg))
    )
    monkeypatch.setattr(md, "entry_points", lambda **kw: [ep])
    assert isinstance(create_provider("ep-prov", {}), LocalPasswordProvider)


def test_local_provider_credential_fields(provider):
    fields = provider.credential_fields()
    assert [(f.name, f.secret) for f in fields] == [
        ("username", False),
        ("password", True),
    ]


# ---------------------------------------------------------------- sessions
def test_session_token_not_stored_in_clear(provider):
    sm = SessionManager(provider)
    s = sm.create(Identity("1", "a", frozenset()))
    keys = list(sm.store._d)
    assert s.token not in keys and len(keys[0]) == 64
    assert len(s.token) >= 40


def test_tokens_are_unique(provider):
    sm = SessionManager(provider)
    assert (
        len({sm.create(Identity("1", "a", frozenset())).token for _ in range(200)})
        == 200
    )


def test_validate_updates_last_seen_and_revoke(provider):
    ident = provider.authenticate({"username": "alice", "password": PW})
    sm = SessionManager(provider, idle_seconds=60)
    s = sm.create(ident)
    before = s.last_seen
    time.sleep(0.01)
    assert sm.validate(s.token).last_seen > before
    sm.revoke(s.token)
    with pytest.raises(SessionExpiredError):
        sm.validate(s.token)


def test_failed_refresh_deletes_session(provider, store):
    ident = provider.authenticate({"username": "alice", "password": PW})
    sm = SessionManager(provider)
    s = sm.create(ident)
    store.set_active("alice", False)
    with pytest.raises(AuthenticationError):
        sm.validate(s.token)
    with pytest.raises(SessionExpiredError):  # and it is gone, not merely refused once
        sm.validate(s.token)


def test_in_memory_store_is_thread_safe():
    st = InMemorySessionStore()

    def work(i):
        for j in range(200):
            st.put(f"{i}-{j}", object())
            st.delete(f"{i}-{j - 1}")

    ts = [threading.Thread(target=work, args=(i,)) for i in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(st._d) == 8
