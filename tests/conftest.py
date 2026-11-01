import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))  # lets tests `import fakes`

_ENV_PREFIX = "NATQUERY_"


@pytest.fixture(autouse=True)
def isolated_natquery_home(tmp_path, monkeypatch):
    """Every test gets a private NATQUERY_HOME and no ambient NATQUERY_* settings."""
    for key in [
        k
        for k in os.environ
        if k.startswith(_ENV_PREFIX) and not k.startswith("NATQUERY_TEST_")
    ]:
        monkeypatch.delenv(key)
    home = tmp_path / ".natquery"
    home.mkdir()
    monkeypatch.setenv("NATQUERY_HOME", str(home))
    yield home


@pytest.fixture
def erp_policy():
    import fakes

    return fakes.erp_policy()


@pytest.fixture
def erp_schema():
    import fakes

    return fakes.ERP_SCHEMA


@pytest.fixture
def svc_factory(tmp_path):
    """make_service bound to this test's tmp_path."""
    import fakes

    def factory(**kw):
        return fakes.make_service(tmp_path, **kw)

    return factory


@pytest.fixture(autouse=True)
def fast_password_hashing(monkeypatch):
    """scrypt at production cost makes every login ~100ms; tests use a cheap work factor.

    Cost-specific behaviour (rehash-on-login) is tested explicitly in test_auth.py.
    """
    from natquery.auth import hashing

    monkeypatch.setattr(hashing, "_N", 2**10)
    monkeypatch.setattr(hashing, "_MAXMEM", 128 * 2**10 * 8 * 2)
    monkeypatch.setattr(
        hashing, "DUMMY_HASH", hashing.hash_password("natquery-dummy-password")
    )


# --- POSIX-permission tests --------------------------------------------------
# These assert Unix file modes (0o600 / 0o700), which Windows does not model.
# The behaviour is still exercised on Linux and macOS in CI.
_POSIX_ONLY = {
    "test_user_store_file_is_private",
    "test_file_permissions_private",
    "test_workspace_dir_is_private_and_contained",
    "test_write_private_is_atomic_0600_and_overwrites",
    "test_write_private_tightens_loose_existing_file",
    "test_open_private_append",
    "test_config_roundtrip_and_permissions",
    "test_private_files_are_never_world_readable_even_transiently",
    "test_schema_roundtrip_and_semantics_merge",
    "test_system_log_scrubs_secrets_and_is_private",
    "test_connect_interactive_stores_private_config_and_hides_secrets",
}


def pytest_collection_modifyitems(config, items):
    import sys

    if sys.platform != "win32":
        return
    skip = pytest.mark.skip(
        reason="POSIX file-mode semantics are not available on Windows"
    )
    for item in items:
        if item.name in _POSIX_ONLY:
            item.add_marker(skip)
