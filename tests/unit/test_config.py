import os
import stat

import pytest

from natquery.config import paths
from natquery.config.settings import DEFAULT_SECURITY, Settings


def mode(p):
    return stat.S_IMODE(os.stat(p).st_mode)


# ---------------------------------------------------------------- paths
@pytest.mark.parametrize("name", ["erp", "my-db_1", "a.b", "A" * 64, "0db"])
def test_safe_name_accepts(name):
    assert paths.safe_name(name) == name


@pytest.mark.parametrize(
    "name",
    [
        "",
        " ",
        "../etc",
        "a/b",
        "a\\b",
        "..",
        ".hidden",
        "-dash",
        "a..b",
        "A" * 65,
        "a b",
        "a\x00b",
        "é",
        "a;b",
        None,
    ],
)
def test_safe_name_rejects_traversal_and_junk(name):
    with pytest.raises(ValueError):
        paths.safe_name(name)


def test_base_dir_follows_env(monkeypatch, tmp_path):
    monkeypatch.setenv("NATQUERY_HOME", str(tmp_path / "h"))
    assert paths.base_dir() == tmp_path / "h"
    monkeypatch.delenv("NATQUERY_HOME")
    assert paths.base_dir().name == ".natquery"


def test_workspace_dir_is_private_and_contained():
    d = paths.workspace_dir("erp")
    assert mode(d) == 0o700 and d.parent == paths.base_dir()
    with pytest.raises(ValueError):
        paths.workspace_dir("../escape")


def test_write_private_is_atomic_0600_and_overwrites(tmp_path):
    f = tmp_path / "sub" / "f.txt"
    paths.write_private(f, "one")
    paths.write_private(f, "two")
    assert f.read_text() == "two" and mode(f) == 0o600 and mode(f.parent) == 0o700
    assert not list(f.parent.glob("*.tmp"))


def test_write_private_tightens_loose_existing_file(tmp_path):
    f = tmp_path / "f"
    f.write_text("x")
    os.chmod(f, 0o666)
    paths.write_private(f, "y")
    assert mode(f) == 0o600


def test_open_private_append(tmp_path):
    f = tmp_path / "d" / "log"
    for text in ("a\n", "b\n"):
        with paths.open_private_append(f) as fh:
            fh.write(text)
    assert f.read_text() == "a\nb\n" and mode(f) == 0o600


# ---------------------------------------------------------------- settings
def write_cfg(name="erp", **over):
    cfg = {
        "connection_type": "standard",
        "db_host": "h",
        "db_port": "5432",
        "db_name": name,
        "db_user": "u",
        "db_password": "pw",
        "llm_provider": "groq",
        "llm_api_key": "key",
        "llm_model": "m",
    }
    cfg.update(over)
    Settings.save_config(name, cfg)
    paths.write_private(paths.base_dir() / "current_db", name)
    return cfg


def test_no_active_workspace():
    assert Settings.exists() is False
    with pytest.raises(RuntimeError, match="natquery connect"):
        Settings.load_config()
    with pytest.raises(RuntimeError):
        Settings.workspace()


def test_active_name_is_validated_against_traversal():
    paths.write_private(paths.base_dir() / "current_db", "../../etc")
    with pytest.raises(ValueError):
        Settings.workspace()


def test_config_roundtrip_and_permissions():
    write_cfg()
    assert Settings.exists() and Settings.load_config()["db_user"] == "u"
    assert mode(paths.base_dir() / "erp" / "config.json") == 0o600


def test_configured_workspace_without_file_is_not_configured():
    paths.write_private(paths.base_dir() / "current_db", "erp")
    assert Settings.exists() is False
    with pytest.raises(RuntimeError, match="not configured"):
        Settings.load_config()


def test_db_config_standard_and_env_password(monkeypatch):
    write_cfg(sslmode="require")
    c = Settings.get_db_config()
    assert (
        c["type"] == "standard" and c["password"] == "pw" and c["sslmode"] == "require"
    )
    monkeypatch.setenv("NATQUERY_DB_PASSWORD", "from-env")
    assert Settings.get_db_config()["password"] == "from-env"


def test_db_password_may_live_only_in_env(monkeypatch):
    cfg = write_cfg()
    cfg.pop("db_password")
    Settings.save_config("erp", cfg)
    with pytest.raises(ValueError, match="db_password"):
        Settings.get_db_config()
    monkeypatch.setenv("NATQUERY_DB_PASSWORD", "x")
    assert Settings.get_db_config()["password"] == "x"


@pytest.mark.parametrize("missing", ["db_host", "db_port", "db_name", "db_user"])
def test_db_config_required_fields(missing):
    cfg = write_cfg()
    cfg.pop(missing)
    Settings.save_config("erp", cfg)
    with pytest.raises(ValueError, match=missing):
        Settings.get_db_config()


def test_dsn_from_config_and_env(monkeypatch):
    write_cfg(connection_type="dsn", db_dsn="postgres://x")
    assert Settings.get_db_config() == {
        "type": "dsn",
        "dsn": "postgres://x",
        "dbname": "erp",
        "schemas": ["public"],
    }
    monkeypatch.setenv("NATQUERY_DB_DSN", "postgres://env")
    assert Settings.get_db_config()["dsn"] == "postgres://env"
    write_cfg(connection_type="dsn")
    monkeypatch.delenv("NATQUERY_DB_DSN")
    with pytest.raises(ValueError, match="DSN"):
        Settings.get_db_config()


def test_llm_config_env_override_and_missing(monkeypatch):
    write_cfg()
    assert Settings.get_llm_config() == {
        "provider": "groq",
        "api_key": "key",
        "model": "m",
    }
    monkeypatch.setenv("NATQUERY_LLM_API_KEY", "env-key")
    assert Settings.get_llm_config()["api_key"] == "env-key"
    cfg = write_cfg()
    cfg.pop("llm_model")
    Settings.save_config("erp", cfg)
    with pytest.raises(ValueError, match="model"):
        Settings.get_llm_config()


def test_security_defaults_and_overrides():
    write_cfg(
        security={"rate_limit_per_minute": 3, "auth_config": {"lock_threshold": 9}}
    )
    s = Settings.get_security_config()
    assert (
        s["rate_limit_per_minute"] == 3
        and s["max_repair_attempts"] == DEFAULT_SECURITY["max_repair_attempts"]
    )
    assert s["policy_file"].endswith(os.path.join("erp", "policy.yaml"))
    write_cfg(security={"policy_file": "/etc/custom.yaml"})
    assert Settings.get_security_config()["policy_file"] == "/etc/custom.yaml"


def test_security_defaults_are_conservative():
    assert DEFAULT_SECURITY["enable_optimizer"] is False
    assert DEFAULT_SECURITY["auth_provider"] == "local"
    assert DEFAULT_SECURITY["rate_limit_per_minute"] <= 60


def test_default_security_dict_is_not_mutated_by_callers():
    write_cfg(security={"rate_limit_per_minute": 1})
    Settings.get_security_config()
    assert DEFAULT_SECURITY["rate_limit_per_minute"] != 1
    assert "policy_file" in DEFAULT_SECURITY and DEFAULT_SECURITY["policy_file"] is None


def test_trailing_newline_does_not_sneak_past_validation():
    """`$` matches before a trailing newline; validators must use fullmatch."""
    for bad in ("a\nb", "a\n\nb", "\nerp"[1:] + "\x00"):
        with pytest.raises(ValueError):
            paths.safe_name(bad)
    assert (
        paths.safe_name("erp\n") == "erp"
    )  # surrounding whitespace is stripped, never embedded


def test_permission_tightening_tolerates_unsupported_filesystems(monkeypatch, tmp_path):
    def nope(*a, **k):
        raise OSError("chmod unsupported")

    monkeypatch.setattr(paths.os, "chmod", nope)
    paths.ensure_private_dir(tmp_path / "d")
    paths.write_private(tmp_path / "d" / "f", "x")
    assert (tmp_path / "d" / "f").read_text() == "x"


def test_private_files_are_never_world_readable_even_transiently(monkeypatch, tmp_path):
    """The temp file is created 0600 *before* data is written, not chmod-ed afterwards."""
    seen = []
    real_replace = os.replace

    def spy(src, dst):
        seen.append(mode(src))
        return real_replace(src, dst)

    monkeypatch.setattr(paths.os, "replace", spy)
    paths.write_private(tmp_path / "secret.json", "{}")
    assert seen == [0o600]
