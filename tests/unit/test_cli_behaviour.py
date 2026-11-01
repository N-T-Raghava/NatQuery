"""CLI behaviour with a real on-disk workspace (policy, users, audit) and scripted terminal input."""

import json
import shutil
from types import SimpleNamespace

import fakes
import pytest

from natquery.app import NatQueryApp
from natquery.cli import admin, commands, formatter, shell
from natquery.cli.main import _build_parser, main
from natquery.config import paths
from natquery.config.settings import Settings
from natquery.errors import ConfigError, QueryRejected
from natquery.schema.store import save_schema

PW = "Str0ng-Passw0rd"


class Terminal:
    """Feeds scripted answers to input()/getpass()/console.input() and fails if over-read."""

    def __init__(self, monkeypatch, *answers):
        self.answers = list(answers)
        self.prompts = []
        for target in (
            "builtins.input",
            "natquery.cli.admin.getpass",
            "natquery.cli.commands.getpass",
        ):
            monkeypatch.setattr(target, self._next)
        monkeypatch.setattr(shell, "console", SimpleNamespace(input=self._next))
        monkeypatch.setattr(shell.time, "sleep", lambda s: None)
        monkeypatch.setattr(admin.time, "sleep", lambda s: None)

    def _next(self, prompt=""):
        self.prompts.append(prompt)
        if not self.answers:
            raise EOFError
        a = self.answers.pop(0)
        if isinstance(a, BaseException):
            raise a
        return a


@pytest.fixture
def workspace(tmp_path):
    cfg = {
        "connection_type": "standard",
        "db_host": "127.0.0.1",
        "db_port": "5432",
        "db_name": "erp",
        "db_user": "reader",
        "db_password": "dbpass-SECRET",
        "llm_provider": "groq",
        "llm_api_key": "gsk_SECRETKEY",
        "llm_model": "m",
        "workspace_name": "erp",
    }
    Settings.save_config("erp", cfg)
    paths.write_private(paths.base_dir() / "current_db", "erp")
    shutil.copy(fakes.EXAMPLE_POLICY, paths.base_dir() / "erp" / "policy.yaml")
    return paths.base_dir() / "erp"


def seed_admin(username="root", roles=("admin",), attrs=None):
    from natquery.auth import hashing
    from natquery.auth.user_store import UserStore

    UserStore().add(username, hashing.hash_password(PW), list(roles), attrs or {})


def ns(**kw):
    return SimpleNamespace(**kw)


def audit_events():
    from natquery.audit.log import AuditLog

    return AuditLog().tail(1000)


# ====================================================================== user management
def test_bootstrap_first_account_must_be_able_to_manage_users(
    workspace, monkeypatch, capsys
):
    Terminal(monkeypatch)
    rc = admin.user_command(
        ns(user_cmd="add", username="bob", roles="student", attr=[])
    )
    assert rc == 1 and "can_manage_users" in capsys.readouterr().out
    from natquery.auth.user_store import UserStore

    assert UserStore().count() == 0


def test_bootstrap_creates_first_admin_without_login(workspace, monkeypatch, capsys):
    Terminal(monkeypatch, PW, PW)
    rc = admin.user_command(
        ns(user_cmd="add", username="root", roles="admin", attr=["emp_id=F1"])
    )
    assert rc == 0
    from natquery.auth.user_store import UserStore

    rec = UserStore().get("root")
    assert rec.roles == frozenset({"admin"}) and rec.attributes == {"emp_id": "F1"}
    assert PW not in rec.password_hash and rec.password_hash.startswith("scrypt$")
    ev = [e for e in audit_events() if e["event"] == "user_add"][0]
    assert ev["actor"] == {"username": "<bootstrap>"} and PW not in json.dumps(ev)


def test_second_add_requires_an_admin_login(workspace, monkeypatch, capsys):
    seed_admin()
    Terminal(monkeypatch, "root", PW, "Zebra-Pass-77", "Zebra-Pass-77")
    rc = admin.user_command(
        ns(
            user_cmd="add",
            username="carol",
            roles="student",
            attr=["roll_no=21CS10009"],
        )
    )
    out = capsys.readouterr().out
    assert rc == 0 and "created" in out
    from natquery.auth.user_store import UserStore

    assert UserStore().get("carol").attributes == {"roll_no": "21CS10009"}


def test_non_admin_cannot_manage_users(workspace, monkeypatch, capsys):
    seed_admin("stud", ["student"], {"roll_no": "1"})
    Terminal(monkeypatch, "stud", PW)
    assert admin.user_command(ns(user_cmd="list")) == 1
    assert "not permitted" in capsys.readouterr().out
    last = [e for e in audit_events() if e["event"] == "admin_login"][-1]
    assert (
        last["outcome"] == "denied"
        and last["details"]["reason"] == "missing_permission"
    )


def test_admin_login_gives_up_after_three_failures(workspace, monkeypatch, capsys):
    seed_admin()
    Terminal(monkeypatch, "root", "bad1", "root", "bad2", "root", "bad3")
    assert admin.user_command(ns(user_cmd="list")) == 1
    out = capsys.readouterr().out
    assert "Too many failed attempts" in out
    denied = [
        e
        for e in audit_events()
        if e["event"] == "admin_login" and e["outcome"] == "denied"
    ]
    assert len(denied) == 3
    # check the free-form fields only: ids/hashes are random hex and could contain "bad1" by chance
    for e in denied:
        assert not any(w in json.dumps(e["details"]) for w in ("bad1", "bad2", "bad3"))


@pytest.mark.parametrize(
    "cmd,effect",
    [
        (ns(user_cmd="disable", username="bob"), lambda r: r.active is False),
        (
            ns(user_cmd="roles", username="bob", roles="teacher"),
            lambda r: r.roles == frozenset({"teacher"}),
        ),
        (
            ns(user_cmd="attrs", username="bob", attr=["emp_id=F9"]),
            lambda r: r.attributes == {"emp_id": "F9"},
        ),
    ],
)
def test_user_mutations_are_applied_and_audited(workspace, monkeypatch, cmd, effect):
    seed_admin()
    seed_admin("bob", ["student"], {"roll_no": "1"})
    Terminal(monkeypatch, "root", PW)
    assert admin.user_command(cmd) == 0
    from natquery.auth.user_store import UserStore

    assert effect(UserStore().get("bob"))
    assert any(
        e["event"].startswith("user_") and e["details"].get("target") == "bob"
        for e in audit_events()
    )


def test_enable_after_disable_and_password_change(workspace, monkeypatch):
    seed_admin()
    seed_admin("bob", ["student"], {"roll_no": "1"})
    from natquery.auth.user_store import UserStore

    UserStore().set_active("bob", False)
    Terminal(monkeypatch, "root", PW)
    assert admin.user_command(ns(user_cmd="enable", username="bob")) == 0
    assert UserStore().get("bob").active
    Terminal(monkeypatch, "root", PW, "Brand-New-Pass9", "Brand-New-Pass9")
    old = UserStore().get("bob").password_hash
    assert admin.user_command(ns(user_cmd="passwd", username="bob")) == 0
    assert UserStore().get("bob").password_hash != old


def test_unknown_role_and_bad_attribute_rejected(workspace, monkeypatch, capsys):
    seed_admin()
    Terminal(monkeypatch, "root", PW)
    assert (
        admin.user_command(ns(user_cmd="roles", username="root", roles="superuser"))
        == 1
    )
    assert "unknown role" in capsys.readouterr().out
    Terminal(monkeypatch, "root", PW)
    assert (
        admin.user_command(
            ns(user_cmd="attrs", username="root", attr=["roll_no=x'; --"])
        )
        == 1
    )


@pytest.mark.parametrize(
    "name", ["ab", "a b", "x;y", "a" * 65, "é", "a/b", "alice\n", "bob\r\n"]
)
def test_bad_usernames_rejected(workspace, monkeypatch, capsys, name):
    seed_admin()
    Terminal(monkeypatch, "root", PW)
    assert (
        admin.user_command(ns(user_cmd="add", username=name, roles="student", attr=[]))
        == 1
    )


@pytest.mark.parametrize("pair", ["roll_no=1\n", "roll_no=a\nb", "roll_no\n=1"])
def test_trailing_newline_in_attributes_rejected(pair):
    with pytest.raises(ValueError):
        admin._parse_attrs([pair])


def test_missing_user_reported_not_crashed(workspace, monkeypatch, capsys):
    seed_admin()
    Terminal(monkeypatch, "root", PW)
    assert admin.user_command(ns(user_cmd="disable", username="ghost")) == 1
    assert "No such user" in capsys.readouterr().out


def test_user_list_shows_state_without_hashes(workspace, monkeypatch, capsys):
    seed_admin()
    seed_admin("bob", ["student"], {"roll_no": "1"})
    Terminal(monkeypatch, "root", PW)
    assert admin.user_command(ns(user_cmd="list")) == 0
    out = capsys.readouterr().out
    assert "bob" in out and "root" in out and "scrypt" not in out


def test_password_prompt_enforces_strength_and_match(monkeypatch, capsys):
    Terminal(
        monkeypatch,
        "short",
        "alice-longpassword1",
        "Good-Passw0rd1",
        "mismatch",
        "Good-Passw0rd1",
        "Good-Passw0rd1",
    )
    assert admin._new_password("alice") == "Good-Passw0rd1"
    out = capsys.readouterr().out
    assert "at least" in out and "username" in out and "do not match" in out


def test_user_management_refused_for_non_local_provider(workspace, monkeypatch, capsys):
    cfg = Settings.load_config()
    cfg["security"] = {"auth_provider": "ldap-ish"}
    Settings.save_config("erp", cfg)
    from natquery.auth.base import AuthProvider, register_provider

    class Other(AuthProvider):
        name = "ldap-ish"

        def credential_fields(self):
            return []

        def authenticate(self, c):
            raise NotImplementedError

    register_provider("ldap-ish", lambda c: Other())
    assert admin.user_command(ns(user_cmd="list")) == 1
    assert "only available for the built-in" in capsys.readouterr().out


# ====================================================================== policy / schema / audit commands
def test_policy_init_keeps_existing_files(workspace, capsys):
    (workspace / "policy.yaml").unlink()
    Settings.save_config("erp", {**Settings.load_config()})
    assert admin.policy_init_command() == 0
    assert (workspace / "policy.yaml").exists() and (
        workspace / "semantics.json"
    ).exists()
    (workspace / "policy.yaml").write_text("roles: {}")
    admin.policy_init_command()
    assert (
        workspace / "policy.yaml"
    ).read_text() == "roles: {}"  # never clobbers an edited policy
    assert "Keeping existing" in capsys.readouterr().out


def test_policy_check_ok_with_and_without_schema(workspace, capsys):
    assert admin.policy_check_command() == 0
    assert "no schema yet" in capsys.readouterr().out
    save_schema("erp", fakes.ERP_SCHEMA)
    assert admin.policy_check_command() == 0
    out = capsys.readouterr().out
    assert "student" in out and "sees" in out and "row-filtered tables" in out


def test_policy_check_reports_invalid_policy_and_typos(workspace, capsys):
    (workspace / "policy.yaml").write_text("roles:\n  a:\n    tabels: {}\n")
    assert admin.policy_check_command() == 1
    assert "INVALID policy" in capsys.readouterr().out
    (workspace / "policy.yaml").write_text(
        "roles:\n  a:\n    tables:\n      studentz: {columns: '*'}\n"
    )
    save_schema("erp", fakes.ERP_SCHEMA)
    assert admin.policy_check_command() == 0
    assert "studentz" in capsys.readouterr().out


def test_schema_refresh_requires_permission_and_saves(workspace, monkeypatch, capsys):
    seed_admin()
    seed_admin("stud", ["student"], {"roll_no": "1"})

    class B:
        closed = False

        def extract_schema(self):
            return fakes.ERP_SCHEMA

        def close(self):
            B.closed = True

    monkeypatch.setattr(admin, "create_backend", lambda cfg: B())
    Terminal(monkeypatch, "stud", PW)
    assert (
        admin.schema_refresh_command() == 1 and not (workspace / "schema.json").exists()
    )
    Terminal(monkeypatch, "root", PW)
    assert admin.schema_refresh_command() == 0 and B.closed
    assert "erp_users" in json.loads((workspace / "schema.json").read_text())["tables"]
    assert any(e["event"] == "schema_refresh" for e in audit_events())


def test_audit_verify_and_tail(workspace, monkeypatch, capsys):
    seed_admin()
    Terminal(monkeypatch)
    assert admin.audit_command(ns(audit_cmd="verify")) == 0
    assert "OK" in capsys.readouterr().out
    Terminal(monkeypatch, "root", PW)
    assert admin.audit_command(ns(audit_cmd="tail", n=5)) == 0
    assert "admin_login" in capsys.readouterr().out
    log = paths.base_dir() / "audit.log"
    log.write_text(log.read_text().replace("admin_login", "admin_logxx", 1))
    assert admin.audit_command(ns(audit_cmd="verify")) == 2
    assert "TAMPERED" in capsys.readouterr().out


def test_audit_tail_needs_permission(workspace, monkeypatch, capsys):
    seed_admin("stud", ["student"], {"roll_no": "1"})
    Terminal(monkeypatch, "stud", PW)
    assert admin.audit_command(ns(audit_cmd="tail", n=5)) == 1


# ====================================================================== workspace commands
def test_connect_interactive_stores_private_config_and_hides_secrets(
    monkeypatch, capsys
):
    Terminal(
        monkeypatch,
        "erp",
        "yes",
        "db.local",
        "5432",
        "reader",
        "dbpw",
        "groq",
        "gsk_key",
        "model-x",
    )
    commands.connect_command()
    cfg = Settings.load_config()
    assert (
        cfg["db_password"] == "dbpw"
        and cfg["sslmode"] == "require"
        and cfg["llm_model"] == "model-x"
    )
    assert (
        oct((paths.base_dir() / "erp" / "config.json").stat().st_mode & 0o777)
        == "0o600"
    )
    assert (paths.base_dir() / "current_db").read_text() == "erp"
    assert "dbpw" not in capsys.readouterr().out


def test_connect_blank_secrets_defer_to_environment(monkeypatch):
    Terminal(monkeypatch, "erp", "no", "h", "1", "u", "", "", "", "m")
    commands.connect_command()
    cfg = Settings.load_config()
    assert (
        "db_password" not in cfg
        and "llm_api_key" not in cfg
        and cfg["llm_provider"] == "groq"
    )


def test_connect_with_dsn_warns_and_stores(monkeypatch, capsys):
    Terminal(monkeypatch, "", "groq", "", "m")
    monkeypatch.setattr(commands, "_ask_workspace_name", lambda suggested="": "fromdsn")
    commands.connect_command("postgresql://u:p@h/fromdsn")
    assert "shell history" in capsys.readouterr().out
    assert Settings.load_config()["connection_type"] == "dsn"


def test_workspace_name_prompt_loops_on_bad_names(monkeypatch, capsys):
    Terminal(monkeypatch, "../evil", "has space", "ok-name")
    assert commands._ask_workspace_name() == "ok-name"
    assert capsys.readouterr().out.count("Invalid name") == 2


@pytest.mark.parametrize(
    "dsn,name",
    [
        ("postgresql://u:p@h:5432/mydb?sslmode=require", "mydb"),
        ("postgres://h/x", "x"),
        ("postgres://h", "unknown_db"),
        ("host=h dbname=kv user=u", "kv"),
        ("host=h", "unknown_db"),
        ("", "unknown_db"),
    ],
)
def test_dsn_name_extraction(dsn, name):
    assert commands.extract_db_name_from_dsn(dsn) == name


def test_show_config_masks_every_secret(workspace, capsys):
    commands.show_config_command()
    out = capsys.readouterr().out
    assert "dbpass-SECRET" not in out and "gsk_SECRETKEY" not in out and "reader" in out


def test_show_config_when_unconfigured(capsys):
    commands.show_config_command()
    assert "not configured" in capsys.readouterr().out.lower()


def test_reset_removes_connection_but_keeps_policy_users_audit(workspace, capsys):
    save_schema("erp", fakes.ERP_SCHEMA)
    (paths.base_dir() / "audit.log").write_text("keep")
    commands.reset_command()
    assert (
        not (workspace / "config.json").exists()
        and not (workspace / "schema.json").exists()
    )
    assert (workspace / "policy.yaml").exists() and (
        paths.base_dir() / "audit.log"
    ).read_text() == "keep"
    commands.reset_command()
    assert "Nothing to remove" in capsys.readouterr().out


def test_reset_without_workspace(capsys):
    commands.reset_command()
    assert "No active database" in capsys.readouterr().out


def test_select_database_lists_validates_and_switches(workspace, monkeypatch, capsys):
    Settings.save_config("second", {"db_name": "second"})
    (paths.base_dir() / "not a valid name").mkdir()
    Terminal(monkeypatch, "abc", "9", "2")
    commands.select_database_command()
    out = capsys.readouterr().out
    assert "erp (current)" in out and "not a valid" not in out
    assert (paths.base_dir() / "current_db").read_text() == "second"


def test_select_database_when_none(capsys):
    commands.select_database_command()
    assert "No configured databases" in capsys.readouterr().out


# ====================================================================== formatter
def test_table_single_column_and_empty(capsys):
    formatter.print_table([{"n": 1}, {"n": 2}])
    assert "- 1" in capsys.readouterr().out
    formatter.print_table([])
    assert "No results" in capsys.readouterr().out


def test_table_formats_none_numbers_and_long_text(capsys):
    formatter.print_table([{"a": None, "b": 3, "c": "x" * 300}])
    out = capsys.readouterr().out
    assert "..." in out and "x" * 120 not in out


def test_table_paginates_and_stops_on_ctrl_c(monkeypatch, capsys):
    rows = [{"a": i, "b": i} for i in range(45)]
    monkeypatch.setattr("builtins.input", lambda *a: "")
    formatter.print_table(rows, page_size=20)
    assert "3/3" in capsys.readouterr().out
    monkeypatch.setattr(
        "builtins.input", lambda *a: (_ for _ in ()).throw(KeyboardInterrupt)
    )
    formatter.print_table(rows, page_size=20)
    out = capsys.readouterr().out
    assert "Pagination stopped" in out and "3/3" not in out


def test_table_never_renders_markup_from_data(capsys):
    formatter.print_table([{"a": "[bold]x[/bold]", "b": 1}])  # must not raise
    assert "x" in capsys.readouterr().out


@pytest.mark.parametrize(
    "rows,col,expected",
    [
        ([{"a": None}, {"a": 1}], "a", "right"),
        ([{"a": "x"}], "a", "left"),
        ([{"a": None}], "a", "left"),
        ([], "a", "left"),
    ],
)
def test_alignment_inference(rows, col, expected):
    assert formatter._infer_alignment(rows, col) == expected


# ====================================================================== interactive shell
@pytest.fixture
def make_app(tmp_path):
    def build(**kw):
        svc, ctx = fakes.make_service(tmp_path, **kw)
        app = NatQueryApp(svc, ctx.provider, ctx.policy, ctx.audit)
        app.ctx = ctx
        return app

    return build


def run_shell(app, monkeypatch, *answers):
    t = Terminal(monkeypatch, *answers)
    shell.start_shell(app)
    return t


def test_shell_login_query_and_exit(make_app, monkeypatch, capsys):
    app = make_app(llm=fakes.FakeLLM("SELECT name FROM students"))
    run_shell(app, monkeypatch, "alice", fakes.PASSWORD, "list names", "exit")
    out = capsys.readouterr().out
    assert "Logged in as alice" in out and "1 row(s)" in out and "ref:" in out
    assert "SELECT" not in out.split("Logged in")[1].replace(
        "SELECT name", ""
    )  # student never sees SQL


def test_shell_login_failure_then_lockout_message(make_app, monkeypatch, capsys):
    app = make_app()
    run_shell(app, monkeypatch, "alice", "bad", "alice", "bad", "alice", "bad")
    out = capsys.readouterr().out
    assert out.count("Invalid credentials") == 3 and "Too many failed attempts" in out


def test_shell_login_aborted_with_ctrl_d(make_app, monkeypatch, capsys):
    run_shell(make_app(), monkeypatch)
    assert "Logged in" not in capsys.readouterr().out


def test_shell_commands_whoami_role_help_logout(make_app, monkeypatch, capsys):
    app = make_app(
        users=[("multi", ["student", "teacher"], {"roll_no": "1", "emp_id": "F1"})]
    )
    run_shell(
        app,
        monkeypatch,
        "multi",
        fakes.PASSWORD,
        "/whoami",
        "/role",
        "/role admin",
        "/role student",
        "/whoami",
        "/help",
        "",
        "/logout",
        "multi",
        fakes.PASSWORD,
        "exit",
    )
    out = capsys.readouterr().out
    assert "active_role=teacher" in out and "do not hold that role" in out
    assert "Active role: student" in out and "active_role=student" in out
    assert "Logged out" in out and "Commands:" in out


def test_shell_sql_and_plan_toggles_obey_permissions(make_app, monkeypatch, capsys):
    app = make_app(
        llm=fakes.FakeLLM("SELECT name FROM students"),
        security={"enable_optimizer": True},
    )
    run_shell(app, monkeypatch, "alice", fakes.PASSWORD, "/sql", "/plan", "exit")
    out = capsys.readouterr().out
    assert (
        "not permitted to view generated SQL" in out
        and "not permitted to view execution plans" in out
    )
    run_shell(
        app, monkeypatch, "root", fakes.PASSWORD, "/sql", "/plan", "list names", "exit"
    )
    out = capsys.readouterr().out
    assert "Show SQL: ON" in out and "Executed SQL" in out and "Execution Plan" in out


def test_shell_shows_errors_without_internals(make_app, monkeypatch, capsys):
    app = make_app(llm=fakes.FakeLLM("SELECT pg_read_file('/etc/passwd')"))
    run_shell(app, monkeypatch, "alice", fakes.PASSWORD, "show me a file", "exit")
    out = capsys.readouterr().out
    assert (
        "blocked by security policy" in out
        and "ref:" in out
        and "pg_read_file" not in out
    )


def test_shell_survives_unexpected_exceptions(make_app, monkeypatch, capsys):
    app = make_app()
    real = app.ask
    app.ask = lambda *a, **k: (_ for _ in ()).throw(
        RuntimeError("boom internal detail")
    )
    run_shell(app, monkeypatch, "alice", fakes.PASSWORD, "hello", "exit")
    out = capsys.readouterr().out
    assert "Something went wrong" in out and "boom internal detail" not in out
    app.ask = real


def test_shell_relogs_in_after_session_expiry(make_app, monkeypatch, capsys):
    app = make_app(llm=fakes.FakeLLM("SELECT name FROM students"))
    original_ask = app.ask
    calls = {"n": 0}

    def expire_on_second(session, q, role=None):
        calls["n"] += 1
        if calls["n"] == 2:
            app.logout(session)  # token revoked between requests
        return original_ask(session, q, role=role)

    app.ask = expire_on_second
    run_shell(
        app,
        monkeypatch,
        "alice",
        fakes.PASSWORD,
        "first",
        "second",
        "alice",
        fakes.PASSWORD,
        "third",
        "exit",
    )
    out = capsys.readouterr().out
    assert "session has expired" in out and out.count("Logged in as alice") == 1
    assert calls["n"] == 3 and out.count("1 row(s)") == 2  # first and third succeeded


def test_shell_ctrl_c_during_prompt_exits_cleanly(make_app, monkeypatch, capsys):
    run_shell(make_app(), monkeypatch, "alice", fakes.PASSWORD, KeyboardInterrupt())
    assert "Goodbye" in capsys.readouterr().out


def test_plan_output_formats(capsys):
    shell.format_plan_output(None, [])
    shell.format_plan_output(
        {"total_cost": 1.5, "planning_time_ms": 0.2, "plan_rows": 3},
        [{"type": "index", "reason": "r", "sql": "CREATE INDEX i ON t(a);"}],
    )
    out = capsys.readouterr().out
    assert "No plan data" in out and "Total Cost: 1.50" in out and "CREATE INDEX" in out
    shell.format_plan_output({"total_cost": 1}, [])
    assert "No optimization recommendations" in capsys.readouterr().out


def test_show_error_includes_reference(capsys):
    e = QueryRejected("nope")
    e.ref = "abc123"
    shell._show_error(e)
    assert "nope" in capsys.readouterr().out
    e.ref = None
    shell._show_error(e)
    assert "ref:" not in capsys.readouterr().out


# ====================================================================== main()
@pytest.mark.parametrize(
    "argv",
    [
        ["connect"],
        ["connect", "postgres://x/y"],
        ["select"],
        ["reset"],
        ["config"],
        ["schema", "refresh"],
        ["policy", "init"],
        ["policy", "check"],
        ["user", "list"],
        ["audit", "verify"],
        ["audit", "tail", "-n", "3"],
        ["user", "add", "a", "--roles", "r", "--attr", "k=v", "--attr", "x=y"],
        ["user", "attrs", "a", "k=v"],
        ["user", "passwd", "a"],
        ["user", "roles", "a", "r,s"],
    ],
)
def test_parser_accepts_documented_commands(argv):
    assert _build_parser().parse_args(argv)


@pytest.mark.parametrize(
    "argv",
    [
        ["user"],
        ["audit"],
        ["schema"],
        ["policy", "nuke"],
        ["user", "add", "x"],
        ["bogus"],
    ],
)
def test_parser_rejects_bad_usage(argv):
    with pytest.raises(SystemExit):
        _build_parser().parse_args(argv)


def run_main(monkeypatch, *argv):
    monkeypatch.setattr("sys.argv", ["natquery", *argv])
    return main()


def test_main_dispatches(monkeypatch, workspace):
    seen = []
    monkeypatch.setattr(commands, "show_config_command", lambda: seen.append("config"))
    monkeypatch.setattr(commands, "reset_command", lambda: seen.append("reset"))
    monkeypatch.setattr(
        commands, "select_database_command", lambda: seen.append("select")
    )
    monkeypatch.setattr(
        commands, "connect_command", lambda dsn: seen.append(("connect", dsn))
    )
    monkeypatch.setattr(
        admin, "policy_check_command", lambda: seen.append("pcheck") or 0
    )
    monkeypatch.setattr(admin, "policy_init_command", lambda: seen.append("pinit") or 0)
    monkeypatch.setattr(
        admin, "schema_refresh_command", lambda: seen.append("schema") or 0
    )
    monkeypatch.setattr(
        admin, "user_command", lambda a: seen.append(("user", a.user_cmd)) or 0
    )
    monkeypatch.setattr(
        admin, "audit_command", lambda a: seen.append(("audit", a.audit_cmd)) or 0
    )
    for argv in (
        ["config"],
        ["reset"],
        ["select"],
        ["connect", "d"],
        ["policy", "check"],
        ["policy", "init"],
        ["schema", "refresh"],
        ["user", "list"],
        ["audit", "verify"],
    ):
        assert run_main(monkeypatch, *argv) == 0
    assert seen == [
        "config",
        "reset",
        "select",
        ("connect", "d"),
        "pcheck",
        "pinit",
        "schema",
        ("user", "list"),
        ("audit", "verify"),
    ]


def test_main_propagates_command_exit_codes(monkeypatch):
    monkeypatch.setattr(admin, "policy_check_command", lambda: 1)
    assert run_main(monkeypatch, "policy", "check") == 1


def test_main_error_handling(monkeypatch, capsys):
    def raising(exc):
        def f(*a):
            raise exc

        return f

    monkeypatch.setattr(
        commands,
        "show_config_command",
        raising(ConfigError(internal="bad policy at x")),
    )
    assert (
        run_main(monkeypatch, "config") == 1
        and "bad policy at x" in capsys.readouterr().out
    )
    monkeypatch.setattr(
        commands,
        "show_config_command",
        raising(QueryRejected("safe text", internal="SECRET")),
    )
    assert run_main(monkeypatch, "config") == 1
    out = capsys.readouterr().out
    assert "safe text" in out and "SECRET" not in out
    monkeypatch.setattr(
        commands, "show_config_command", raising(RuntimeError("Run: natquery connect"))
    )
    assert (
        run_main(monkeypatch, "config") == 1
        and "natquery connect" in capsys.readouterr().out
    )
    monkeypatch.setattr(commands, "show_config_command", raising(KeyboardInterrupt()))
    assert run_main(monkeypatch, "config") == 130
    monkeypatch.setattr(commands, "show_config_command", raising(EOFError()))
    assert run_main(monkeypatch, "config") == 130


def test_main_without_command_requires_configuration(monkeypatch, capsys):
    assert run_main(monkeypatch) == 1
    assert "natquery connect" in capsys.readouterr().out


def test_main_without_command_runs_shell_and_closes_app(monkeypatch, workspace):
    import natquery.app as appmod

    state = {}

    class FakeApp:
        def close(self):
            state["closed"] = True

    monkeypatch.setattr(
        appmod.NatQueryApp, "from_workspace", classmethod(lambda cls, **kw: FakeApp())
    )
    monkeypatch.setattr(
        shell, "start_shell", lambda app: state.setdefault("shell", app)
    )
    assert run_main(monkeypatch) == 0
    assert state["closed"] and isinstance(state["shell"], FakeApp)


def test_main_closes_app_even_if_shell_crashes(monkeypatch, workspace):
    import natquery.app as appmod

    state = {}

    class FakeApp:
        def close(self):
            state["closed"] = True

    monkeypatch.setattr(
        appmod.NatQueryApp, "from_workspace", classmethod(lambda cls, **kw: FakeApp())
    )
    monkeypatch.setattr(
        shell, "start_shell", lambda app: (_ for _ in ()).throw(RuntimeError("x"))
    )
    assert run_main(monkeypatch) == 1 and state["closed"]


# ====================================================================== composition root
def test_from_workspace_builds_everything_with_injected_parts(workspace, monkeypatch):
    save_schema("erp", fakes.ERP_SCHEMA)
    app = NatQueryApp.from_workspace(
        llm=fakes.FakeLLM("SELECT name FROM students"), backend=fakes.FakeBackend()
    )
    seed_admin("stud", ["student"], {"roll_no": "1"})
    s = app.login({"username": "stud", "password": PW})
    assert app.ask(s, "list").rows and app.service.db_name == "erp"
    app.logout(s)
    app.close()


def test_from_workspace_errors_are_actionable(workspace):
    with pytest.raises(ConfigError, match="schema refresh"):
        NatQueryApp.from_workspace(llm=fakes.FakeLLM(), backend=fakes.FakeBackend())
    save_schema("erp", fakes.ERP_SCHEMA)
    (workspace / "policy.yaml").unlink()
    with pytest.raises(ConfigError, match="policy init"):
        NatQueryApp.from_workspace(llm=fakes.FakeLLM(), backend=fakes.FakeBackend())


def test_policy_warnings_are_logged_not_fatal(workspace):
    save_schema("erp", {"tables": {"students": {"columns": {"roll_no": "text"}}}})
    NatQueryApp.from_workspace(llm=fakes.FakeLLM(), backend=fakes.FakeBackend())
    log = (paths.base_dir() / "system.log").read_text()
    assert "policy_warning" in log
