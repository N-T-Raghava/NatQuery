import argparse
import sys

from rich import print

from natquery.errors import ConfigError, NatQueryError


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="natquery", description="Ask your database questions in English."
    )
    sub = p.add_subparsers(dest="cmd")

    c = sub.add_parser("connect", help="configure database + LLM for a workspace")
    c.add_argument("dsn", nargs="?")
    sub.add_parser("select", help="switch the active workspace")
    sub.add_parser("reset", help="remove connection config + cached schema")
    sub.add_parser("config", help="show configuration (secrets masked)")

    s = sub.add_parser("schema", help="schema administration")
    s.add_argument("schema_cmd", choices=["refresh", "hints"])
    s.add_argument(
        "--db-role", default=None, help="hints: database role that may read the data"
    )

    po = sub.add_parser("policy", help="access-policy administration")
    po.add_argument("policy_cmd", choices=["init", "check"])

    u = sub.add_parser("user", help="manage built-in accounts (needs an admin login)")
    us = u.add_subparsers(dest="user_cmd", required=True)
    a = us.add_parser("add")
    a.add_argument("username")
    a.add_argument(
        "--roles", required=True, help="comma-separated role names from the policy"
    )
    a.add_argument(
        "--attr",
        action="append",
        default=[],
        help="key=value, e.g. roll_no=21CS10001 (repeatable)",
    )
    us.add_parser("list")
    r = us.add_parser("roles")
    r.add_argument("username")
    r.add_argument("roles")
    t = us.add_parser("attrs")
    t.add_argument("username")
    t.add_argument("attr", nargs="+")
    for name in ("passwd", "disable", "enable"):
        us.add_parser(name).add_argument("username")

    au = sub.add_parser("audit", help="audit log")
    aus = au.add_subparsers(dest="audit_cmd", required=True)
    aus.add_parser("verify")
    tl = aus.add_parser("tail")
    tl.add_argument("-n", type=int, default=20)
    return p


def main() -> int:
    args = _build_parser().parse_args()
    try:
        if args.cmd == "connect":
            from natquery.cli.commands import connect_command

            connect_command(args.dsn)
        elif args.cmd == "select":
            from natquery.cli.commands import select_database_command

            select_database_command()
        elif args.cmd == "reset":
            from natquery.cli.commands import reset_command

            reset_command()
        elif args.cmd == "config":
            from natquery.cli.commands import show_config_command

            show_config_command()
        elif args.cmd == "schema":
            from natquery.cli.admin import schema_hints_command, schema_refresh_command

            if args.schema_cmd == "hints":
                return schema_hints_command(args.db_role)
            return schema_refresh_command()
        elif args.cmd == "policy":
            from natquery.cli import admin

            return (
                admin.policy_init_command()
                if args.policy_cmd == "init"
                else admin.policy_check_command()
            )
        elif args.cmd == "user":
            from natquery.cli.admin import user_command

            return user_command(args)
        elif args.cmd == "audit":
            from natquery.cli.admin import audit_command

            return audit_command(args)
        else:
            return _run_shell()
    except ConfigError as e:  # operator-facing: startup problems are shown in full
        print(f"[bold red]Configuration problem:[/bold red] {e.internal}")
        return 1
    except NatQueryError as e:
        print(f"[bold red]{e.user_message}[/bold red]")
        return 1
    except RuntimeError as e:
        print(f"[bold red]{e}[/bold red]")
        return 1
    except (KeyboardInterrupt, EOFError):
        print()
        return 130
    return 0


def _run_shell() -> int:
    from natquery.app import NatQueryApp
    from natquery.cli.shell import start_shell
    from natquery.config.settings import Settings

    if not Settings.exists():
        print("NatQuery not configured.\nRun: natquery connect")
        return 1
    print("\n[bold cyan]Starting NatQuery...[/bold cyan]\n")
    app = NatQueryApp.from_workspace()
    try:
        start_shell(app)
    finally:
        app.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
