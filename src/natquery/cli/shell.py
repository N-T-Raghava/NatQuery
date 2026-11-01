import time

from rich import print
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from natquery.app import NatQueryApp
from natquery.auth.session import Session
from natquery.cli.admin import prompt_credentials
from natquery.cli.formatter import print_table
from natquery.errors import AuthenticationError, NatQueryError, SessionExpiredError
from natquery.observability.logger import NatQueryLogger

console = Console()
MAX_LOGIN_ATTEMPTS = 3

HELP = (
    "[dim]Ask a question in plain English.  Commands: /whoami  /role [name]  /sql  /plan  "
    "/logout  /help  exit[/dim]"
)


def show_banner():
    banner = Text(
        """
███╗   ██╗ █████╗ ████████╗ ██████╗ ██╗   ██╗███████╗██████╗ ██╗   ██╗
████╗  ██║██╔══██╗╚══██╔══╝██╔═══██╗██║   ██║██╔════╝██╔══██╗╚██╗ ██╔╝
██╔██╗ ██║███████║   ██║   ██║   ██║██║   ██║█████╗  ██████╔╝ ╚████╔╝ 
██║╚██╗██║██╔══██║   ██║   ██║▄▄ ██║██║   ██║██╔══╝  ██╔══██╗  ╚██╔╝  
██║ ╚████║██║  ██║   ██║   ╚██████╔╝╚██████╔╝███████╗██║  ██║   ██║   
╚═╝  ╚═══╝╚═╝  ╚═╝   ╚═╝    ╚══▀▀═╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝   ╚═╝   
""",
        style="bold cyan",
    )
    print(Panel(banner, title="NatQuery CLI", border_style="green"))


def _show_error(e: NatQueryError) -> None:
    ref = f"  [dim](ref: {e.ref})[/dim]" if e.ref else ""
    print(f"[bold red]{e.user_message}[/bold red]{ref}")


def login_loop(app: NatQueryApp):
    """Prompt for credentials (whatever the configured provider asks for)."""
    for attempt in range(1, MAX_LOGIN_ATTEMPTS + 1):
        try:
            return app.login(prompt_credentials(app.provider))
        except AuthenticationError as e:
            _show_error(e)
            time.sleep(attempt)  # slow down guessing
        except (EOFError, KeyboardInterrupt):
            return None
    print("[bold red]Too many failed attempts.[/bold red]")
    return None


def format_plan_output(summary, suggestions):
    if not summary and not suggestions:
        print("[dim]No plan data available[/dim]")
        return
    if summary:
        metrics = []
        if summary.get("planning_time_ms"):
            metrics.append(f"Planning Time: {summary['planning_time_ms']:.2f}ms")
        if summary.get("total_cost"):
            metrics.append(f"Total Cost: {summary['total_cost']:.2f}")
        if summary.get("plan_rows"):
            metrics.append(f"Planned Rows: {summary['plan_rows']}")
        if metrics:
            print(
                Panel("\n".join(metrics), title="Execution Plan", border_style="yellow")
            )
    if suggestions:
        lines = []
        for i, sugg in enumerate(suggestions, 1):
            lines.append(f"[bold]{i}. {sugg.get('type', 'unknown').upper()}[/bold]")
            if sugg.get("reason"):
                lines.append(f"   Reason: {sugg['reason']}")
            if sugg.get("sql"):
                lines.append(f"   SQL: {sugg['sql']}")
            lines.append("")
        print(
            Panel("\n".join(lines), title="Index Recommendations", border_style="cyan")
        )
    else:
        print("[dim]No optimization recommendations[/dim]")


def start_shell(app: NatQueryApp):
    show_banner()
    session: Session | None = login_loop(app)
    if session is None:
        return

    show_sql = show_plan = False
    active_role = None  # None -> highest-priority role the policy knows for this user

    def role_obj():
        return app.service.role_for(session.token, active_role)

    print(f"[bold green]Logged in as {session.identity.username}.[/bold green]")
    print(HELP + "\n")

    while True:
        try:
            query = console.input(
                f"[bold blue]{session.identity.username}> [/bold blue]"
            ).strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[bold red]Goodbye![/bold red]")
            break
        if not query:
            continue
        low = query.lower()

        try:
            if low in ("exit", "quit"):
                print("[bold red]Goodbye![/bold red]")
                break
            if low == "/help":
                print(HELP)
                continue
            if low == "/logout":
                app.logout(session)
                print("[yellow]Logged out.[/yellow]")
                session = login_loop(app)
                if session is None:
                    break
                active_role = None
                continue
            if low == "/whoami":
                r = role_obj()
                print(
                    f"user={session.identity.username}  roles={sorted(session.identity.roles)}  active_role={r.name}"
                )
                continue
            if low.startswith("/role"):
                parts = query.split()
                known = [r for r in session.identity.roles if r in app.policy.roles]
                if len(parts) == 1:
                    print(
                        f"Your roles: {sorted(known)}  (active: {role_obj().name})  Use: /role <name>"
                    )
                elif parts[1] in known:
                    active_role = parts[1]
                    print(f"[yellow]Active role:[/yellow] {active_role}")
                else:
                    print("[red]You do not hold that role.[/red]")
                continue
            if low == "/sql":
                if not role_obj().can("can_view_sql"):
                    print(
                        "[red]Your role is not permitted to view generated SQL.[/red]"
                    )
                else:
                    show_sql = not show_sql
                    print(f"[yellow]Show SQL:[/yellow] {'ON' if show_sql else 'OFF'}")
                continue
            if low == "/plan":
                if not role_obj().can("can_view_plan"):
                    print(
                        "[red]Your role is not permitted to view execution plans.[/red]"
                    )
                else:
                    show_plan = not show_plan
                    print(f"[yellow]Show Plan:[/yellow] {'ON' if show_plan else 'OFF'}")
                continue

            resp = app.ask(session, query, role=active_role)
            if show_sql and resp.generated_sql:
                print(
                    Panel(resp.generated_sql, title="Executed SQL", border_style="cyan")
                )
            print_table(resp.rows)
            for note in resp.notes:
                print(f"[dim]{note}[/dim]")
            if show_plan:
                format_plan_output(resp.plan_summary, resp.suggestions)
            print(
                f"[dim]{resp.rows_returned} row(s) in {resp.execution_time_ms:.0f} ms  (ref: {resp.ref})[/dim]"
            )

        except SessionExpiredError as e:
            _show_error(e)
            session = login_loop(app)
            if session is None:
                break
            active_role = None
        except NatQueryError as e:
            _show_error(e)
        except KeyboardInterrupt:
            print("[yellow]Cancelled.[/yellow]")
        except Exception as e:  # last resort: log details, show nothing sensitive
            NatQueryLogger.log_event(
                "ERROR",
                "shell_unhandled",
                details={"error": f"{type(e).__name__}: {e}"},
            )
            print(
                "[bold red]Something went wrong. Please try again or contact an administrator.[/bold red]"
            )
