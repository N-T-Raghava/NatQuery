from rich import print
from rich.panel import Panel
from rich.text import Text
from rich.console import Console
from natquery.orchestration.pipeline import run_query
from natquery.llm.client import generate_sql
from natquery.cli.formatter import print_table

console = Console()


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

    print(Panel(banner, title="⚡ NatQuery CLI", border_style="green"))


def format_plan_output(summary, suggestions):
    """Format execution plan and index recommendations for display."""
    if not summary and not suggestions:
        print("[dim]No plan data available[/dim]")
        return

    # Display execution metrics
    if summary:
        metrics = []
        if summary.get("execution_time_ms"):
            metrics.append(f"Execution Time: {summary['execution_time_ms']:.2f}ms")
        if summary.get("planning_time_ms"):
            metrics.append(f"Planning Time: {summary['planning_time_ms']:.2f}ms")
        if summary.get("total_cost"):
            metrics.append(f"Total Cost: {summary['total_cost']:.2f}")
        if summary.get("plan_rows"):
            metrics.append(f"Planned Rows: {summary['plan_rows']}")

        if metrics:
            print(
                Panel(
                    "\n".join(metrics),
                    title="Execution Plan",
                    border_style="yellow",
                )
            )

    # Display index recommendations
    if suggestions and len(suggestions) > 0:
        suggestion_lines = []
        for i, sugg in enumerate(suggestions, 1):
            sugg_type = sugg.get("type", "unknown")
            reason = sugg.get("reason", "")
            suggestion_lines.append(f"[bold]{i}. {sugg_type.upper()}[/bold]")
            if reason:
                suggestion_lines.append(f"   Reason: {reason}")
            if sugg.get("sql"):
                suggestion_lines.append(f"   SQL: {sugg['sql']}")
            suggestion_lines.append("")

        print(
            Panel(
                "\n".join(suggestion_lines),
                title="Index Recommendations",
                border_style="cyan",
            )
        )
    elif suggestions is not None and len(suggestions) == 0:
        print("[dim]No optimization recommendations[/dim]")


def start_shell():
    show_banner()
    show_sql = False
    show_plan = False
    print("[bold green]NatQuery ready.[/bold green]")
    print(
        "[dim]Commands: /sql (toggle SQL), /plan (toggle execution plan), exit[/dim]\n"
    )

    while True:
        query = console.input("[bold blue]> [/bold blue]")

        if query.lower() in ["exit", "quit"]:
            print("[bold red]Goodbye![/bold red]")
            break

        # Toggle SQL display
        if query.strip() == "/sql":
            show_sql = not show_sql
            status = "ON" if show_sql else "OFF"
            print(f"[yellow]Show SQL:[/yellow] {status}")
            continue

        # Toggle plan display
        if query.strip() == "/plan":
            show_plan = not show_plan
            status = "ON" if show_plan else "OFF"
            print(f"[yellow]Show Plan:[/yellow] {status}")
            continue

        if not query.strip():
            continue

        try:
            query_output = run_query(query)

            # Handle both old (list) and new (dict) return formats
            if isinstance(query_output, dict):
                result = query_output["result"]
                summary = query_output.get("summary")
                suggestions = query_output.get("suggestions")
            else:
                result = query_output
                summary = None
                suggestions = None

            if show_sql:
                generated_sql = generate_sql(query)
                print(Panel(generated_sql, title="Generated SQL", border_style="cyan"))

            print_table(result)

            if show_plan:
                format_plan_output(summary, suggestions)

        except Exception as e:
            print(f"[bold red]Error:[/bold red] {e}")
