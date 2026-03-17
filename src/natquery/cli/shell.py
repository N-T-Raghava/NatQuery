# src/natquery/cli/shell.py
from rich import print
from rich.panel import Panel
from rich.text import Text
from rich.console import Console
from natquery.orchestration.pipeline import run_query
from natquery.llm.client import generate_sql

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


def start_shell():
    show_banner()  # 👈 ADD THIS
    show_sql = False
    # print("[bold green]NatQuery ready.[/bold green] Type 'exit' to quit.\n")
    # print("NatQuery CLI. Type 'exit' to quit.")
    print("[bold green]NatQuery ready.[/bold green]")
    print("[dim]Commands: /sql (toggle SQL), exit[/dim]\n")

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

        try:
            # ✅ Step 1: Generate SQL from LLM
            generated_sql = generate_sql(query)

            # ✅ Step 2: Execute using pipeline
            result = run_query(query)

            # ✅ Step 3: Display nicely
            if show_sql:
                print(
                    Panel(generated_sql, title="🧠 Generated SQL", border_style="cyan")
                )

            print(Panel(str(result), title="📊 Result", border_style="green"))

        except Exception as e:
            print(f"[bold red]Error:[/bold red] {e}")
