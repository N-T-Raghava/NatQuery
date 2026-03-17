import sys
from natquery.cli.commands import connect_command, reset_command, show_config_command
from natquery.cli.shell import start_shell
from natquery.config.settings import Settings
from natquery.orchestration.workspace import initialize_workspace


def main():

    if len(sys.argv) > 1:

        cmd = sys.argv[1]

        if cmd == "connect":
            # Pass DSN if provided: natquery connect <dsn>
            dsn = sys.argv[2] if len(sys.argv) > 2 else None
            connect_command(dsn)
            return

        elif cmd == "reset":
            reset_command()
            return

        elif cmd == "config":
            show_config_command()
            return

        else:
            print(f"Unknown command: {cmd}")
            return

    # No subcommand → start REPL

    if not Settings.exists():
        print("NatQuery not configured.")
        print("Run: natquery connect")
        return

    print("\n[bold cyan]Starting NatQuery...[/bold cyan]\n")
    initialize_workspace()
    start_shell()
