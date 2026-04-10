from rich.table import Table
from rich.console import Console
from rich.panel import Panel
from rich import box
import math

console = Console()


def _is_number(value):
    return isinstance(value, (int, float))


def _infer_alignment(data, column):
    """
    Infer alignment based on column values.
    """
    for row in data:
        val = row.get(column)
        if val is not None:
            if _is_number(val):
                return "right"
            else:
                return "left"
    return "left"


def _truncate(text, max_length=50):
    """
    Soft truncate long values (for extremely wide columns).
    """
    text = str(text)
    if len(text) > max_length:
        return text[: max_length - 3] + "..."
    return text


def _format_value(value):
    """
    Clean formatting of cell values.
    """
    if value is None:
        return ""
    return str(value)


def print_table(
    data: list[dict],
    title: str = "#### Result ####",
    page_size: int = 20,
    wrap: bool = True,
):
    """
    Advanced table printer:
    - smart alignment
    - pagination
    - wrapping
    - adaptive formatting
    """

    if not data:
        console.print("[yellow]No results found.[/yellow]")
        return

    columns = list(data[0].keys())

    # Special case: single column → vertical list
    if len(columns) == 1:
        col = columns[0]
        console.print(Panel(f"[bold cyan]{col}[/bold cyan]", title=title))

        for row in data:
            console.print(f"- {row.get(col)}")

        return

    total_rows = len(data)
    total_pages = math.ceil(total_rows / page_size)

    for page in range(total_pages):

        start = page * page_size
        end = start + page_size
        chunk = data[start:end]

        table = Table(
            title=f"{title} (Page {page+1}/{total_pages})",
            show_header=True,
            header_style="bold magenta",
            box=box.SIMPLE_HEAVY,
        )

        # Add columns with smart alignment
        for col in columns:
            justify = _infer_alignment(chunk, col)

            table.add_column(
                col,
                justify=justify,
                overflow="fold" if wrap else "ellipsis",
                no_wrap=not wrap,
            )

        # Add rows
        for row in chunk:
            row_values = []

            for col in columns:
                val = _format_value(row.get(col))
                val = _truncate(val, 80)
                row_values.append(val)

            table.add_row(*row_values)

        console.print(table)

        # Pagination pause
        if page < total_pages - 1:
            console.print("[dim]Press Enter for next page...[/dim]")
            input()
