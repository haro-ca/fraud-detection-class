from datetime import datetime

import pyfiglet
import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

app = typer.Typer(help="HOUND — fraud detection pipeline CLI")
console = Console()


def banner():
    text = pyfiglet.figlet_format("HOUND", font="slant")
    console.print(f"[bold red]{text}[/bold red]", end="")
    console.print("[dim]fraud detection pipeline[/dim]\n")


@app.command()
def run(
    start: str = typer.Option(None, help="Start timestamp (ISO format)"),
    end: str = typer.Option(None, help="End timestamp (ISO format)"),
    limit: int = typer.Option(None, help="Max applications to process"),
    store: str = typer.Option("duckdb", help="OLAP store type (duckdb or databricks)"),
    db_path: str = typer.Option("hound.db", help="DuckDB file path"),
):
    """Trigger the fraud detection pipeline."""
    banner()
    from src.etl import create_store, run_pipeline

    start_dt = datetime.fromisoformat(start) if start else None
    end_dt = datetime.fromisoformat(end) if end else None

    olap = create_store(store, db_path=db_path) if store == "duckdb" else create_store(store)

    console.print("[bold]Running pipeline...[/bold]")
    if start_dt or end_dt:
        console.print(f"  Window: {start_dt or '...'} → {end_dt or '...'}")
    if limit:
        console.print(f"  Limit: {limit} applications")

    result = run_pipeline(olap, start=start_dt, end=end_dt, limit=limit)

    if result["processed"] == 0:
        console.print("\n[yellow]No new applications found.[/yellow]")
    else:
        console.print(
            Panel(
                f"[green]Processed {result['processed']} applications[/green]\n"
                f"Watermark: {result.get('watermark', 'N/A')}",
                title="Pipeline Complete",
            )
        )


@app.command()
def status(
    store: str = typer.Option("duckdb", help="OLAP store type"),
    db_path: str = typer.Option("hound.db", help="DuckDB file path"),
):
    """Show summary of application statuses."""
    banner()
    from src.etl.stage import create_store as _create_store

    olap = _create_store(store, db_path=db_path) if store == "duckdb" else _create_store(store)
    apps = olap.query_applications_since(None)

    if apps.is_empty():
        console.print("[yellow]No applications in OLAP store.[/yellow]")
        return

    table = Table(title="Application Status Summary")
    table.add_column("Status", style="bold")
    table.add_column("Count", justify="right")

    counts = apps.group_by("status").len().sort("status")
    for row in counts.iter_rows(named=True):
        status_val = row["status"]
        count = row["len"]
        color = {"approved": "green", "rejected": "red", "pending": "yellow"}.get(
            status_val, "white"
        )
        table.add_row(f"[{color}]{status_val}[/{color}]", str(count))

    table.add_row("[bold]Total[/bold]", f"[bold]{len(apps)}[/bold]")
    console.print(table)


@app.command()
def inspect(
    application_id: int = typer.Argument(help="Application ID to inspect"),
    store: str = typer.Option("duckdb", help="OLAP store type"),
    db_path: str = typer.Option("hound.db", help="DuckDB file path"),
):
    """Inspect fraud results for a specific application."""
    banner()
    import duckdb

    conn = duckdb.connect(db_path, read_only=True)

    app_row = conn.execute(
        "SELECT * FROM applications WHERE id = ?", [application_id]
    ).fetchone()

    if app_row is None:
        console.print(f"[red]Application {application_id} not found in OLAP store.[/red]")
        conn.close()
        return

    app_cols = [d[0] for d in conn.description]
    app_dict = dict(zip(app_cols, app_row))

    console.print(
        Panel(
            f"[bold]{app_dict['applicant_name']}[/bold] ({app_dict['email']})\n"
            f"Income: ${app_dict['annual_income']:,.2f}  |  "
            f"Requested: ${app_dict['requested_amount']:,.2f}\n"
            f"Status: [{'green' if app_dict['status'] == 'approved' else 'red' if app_dict['status'] == 'rejected' else 'yellow'}]"
            f"{app_dict['status']}[/]\n"
            f"Created: {app_dict['created_at']}",
            title=f"Application #{application_id}",
        )
    )

    results = conn.execute(
        "SELECT rule_name, triggered, score, details FROM fraud_results WHERE application_id = ? ORDER BY rule_name",
        [application_id],
    ).fetchall()
    conn.close()

    if not results:
        console.print("[yellow]No fraud results found for this application.[/yellow]")
        return

    table = Table(title="Fraud Rules")
    table.add_column("Rule", style="bold")
    table.add_column("Triggered", justify="center")
    table.add_column("Score", justify="right")
    table.add_column("Details")

    for rule_name, triggered, score, details in results:
        triggered_str = "[red]YES[/red]" if triggered else "[green]no[/green]"
        score_color = "red" if score > 50 else "yellow" if score > 25 else "green"
        table.add_row(rule_name, triggered_str, f"[{score_color}]{score:.1f}[/{score_color}]", details)

    console.print(table)


@app.command()
def dashboard(
    db_path: str = typer.Option("hound.db", help="DuckDB file path"),
    port: int = typer.Option(8501, help="Port for the Streamlit server"),
):
    """Launch the Streamlit analytics dashboard."""
    import os
    import subprocess
    import sys

    banner()
    console.print("[bold]Launching dashboard...[/bold]")
    console.print(f"  DuckDB: {db_path}")
    console.print(f"  Port: {port}\n")

    env = os.environ.copy()
    env["HOUND_DB_PATH"] = db_path

    dashboard_path = os.path.join(os.path.dirname(__file__), "..", "dashboard", "app.py")
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", dashboard_path, "--server.port", str(port)],
        env=env,
    )


if __name__ == "__main__":
    app()
