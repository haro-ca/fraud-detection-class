import logging

import polars as pl
import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from scripts.pipeline import (
    load_applications,
    load_transactions,
    run_all,
    write_fraud_results,
    update_application_statuses,
)

console = Console()
app = typer.Typer(help="Fraud detection CLI", rich_markup_mode="rich")

logger = logging.getLogger("fraud_pipeline")


def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


@app.command()
def run(
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output"
    ),
):
    """
    Run the fraud detection pipeline.
    """
    setup_logging(verbose)
    console.print("\n[bold blue]Fraud Detection Pipeline[/bold blue]\n")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("[cyan]Loading data...", total=None)
        transactions = load_transactions()
        applications = load_applications()
        progress.update(
            task,
            description=f"[green]Loaded {len(transactions)} transactions, {len(applications)} applications",
        )

        task = progress.add_task("[cyan]Running fraud rules...", total=None)
        results = run_all(transactions, applications)
        triggered_count = results.filter(pl.col("triggered")).height
        progress.update(task, description="[green]Fraud rules complete")

        task = progress.add_task("[cyan]Writing results...", total=None)
        write_fraud_results(results)
        update_application_statuses(results)
        progress.update(task, description="[green]Results saved")

    table = Table(title="Pipeline Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Total Flags", str(len(results)))
    table.add_row("Triggered", str(triggered_count))
    table.add_row("Clean", str(len(results) - triggered_count))
    table.add_row("Applications Processed", str(len(applications)))

    console.print("\n")
    console.print(table)
    console.print("\n[bold green]Pipeline complete![/bold green]\n")


@app.command()
def etl(
    days: int = typer.Option(7, "--days", "-d", help="Number of days to load"),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output"
    ),
):
    """
    Run the ETL pipeline to load data from Neon to DuckDB.
    """
    from scripts.etl import run_etl

    setup_logging(verbose)
    console.print(f"\n[bold blue]ETL Pipeline[/bold blue] (last {days} days)\n")
    run_etl(days)
    console.print("\n[bold green]ETL complete![/bold green]\n")


@app.command()
def status():
    """
    Show fraud detection status summary.
    """
    from creditapp.models import CreditApplication, FraudResult

    total_apps = CreditApplication.objects.count()
    approved = CreditApplication.objects.filter(status="approved").count()
    rejected = CreditApplication.objects.filter(status="rejected").count()
    pending = CreditApplication.objects.filter(status="pending").count()
    flagged = FraudResult.objects.filter(triggered=True).count()

    table = Table(title="Fraud Detection Status")
    table.add_column("Status", style="cyan")
    table.add_column("Count", style="green")

    table.add_row("Total Applications", str(total_apps))
    table.add_row("Approved", str(approved))
    table.add_row("Rejected", str(rejected))
    table.add_row("Pending", str(pending))
    table.add_row("Total Fraud Flags", str(flagged))

    console.print("\n")
    console.print(table)
    console.print()


if __name__ == "__main__":
    app()
