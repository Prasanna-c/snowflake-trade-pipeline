"""Command-line interface.

    trade-sim generate     -- write trade files locally, no Snowflake needed
    trade-sim load         -- generate, PUT to the stage, COPY, drain the stream
    trade-sim stream       -- continuous producer, one file every N seconds
    trade-sim drain        -- force a stream drain
    trade-sim reconcile    -- compare manifests against the warehouse's verdicts
    trade-sim status       -- pipeline health from MONITORING.VW_PIPELINE_SLA
    trade-sim emit-seeds   -- regenerate dbt seed CSVs from reference.py
    trade-sim reset-book   -- forget the trade book and start a fresh universe

`generate` and `emit-seeds` never touch the network, so the whole modelling layer can
be developed and reviewed offline.
"""

from __future__ import annotations

import csv
import logging
import secrets
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from trade_sim import reference as ref
from trade_sim.config import REPO_ROOT, simulator_settings, snowflake_settings
from trade_sim.generator import TradeBook, TradeGenerator
from trade_sim.loaders.snowflake_loader import SnowflakeLoader, SnowflakeSession
from trade_sim.reconcile import Reconciler, find_manifests, load_manifest
from trade_sim.writer import BatchWriter, summarise_events

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Trade event simulator and Snowflake loader.",
)
console = Console()


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True, show_path=verbose)],
    )
    # The Snowflake driver is extremely chatty at INFO.
    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)


def _new_batch_ref() -> str:
    """Short, collision-resistant, sortable-enough batch reference."""
    return f"b{datetime.now(UTC).strftime('%H%M%S')}{secrets.token_hex(2)}"


def _print_summary(summary: dict[str, object]) -> None:
    table = Table(title="Generated batch", show_header=True, header_style="bold cyan")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Total events", str(summary["total"]))
    table.add_row("Expected ACCEPTED", str(summary["expected_accepted"]))
    table.add_row("Expected REJECTED", str(summary["expected_rejected"]))
    console.print(table)

    faults = summary["by_fault"]
    assert isinstance(faults, dict)
    fault_table = Table(title="Injected faults", show_header=True, header_style="bold magenta")
    fault_table.add_column("Fault")
    fault_table.add_column("Count", justify="right")
    for name, count in faults.items():
        fault_table.add_row(name, str(count))
    console.print(fault_table)


# ---------------------------------------------------------------------------
@app.command()
def generate(
    trades: Annotated[int, typer.Option("--trades", "-n", help="Events per batch.")] = 5_000,
    batches: Annotated[int, typer.Option("--batches", "-b", help="Number of files to write.")] = 1,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="Trade date to generate for (YYYY-MM-DD). Defaults to today."),
    ] = None,
    seed: Annotated[
        int | None, typer.Option("--seed", help="Override the configured RNG seed.")
    ] = None,
    no_compress: Annotated[
        bool, typer.Option("--no-compress", help="Write plain NDJSON for easy inspection.")
    ] = False,
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
) -> None:
    """Write trade files to disk. No Snowflake connection required."""
    _configure_logging(verbose)
    settings = simulator_settings()
    if seed is not None:
        settings = settings.model_copy(update={"seed": seed})
    if no_compress:
        settings = settings.model_copy(update={"compress": False})

    trade_date = date.fromisoformat(as_of) if as_of else date.today()
    writer = BatchWriter(settings.output_dir, env="dev", compress=settings.compress)

    written: list[Path] = []
    with TradeBook.exclusive(settings.state_dir / "trade_book.json") as book:
        generator = TradeGenerator(settings, book=book)
        for index in range(batches):
            batch_ref = _new_batch_ref()
            events = list(generator.generate(trades, as_of=trade_date))
            path, materialised = writer.write(events, batch_ref=batch_ref)
            manifest = generator.build_manifest(
                materialised, batch_ref=batch_ref, file_name=path.name
            )
            manifest_path = writer.write_manifest(manifest, path)
            written.append(path)
            console.print(
                f"[green]batch {index + 1}/{batches}[/] {path.name}  "
                f"[dim](manifest: {manifest_path.name})[/]"
            )
            if index == 0:
                _print_summary(summarise_events(materialised))

    console.print(
        f"\n[bold green]Wrote {len(written)} file(s)[/] to {settings.output_dir}\n"
        f"Trade book now holds {len(book.trades)} trades."
    )


# ---------------------------------------------------------------------------
@app.command()
def load(
    trades: Annotated[int, typer.Option("--trades", "-n")] = 5_000,
    batches: Annotated[int, typer.Option("--batches", "-b")] = 1,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    run_id: Annotated[
        str, typer.Option("--run-id", help="Orchestrator run id for lineage.")
    ] = "manual",
    no_drain: Annotated[
        bool, typer.Option("--no-drain", help="Leave the stream for the scheduled task.")
    ] = False,
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
) -> None:
    """Generate files, upload them to the stage, COPY into RAW, and drain the stream."""
    _configure_logging(verbose)
    settings = simulator_settings()
    trade_date = date.fromisoformat(as_of) if as_of else date.today()

    writer = BatchWriter(
        settings.output_dir, env=snowflake_settings().database.split("_")[-1].lower()
    )

    sf = snowflake_settings()
    # The lock is taken around the whole load, not just generation: the identifiers are
    # only safe if nothing else mints from this book until these files are in the
    # warehouse and the book reflects them.
    with (
        TradeBook.exclusive(settings.state_dir / "trade_book.json") as book,
        SnowflakeSession(sf, warehouse=sf.effective_load_warehouse) as session,
    ):
        generator = TradeGenerator(settings, book=book)
        loader = SnowflakeLoader(session)
        total_rows = 0

        for index in range(batches):
            batch_ref = _new_batch_ref()
            events = list(generator.generate(trades, as_of=trade_date))
            path, materialised = writer.write(events, batch_ref=batch_ref)
            manifest = generator.build_manifest(
                materialised, batch_ref=batch_ref, file_name=path.name
            )
            writer.write_manifest(manifest, path)

            result = loader.load_file(
                path,
                ingest_date_partition=path.parent.name,
                orchestrator_run_id=run_id,
                drain=not no_drain,
            )
            total_rows += int(result.get("rows_loaded") or 0)
            console.print(
                f"[green]batch {index + 1}/{batches}[/] {path.name} -> "
                f"{result.get('rows_loaded')} rows loaded, "
                f"{result.get('rows_errored')} errored "
                f"[dim](snowflake batch {result.get('batch_id')})[/]"
            )
            if index == 0:
                _print_summary(summarise_events(materialised))

    console.print(
        f"\n[bold green]Loaded {total_rows} rows across {batches} batch(es).[/]\n"
        "Next: [cyan]make dbt-build-incremental[/] then [cyan]trade-sim reconcile[/]"
    )


# ---------------------------------------------------------------------------
@app.command()
def stream(
    trades_per_file: Annotated[int, typer.Option("--trades-per-file")] = 500,
    interval_seconds: Annotated[int, typer.Option("--interval-seconds")] = 30,
    max_files: Annotated[
        int | None,
        typer.Option("--max-files", help="Stop after N files. Default: run until interrupted."),
    ] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
) -> None:
    """Continuous producer: emit and load one file every N seconds.

    This is how the near-real-time behaviour of the Stream and Task is demonstrated --
    leave it running and watch RAW.TRADE_EVENT_QUEUE fill and drain on its own.
    """
    _configure_logging(verbose)
    settings = simulator_settings()
    generator = TradeGenerator(settings)
    writer = BatchWriter(settings.output_dir, env="dev")

    sf = snowflake_settings()
    emitted = 0
    console.print(
        f"[bold]Streaming[/] {trades_per_file} trades every {interval_seconds}s. Ctrl-C to stop."
    )
    book_path = settings.state_dir / "trade_book.json"
    try:
        with SnowflakeSession(sf, warehouse=sf.effective_load_warehouse) as session:
            loader = SnowflakeLoader(session)
            while max_files is None or emitted < max_files:
                batch_ref = _new_batch_ref()
                # Locked per file rather than for the session: a producer that runs until
                # interrupted would otherwise hold the book indefinitely and any other
                # simulator would wait on it forever. The book is re-read inside the lock
                # so identifiers allocated elsewhere between files are honoured, while the
                # generator itself persists so its random stream keeps advancing.
                with TradeBook.exclusive(book_path) as book:
                    generator.book = book
                    events = list(generator.generate(trades_per_file))
                path, materialised = writer.write(events, batch_ref=batch_ref)
                writer.write_manifest(
                    generator.build_manifest(
                        materialised, batch_ref=batch_ref, file_name=path.name
                    ),
                    path,
                )
                # drain=False on purpose: let the Snowflake task pick it up, which is
                # what we are trying to demonstrate.
                result = loader.load_file(
                    path,
                    ingest_date_partition=path.parent.name,
                    orchestrator_run_id="stream",
                    drain=False,
                )
                emitted += 1
                console.print(
                    f"[dim]{datetime.now(UTC):%H:%M:%S}[/] file {emitted}: "
                    f"{result.get('rows_loaded')} rows -> awaiting TASK_DRAIN_TRADE_EVENT_STREAM"
                )
                time.sleep(interval_seconds)
    except KeyboardInterrupt:
        console.print(f"\n[yellow]Stopped after {emitted} file(s).[/]")


# ---------------------------------------------------------------------------
@app.command()
def drain(
    run_id: Annotated[str, typer.Option("--run-id")] = "manual",
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
) -> None:
    """Force a stream drain rather than waiting for the scheduled task."""
    _configure_logging(verbose)
    sf = snowflake_settings()
    with SnowflakeSession(sf, warehouse=sf.effective_load_warehouse) as session:
        result = SnowflakeLoader(session).drain_stream(run_id)
    if result.get("skipped"):
        console.print("[yellow]Stream was empty -- nothing to drain.[/]")
    else:
        console.print(
            f"[green]Drained {result.get('rows_drained')} rows[/] as batch {result.get('batch_id')}"
        )


# ---------------------------------------------------------------------------
@app.command()
def reconcile(
    batch_ref: Annotated[
        str | None, typer.Option("--batch-ref", help="Reconcile one batch by reference.")
    ] = None,
    all_batches: Annotated[
        bool,
        typer.Option("--all", help="Reconcile every manifest, including superseded batches."),
    ] = False,
    fail_on_mismatch: Annotated[
        bool, typer.Option("--fail-on-mismatch/--no-fail", help="Exit non-zero when a check fails.")
    ] = True,
    show_limit: Annotated[
        int, typer.Option("--show", help="Max discrepancies to print per batch.")
    ] = 15,
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
) -> None:
    """Compare generated manifests against the verdicts the pipeline reached.

    The newest batch by default. A manifest's expectations describe one moment in each
    trade's life, and a later batch moves that on: a version accepted then is SUPERSEDED
    once a higher version of the same trade arrives. `--batch-ref` names an older batch,
    `--all` grades every one.
    """
    _configure_logging(verbose)
    manifest_dir = simulator_settings().output_dir.parent / "manifests"
    manifests = find_manifests(manifest_dir, batch_ref)
    if not manifests:
        console.print(
            f"[yellow]No manifests found in {manifest_dir}. Run `trade-sim load` first.[/]"
        )
        raise typer.Exit(code=1)

    if batch_ref is None and not all_batches:
        manifests = manifests[-1:]
        console.print(f"[dim]Newest batch only: {manifests[0].name}. Use --all for every one.[/]")

    all_passed = True
    with SnowflakeSession(snowflake_settings()) as session:
        reconciler = Reconciler(session)
        for path in manifests:
            manifest = load_manifest(path)
            result = reconciler.reconcile(manifest)
            all_passed = all_passed and result.passed

            colour = "green" if result.passed else "red"
            console.print(f"[{colour}]{result.summary()}[/]")

            for discrepancy in result.discrepancies[:show_limit]:
                console.print(
                    f"  [dim]{discrepancy.kind}[/] {discrepancy.trade_id}"
                    f" v{discrepancy.trade_version}:"
                    f" expected [cyan]{discrepancy.expected}[/],"
                    f" got [magenta]{discrepancy.actual}[/]"
                    + (f"  {discrepancy.detail}" if discrepancy.detail else "")
                )
            if len(result.discrepancies) > show_limit:
                console.print(f"  [dim]... and {len(result.discrepancies) - show_limit} more[/]")

    if not all_passed and fail_on_mismatch:
        console.print("\n[bold red]Reconciliation FAILED.[/] See docs/runbook.md.")
        raise typer.Exit(code=1)
    console.print("\n[bold green]Reconciliation passed.[/]")


# ---------------------------------------------------------------------------
@app.command()
def status(verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False) -> None:
    """Print pipeline health from MONITORING.VW_PIPELINE_SLA."""
    _configure_logging(verbose)
    sf = snowflake_settings()
    with SnowflakeSession(sf) as session:
        rows = session.execute(f"select * from {sf.database}.monitoring.vw_pipeline_sla")
    if not rows:
        console.print("[yellow]No SLA row returned. Has the monitoring layer been deployed?[/]")
        raise typer.Exit(code=1)

    row = rows[0]
    table = Table(title="Pipeline SLA", show_header=True, header_style="bold cyan")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    for key, value in row.items():
        display = str(value)
        if display in {"RED", "AMBER", "GREEN"}:
            colour = {"RED": "red", "AMBER": "yellow", "GREEN": "green"}[display]
            display = f"[{colour}]{display}[/]"
        table.add_row(key.lower(), display)
    console.print(table)


# ---------------------------------------------------------------------------
@app.command("emit-seeds")
def emit_seeds(
    check: Annotated[
        bool, typer.Option("--check", help="Verify the checked-in seeds match, without writing.")
    ] = False,
    seeds_dir: Annotated[Path | None, typer.Option("--seeds-dir")] = None,
) -> None:
    """Regenerate dbt seed CSVs from reference.py.

    CI runs this with `--check` so reference data and seeds cannot drift apart.
    """
    target = seeds_dir or (REPO_ROOT / "dbt" / "seeds")
    target.mkdir(parents=True, exist_ok=True)
    drifted: list[str] = []

    for spec in ref.build_seed_specs():
        path = target / spec.filename
        buffer = Path(path).with_suffix(".tmp") if check else path

        with buffer.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
            writer.writerow(spec.header)
            writer.writerows(spec.rows)

        if check:
            new_text = buffer.read_text(encoding="utf-8")
            buffer.unlink()
            old_text = path.read_text(encoding="utf-8") if path.is_file() else ""
            if new_text != old_text:
                drifted.append(spec.filename)
        else:
            console.print(f"[green]wrote[/] {path.relative_to(REPO_ROOT)} ({len(spec.rows)} rows)")

    if check:
        if drifted:
            console.print(
                "[bold red]Seed drift detected in:[/] "
                + ", ".join(drifted)
                + "\nRun `trade-sim emit-seeds` and commit the result."
            )
            raise typer.Exit(code=1)
        console.print("[green]Seeds are in sync with reference.py.[/]")


# ---------------------------------------------------------------------------
@app.command("reset-book")
def reset_book(
    yes: Annotated[bool, typer.Option("--yes", help="Skip confirmation.")] = False,
) -> None:
    """Delete the local trade book so the next run starts a fresh trade universe.

    Only affects local generator state. Nothing in Snowflake is touched, so the
    warehouse keeps the old trades -- which means new trades will start again from
    TRD-000000001 and collide with them. Truncate RAW.TRADE_EVENT too if you want a
    genuinely clean slate.
    """
    path = simulator_settings().state_dir / "trade_book.json"
    if not path.is_file():
        console.print("[yellow]No trade book to reset.[/]")
        return
    if not yes and not typer.confirm(f"Delete {path}?"):
        raise typer.Abort
    path.unlink()
    console.print(f"[green]Deleted[/] {path}")


def main() -> None:
    try:
        app()
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
