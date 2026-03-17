import importlib.util
import json
import sys
from pathlib import Path
from typing import Optional

import httpx
import typer
from httpx import HTTPStatusError

from thyme.dataset import clear_registry, get_commit_payload

from rich.console import Console
from rich.table import Table

app = typer.Typer(help="Thyme feature platform CLI.")
DEFAULT_API_URL = "http://localhost:8080/api/v1/commit"
DEFAULT_API_BASE = "http://localhost:8080"
DEFAULT_QUERY_URL = "http://localhost:8081"


@app.command()
def version() -> None:
    """Show thyme version."""
    typer.echo("thyme 0.1.0")


def _import_module_by_name(module_name: str) -> None:
    """Import module by dotted path (e.g. myproject.features)."""
    mod = importlib.import_module(module_name)
    importlib.reload(mod)  # Re-run to populate registry (handles cached modules)


def _import_module_by_path(file_path: Path) -> None:
    """Import module from file path. Adds parent dir to sys.path."""
    resolved = file_path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Module file not found: {file_path}")

    parent = str(resolved.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)

    module_name = resolved.stem
    spec = importlib.util.spec_from_file_location(module_name, resolved)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)


@app.command()
def commit(
    module: Optional[str] = typer.Option(None, "-m", "--module", help="Module path (e.g. myproject.features)"),
    path: Optional[Path] = typer.Argument(None, help="Path to feature module file (e.g. features.py)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print payload instead of POSTing"),
    output: Optional[Path] = typer.Option(None, "--output", help="Write payload to file (with --dry-run)"),
    api_url: Optional[str] = typer.Option(None, "--api-url", envvar="THYME_API_URL", help="Control plane API URL"),
) -> None:
    """Import feature module, serialize datasets, and POST to control plane (or dry-run)."""
    if module is None and path is None:
        typer.echo("Error: Provide either -m MODULE or a file path.", err=True)
        raise typer.Exit(1)
    if module is not None and path is not None:
        typer.echo("Error: Provide either -m MODULE or a file path, not both.", err=True)
        raise typer.Exit(1)

    clear_registry()

    try:
        if module is not None:
            _import_module_by_name(module)
        else:
            assert path is not None
            _import_module_by_path(path)
    except Exception as e:
        typer.echo(f"Error importing module: {e}", err=True)
        raise typer.Exit(1)

    payload = get_commit_payload()
    payload_json = json.dumps(payload, indent=2)

    if dry_run:
        if output is not None:
            output.write_text(payload_json)
            n_ds = len(payload["datasets"])
            n_pl = len(payload["pipelines"])
            n_fs = len(payload["featuresets"])
            n_src = len(payload["sources"])
            typer.echo(f"Wrote commit payload ({n_ds} datasets, {n_pl} pipelines, {n_fs} featuresets, {n_src} sources) to {output}")
        else:
            typer.echo(payload_json)
        return

    url = api_url or DEFAULT_API_URL
    try:
        proto_bytes = None
        try:
            from thyme.compiler import compile_commit_request
            proto_msg = compile_commit_request(
                message="",
                datasets=payload["datasets"],
                pipelines=payload["pipelines"],
                featuresets=payload["featuresets"],
                sources=payload["sources"],
            )
            proto_bytes = proto_msg.SerializeToString()
        except Exception:
            pass

        if proto_bytes is not None:
            response = httpx.post(
                url,
                content=proto_bytes,
                headers={"Content-Type": "application/protobuf"},
                timeout=30.0,
            )
        else:
            response = httpx.post(url, json=payload, timeout=30.0)

        response.raise_for_status()
        n_ds = len(payload["datasets"])
        n_pl = len(payload["pipelines"])
        n_fs = len(payload["featuresets"])
        n_src = len(payload["sources"])
        typer.echo(f"Committed {n_ds} dataset(s), {n_pl} pipeline(s), {n_fs} featureset(s), {n_src} source(s) to {url}")
    except HTTPStatusError as e:
        typer.echo(f"Error: {e.response.status_code} {e.response.text}", err=True)
        raise typer.Exit(1)
    except httpx.ConnectError as e:
        typer.echo(f"Error: Could not connect to {url}: {e}", err=True)
        raise typer.Exit(1)


def _check_health(url: str) -> bool:
    """Ping a health endpoint; return True if healthy."""
    try:
        r = httpx.get(url, timeout=3)
        return r.status_code == 200
    except Exception:
        return False


@app.command()
def status(
    json_output: bool = typer.Option(False, "--json", help="Output raw JSON"),
    api_url: Optional[str] = typer.Option(None, "--api-url", envvar="THYME_API_URL"),
    query_url: Optional[str] = typer.Option(None, "--query-url", envvar="THYME_QUERY_URL"),
) -> None:
    """Show system status: committed definitions, jobs, and service health."""
    base = api_url or DEFAULT_API_BASE
    qbase = query_url or DEFAULT_QUERY_URL

    try:
        r = httpx.get(f"{base}/api/v1/status", timeout=10)
        r.raise_for_status()
        data = r.json()
    except httpx.ConnectError as e:
        typer.echo(f"Error: Could not connect to {base}: {e}", err=True)
        raise typer.Exit(1)

    ds_healthy = _check_health(f"{base}/health")
    qs_healthy = _check_health(f"{qbase}/health")

    if json_output:
        combined = {
            "status": data,
            "services": {
                "definition_service": "up" if ds_healthy else "down",
                "query_server": "up" if qs_healthy else "down",
            },
        }
        typer.echo(json.dumps(combined, indent=2))
        return

    console = Console()

    # Services table
    svc_table = Table(title="Services")
    svc_table.add_column("Service")
    svc_table.add_column("Status")
    svc_table.add_row("Definition Service", "[green]UP[/green]" if ds_healthy else "[red]DOWN[/red]")
    svc_table.add_row("Query Server", "[green]UP[/green]" if qs_healthy else "[red]DOWN[/red]")
    console.print(svc_table)

    # Latest commit
    if data.get("latest_commit"):
        c = data["latest_commit"]
        console.print(f"\nLatest commit: {c['id']} ({c['status']}) at {c['created_at']}")

    # Datasets
    if data.get("datasets"):
        t = Table(title="Datasets")
        t.add_column("Name")
        t.add_column("Version")
        for ds in data["datasets"]:
            t.add_row(ds["name"], str(ds["version"]))
        console.print(t)

    # Pipelines
    if data.get("pipelines"):
        t = Table(title="Pipelines")
        t.add_column("Name")
        t.add_column("Version")
        t.add_column("Input")
        t.add_column("Output")
        for p in data["pipelines"]:
            t.add_row(p["name"], str(p["version"]), ", ".join(p.get("input_datasets", [])), p.get("output_dataset", ""))
        console.print(t)

    # Featuresets
    if data.get("featuresets"):
        t = Table(title="Featuresets")
        t.add_column("Name")
        t.add_column("Features")
        for fs in data["featuresets"]:
            t.add_row(fs["name"], str(fs.get("feature_count", 0)))
        console.print(t)

    # Sources
    if data.get("sources"):
        t = Table(title="Sources")
        t.add_column("Dataset")
        t.add_column("Connector")
        for s in data["sources"]:
            t.add_row(s["dataset"], s["connector_type"])
        console.print(t)

    # Jobs
    if data.get("jobs"):
        t = Table(title="Jobs")
        t.add_column("Name")
        t.add_column("Partitions")
        for j in data["jobs"]:
            t.add_row(j["name"], str(j["partition_count"]))
        console.print(t)

    # Backfills
    if data.get("backfills"):
        t = Table(title="Backfills")
        t.add_column("Job")
        t.add_column("Source")
        t.add_column("Status")
        t.add_column("Records")
        for b in data["backfills"]:
            t.add_row(b["job_name"], b["source_dataset"], b["status"], str(b["records_ingested"]))
        console.print(t)

    # Recent events
    if data.get("recent_events"):
        t = Table(title="Recent Activity")
        t.add_column("Time")
        t.add_column("Type")
        t.add_column("Severity")
        t.add_column("Message")
        for ev in data["recent_events"]:
            sev = ev["severity"]
            sev_display = f"[red]{sev}[/red]" if sev == "error" else f"[yellow]{sev}[/yellow]" if sev == "warning" else sev
            t.add_row(ev["created_at"], ev["event_type"], sev_display, ev["message"])
        console.print(t)


@app.command()
def logs(
    limit: int = typer.Option(50, "--limit", "-n", help="Number of events"),
    severity: Optional[str] = typer.Option(None, "--severity", "-s", help="Filter: info, warning, error"),
    event_type: Optional[str] = typer.Option(None, "--type", "-t", help="Filter by event type"),
    json_output: bool = typer.Option(False, "--json", help="Output raw JSON"),
    api_url: Optional[str] = typer.Option(None, "--api-url", envvar="THYME_API_URL"),
) -> None:
    """Show recent service events (commits, errors, backfills, etc.)."""
    base = api_url or DEFAULT_API_BASE

    params: dict = {"limit": limit}
    if severity:
        params["severity"] = severity
    if event_type:
        params["event_type"] = event_type

    try:
        r = httpx.get(f"{base}/api/v1/events", params=params, timeout=10)
        r.raise_for_status()
        events = r.json()
    except httpx.ConnectError as e:
        typer.echo(f"Error: Could not connect to {base}: {e}", err=True)
        raise typer.Exit(1)

    if json_output:
        typer.echo(json.dumps(events, indent=2))
        return

    if not events:
        typer.echo("No events found.")
        return

    console = Console()
    t = Table(title="Service Events")
    t.add_column("Time")
    t.add_column("Type")
    t.add_column("Severity")
    t.add_column("Source")
    t.add_column("Subject")
    t.add_column("Message")
    for ev in events:
        sev = ev["severity"]
        sev_display = f"[red]{sev}[/red]" if sev == "error" else f"[yellow]{sev}[/yellow]" if sev == "warning" else sev
        t.add_row(
            ev["created_at"], ev["event_type"], sev_display,
            ev.get("source", ""), ev.get("subject", ""), ev["message"],
        )
    console.print(t)


@app.command()
def discover(
    source_type: str = typer.Option("iceberg", "--source-type", help="Connector type: iceberg, postgres, s3json, jsonl"),
    use_case: str = typer.Option(..., "--use-case", "-u", help="Feature engineering use case (e.g. 'fraud detection')"),
    # Iceberg options
    catalog: Optional[str] = typer.Option(None, "--catalog", help="Iceberg catalog URI"),
    database: Optional[str] = typer.Option(None, "--database", help="Iceberg database name"),
    table: Optional[str] = typer.Option(None, "--table", help="Iceberg table name"),
    # Postgres options
    pg_host: Optional[str] = typer.Option(None, "--pg-host", help="Postgres host"),
    pg_port: int = typer.Option(5432, "--pg-port", help="Postgres port"),
    pg_database: Optional[str] = typer.Option(None, "--pg-database", help="Postgres database"),
    pg_table: Optional[str] = typer.Option(None, "--pg-table", help="Postgres table"),
    pg_user: Optional[str] = typer.Option(None, "--pg-user", help="Postgres user"),
    pg_password: Optional[str] = typer.Option(None, "--pg-password", envvar="POSTGRES_PASSWORD", help="Postgres password"),
    pg_schema: str = typer.Option("public", "--pg-schema", help="Postgres schema"),
    # S3 options
    s3_bucket: Optional[str] = typer.Option(None, "--s3-bucket", help="S3 bucket name"),
    s3_prefix: str = typer.Option("", "--s3-prefix", help="S3 key prefix"),
    s3_region: str = typer.Option("us-east-1", "--s3-region", help="AWS region"),
    # JSONL options
    path: Optional[str] = typer.Option(None, "--path", help="Local JSONL file path"),
    # Generation options
    entity_key: Optional[str] = typer.Option(None, "--entity-key", help="Entity key field name hint"),
    windows: str = typer.Option("1h,24h,7d", "--windows", help="Comma-separated aggregation windows"),
    dataset_name: Optional[str] = typer.Option(None, "--dataset-name", help="Name for the generated dataset class"),
    sample_n: int = typer.Option(5, "--sample-n", help="Number of sample rows to fetch for schema inference"),
    # Output options
    output: Optional[Path] = typer.Option(None, "--output", help="Write generated code to this file"),
    auto_commit: bool = typer.Option(False, "--auto-commit", help="Automatically commit generated features"),
    api_url: Optional[str] = typer.Option(None, "--api-url", envvar="THYME_API_URL", help="Control plane API URL"),
    api_key: Optional[str] = typer.Option(None, "--api-key", envvar="ANTHROPIC_API_KEY", help="Anthropic API key"),
) -> None:
    """Discover features from a data source using an AI agent.

    Introspects the source schema, then generates @dataset / @pipeline /
    @featureset Thyme SDK code tailored to your use case.
    """
    try:
        from thyme.agent.introspect import (
            introspect_iceberg,
            introspect_jsonl,
            introspect_postgres,
            introspect_s3json,
        )
        from thyme.agent.codegen import generate_thyme_code, validate_code
    except ImportError as exc:
        typer.echo(f"Error: agent dependencies not available: {exc}", err=True)
        raise typer.Exit(1)

    # --- Build connector dict and introspect schema ---
    st = source_type.lower()
    try:
        if st == "iceberg":
            if not (catalog and database and table):
                typer.echo("Error: --catalog, --database, and --table are required for iceberg source.", err=True)
                raise typer.Exit(1)
            schema = introspect_iceberg(catalog, database, table, sample_n=sample_n)
            connector_dict = {
                "connector_type": "iceberg",
                "config": {"catalog": catalog, "database": database, "table": table},
            }

        elif st == "postgres":
            missing = [n for n, v in [("--pg-host", pg_host), ("--pg-database", pg_database),
                                       ("--pg-table", pg_table), ("--pg-user", pg_user),
                                       ("--pg-password", pg_password)] if not v]
            if missing:
                typer.echo(f"Error: {', '.join(missing)} required for postgres source.", err=True)
                raise typer.Exit(1)
            schema = introspect_postgres(
                pg_host, pg_port, pg_database, pg_table,  # type: ignore[arg-type]
                pg_user, pg_password, pg_schema, sample_n=sample_n,  # type: ignore[arg-type]
            )
            connector_dict = {
                "connector_type": "postgres",
                "config": {"host": pg_host, "port": pg_port, "database": pg_database,
                           "table": pg_table, "user": pg_user, "password": pg_password,
                           "schema": pg_schema},
            }

        elif st == "s3json":
            if not s3_bucket:
                typer.echo("Error: --s3-bucket is required for s3json source.", err=True)
                raise typer.Exit(1)
            schema = introspect_s3json(s3_bucket, s3_prefix, s3_region, sample_n=sample_n)
            connector_dict = {
                "connector_type": "s3json",
                "config": {"bucket": s3_bucket, "prefix": s3_prefix, "region": s3_region},
            }

        elif st == "jsonl":
            if not path:
                typer.echo("Error: --path is required for jsonl source.", err=True)
                raise typer.Exit(1)
            schema = introspect_jsonl(path, sample_n=sample_n)
            connector_dict = {"connector_type": "jsonl", "config": {"path": path}}

        else:
            typer.echo(f"Error: unknown source type '{source_type}'. Choose: iceberg, postgres, s3json, jsonl.", err=True)
            raise typer.Exit(1)

    except typer.Exit:
        raise
    except Exception as exc:
        typer.echo(f"Error during schema introspection: {exc}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Introspected {len(schema.fields)} fields from {source_type} source.", err=True)

    # --- Generate code via LLM ---
    window_list = [w.strip() for w in windows.split(",") if w.strip()]
    try:
        code = generate_thyme_code(
            schema=schema,
            connector=connector_dict,
            use_case=use_case,
            entity_key=entity_key,
            windows=window_list,
            dataset_name=dataset_name,
            api_key=api_key,
        )
    except Exception as exc:
        typer.echo(f"Error during code generation: {exc}", err=True)
        raise typer.Exit(1)

    # --- Validate generated code ---
    is_valid, error_msg = validate_code(code)
    if not is_valid:
        typer.echo(f"Warning: generated code failed validation: {error_msg}", err=True)

    # --- Output ---
    if output is not None:
        output.write_text(code)
        typer.echo(f"Generated features written to {output}")
    else:
        typer.echo(code)

    if not is_valid:
        typer.echo("Warning: the generated code has validation issues — review before committing.", err=True)
        if not auto_commit:
            raise typer.Exit(1)

    # --- Optional auto-commit ---
    if auto_commit:
        if output is None:
            typer.echo("Error: --auto-commit requires --output to specify the file to commit.", err=True)
            raise typer.Exit(1)
        clear_registry()
        try:
            _import_module_by_path(output)
        except Exception as exc:
            typer.echo(f"Error importing generated module for commit: {exc}", err=True)
            raise typer.Exit(1)
        payload = get_commit_payload()
        url = api_url or DEFAULT_API_URL
        try:
            response = httpx.post(url, json=payload, timeout=30.0)
            response.raise_for_status()
            typer.echo(f"Auto-committed generated features to {url}")
        except (HTTPStatusError, httpx.ConnectError) as exc:
            typer.echo(f"Error committing: {exc}", err=True)
            raise typer.Exit(1)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
