"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg.

    uv run python run.py --data-dir data --warehouse warehouse

Must work on an empty warehouse folder, and must be safe to run again on the same one.
The grader runs it on a dataset you haven't seen, so never hard-code file names, row counts or SKUs.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

DBT_DIR = Path(__file__).resolve().parent / "dbt"

SUPPLIER_COLUMNS = [
    "supplier_id", "sku", "product_name_ar", "unit_cost", "cost_sar",
    "valid_from", "min_order_qty", "currency",
]


def get_catalog(warehouse: Path) -> SqlCatalog:
    """The Iceberg catalog the grader reads. Don't change the name, the URI or the warehouse."""
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("qafila", uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}", warehouse=warehouse.resolve().as_posix())


def ingest(data_dir: Path, db: Path) -> None:
    """Step 1: land the sources in DuckDB, schema `raw`, exactly as they arrive.

    Full refresh on every run (CREATE OR REPLACE), so running twice never duplicates anything.
    """
    with duckdb.connect(str(db)) as con:
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")

        # 1) app database tables
        source_db = str((data_dir / "qafila_oltp.sqlite").resolve()).replace("\\", "/")
        con.execute("INSTALL sqlite")
        con.execute("LOAD sqlite")
        con.execute(f"ATTACH '{source_db}' AS source_db (TYPE SQLITE, READ_ONLY)")
        try:
            for table in ["orders", "order_items", "products", "stores"]:
                con.execute(f"""
                    CREATE OR REPLACE TABLE raw.{table} AS
                    SELECT *, 'qafila_oltp.sqlite' AS source_file
                    FROM source_db.main.{table}
                """)
        finally:
            con.execute("DETACH source_db")

        # 2) supplier price files: everything as text, one row per file row, with its file name
        con.execute("""
            CREATE OR REPLACE TABLE raw.supplier_prices (
                supplier_id VARCHAR, sku VARCHAR, product_name_ar VARCHAR,
                unit_cost VARCHAR, cost_sar VARCHAR, valid_from VARCHAR,
                min_order_qty VARCHAR, currency VARCHAR, source_file VARCHAR
            )
        """)
        for file in sorted((data_dir / "suppliers").glob("prices_*.csv")):
            path = str(file.resolve()).replace("\\", "/").replace("'", "''")
            # take only the columns this file really has (the schema changes between weeks)
            file_cols = [r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_csv('{path}', all_varchar = true)"
            ).fetchall()]
            keep = [c for c in file_cols if c in SUPPLIER_COLUMNS]
            col_list = ", ".join(f'"{c}"' for c in keep)
            con.execute(
                f"""
                INSERT INTO raw.supplier_prices ({col_list}, source_file)
                SELECT {col_list}, ? FROM read_csv('{path}', all_varchar = true)
                """,
                [file.name],
            )


def transform(db: Path) -> None:
    """Steps 2-4: build and test the dbt project in dbt/ against the DuckDB file.

    dbt runs in its own process: in this one it would keep the DuckDB file open and block publish().
    """
    env = {**os.environ, "QAFILA_DUCKDB": str(db.resolve())}
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()",
           "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if subprocess.run(cmd, env=env).returncode != 0:
        raise SystemExit("dbt build failed")


# Iceberg table -> (dbt model name, schema)
PUBLISH = {
    "silver.product_cost_history": ("product_cost_history", pa.schema([
        ("sku", pa.string()), ("unit_cost_sar", pa.float64()),
        ("valid_from", pa.date32()), ("valid_to", pa.date32()),
        ("is_current", pa.bool_()),
    ])),
    "gold.weekly_margin": ("weekly_margin", pa.schema([
        ("iso_week", pa.int32()), ("city", pa.string()), ("category", pa.string()),
        ("lines", pa.int32()), ("revenue_sar", pa.float64()),
        ("costed_revenue_sar", pa.float64()), ("cost_sar", pa.float64()),
        ("margin_sar", pa.float64()), ("uncosted_lines", pa.int32()),
    ])),
    "quality.price_quarantine": ("price_quarantine", pa.schema([
        ("source_file", pa.string()), ("sku", pa.string()), ("valid_from", pa.date32()),
        ("raw_cost", pa.string()), ("reason", pa.string()),
    ])),
}


def publish(db: Path, warehouse: Path) -> None:
    """Step 5: publish three dbt models to Iceberg, updating in place (never drop/recreate).

    If nothing changed, no new snapshot is written (idempotent). If data changed (e.g. the
    correction file), the table is overwritten in place, so the old snapshot keeps the old numbers.
    """
    catalog = get_catalog(warehouse)

    with duckdb.connect(str(db), read_only=True) as con:
        for ident, (model, schema) in PUBLISH.items():
            namespace = ident.split(".")[0]
            try:
                catalog.create_namespace(namespace)
            except Exception:
                pass  # already exists

            # find the dbt model whatever schema dbt put it in
            row = con.execute(
                """
                SELECT table_schema FROM information_schema.tables
                WHERE table_name = ? AND table_schema NOT IN ('raw', 'information_schema')
                ORDER BY table_schema LIMIT 1
                """,
                [model],
            ).fetchone()
            if row is None:
                raise SystemExit(f"dbt model not found in DuckDB: {model}")

            cols = ", ".join(f'"{n}"' for n in schema.names)
            res = con.execute(f'SELECT {cols} FROM "{row[0]}"."{model}"')
            new = (res.fetch_arrow_table() if hasattr(res, "fetch_arrow_table") else res.arrow()).cast(schema)
            keys = [(n, "ascending") for n in schema.names]
            new = new.sort_by(keys)

            if catalog.table_exists(ident):
                table = catalog.load_table(ident)
                old = table.scan().to_arrow().select(schema.names).cast(schema).sort_by(keys)
                if old.equals(new):
                    print(f"unchanged: {ident}")
                    continue
                table.overwrite(new)
                print(f"updated:   {ident}")
            else:
                table = catalog.create_table(ident, schema=schema)
                table.append(new)
                print(f"created:   {ident}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = ap.parse_args()

    args.warehouse.mkdir(parents=True, exist_ok=True)
    db = args.warehouse / "qafila.duckdb"
    ingest(args.data_dir, db)
    transform(db)
    publish(db, args.warehouse)
    print(f"done: {args.warehouse.resolve()}")


if __name__ == "__main__":
    main()