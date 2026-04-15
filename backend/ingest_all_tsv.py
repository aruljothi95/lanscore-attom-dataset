import argparse
import os
import uuid
import logging
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from schema_builder import infer_schema, generate_create_table_sql, clean_col


log = logging.getLogger("ingest")


def get_required_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def table_for_file(path: Path) -> str:
    name = path.name.lower()
    if "foreclosure" in name:
        return "foreclosure"
    if "loanoriginator" in name:
        return "loan_originator"
    if "assignmentrelease" in name:
        return "assignment_release"
    if "taxassessor" in name:
        return "tax_assessor"
    if "propertydeletes" in name:
        return "property_deletes"
    if "propertytoboundarymatch" in name and "parcel" in name:
        return "property_to_boundarymatch_parcel"
    if "recorderdeletes" in name:
        return "recorder_deletes"
    if "recorder" in name:
        return "recorder"
    return Path(name).stem


def ensure_schema(engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))


def drop_table(engine, schema: str, table: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{table};"))


def table_row_count(engine, schema: str, table: str) -> int | None:
    try:
        with engine.connect() as conn:
            return int(conn.execute(text(f"select count(*) from {schema}.{table}")).scalar())
    except Exception:
        return None


def create_table(engine, schema: str, table: str, tsv_path: str) -> None:
    cols = infer_schema(tsv_path, sep="\t")
    ddl = generate_create_table_sql(cols, f"{schema}.{table}")
    with engine.begin() as conn:
        conn.execute(text(ddl))


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [clean_col(c) for c in df.columns]
    df.insert(0, "id", [str(uuid.uuid4()) for _ in range(len(df))])
    df["ingested_at"] = datetime.utcnow()
    return df.where(pd.notnull(df), None)


def ingest_tsv(engine, schema: str, table: str, tsv_path: str, chunksize: int) -> None:
    reader = pd.read_csv(
        tsv_path,
        sep="\t",
        chunksize=chunksize,
        dtype=str,
        low_memory=False,
        engine="c",
        on_bad_lines="warn",
    )

    for i, chunk in enumerate(reader):
        log.info("Chunk %s: %s", i, Path(tsv_path).name)
        df = transform(chunk)

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cols = ",".join(df.columns)
        sql = f"COPY {schema}.{table} ({cols}) FROM STDIN WITH CSV"

        with engine.raw_connection() as conn:
            cur = conn.cursor()
            cur.copy_expert(sql, buffer)
            conn.commit()


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Create tables and ingest all backend/*.tsv into Postgres.")
    parser.add_argument("--dir", default=str(Path(__file__).parent), help="Directory to scan for .tsv files")
    parser.add_argument("--schema", default=os.getenv("POSTGRES_SCHEMA", "attom_dataset"))
    parser.add_argument("--chunksize", type=int, default=50_000)
    parser.add_argument("--only", nargs="*", default=None, help="Optional list of table names to ingest")
    parser.add_argument(
        "--mode",
        choices=["append", "skip_if_nonempty", "replace"],
        default="skip_if_nonempty",
        help="append: always append; skip_if_nonempty: skip tables that already have rows; replace: drop & recreate table then load",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    db_host = get_required_env("POSTGRES_HOST")
    db_port = get_required_env("POSTGRES_PORT")
    db_name = get_required_env("POSTGRES_DATABASE")
    db_user = get_required_env("POSTGRES_USER")
    db_password = get_required_env("POSTGRES_PASSWORD")

    engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

    ensure_schema(engine, args.schema)

    tsvs = sorted(Path(args.dir).glob("*.tsv"))
    if not tsvs:
        raise RuntimeError(f"No .tsv files found in {args.dir}")

    for tsv in tsvs:
        table = table_for_file(tsv)
        if args.only and table not in args.only:
            continue

        if args.mode == "skip_if_nonempty":
            existing = table_row_count(engine, args.schema, table)
            if existing is not None and existing > 0:
                log.info("Skipping %s.%s (already has %s rows)", args.schema, table, existing)
                continue

        log.info("Preparing table %s.%s from %s", args.schema, table, tsv.name)
        if args.mode == "replace":
            drop_table(engine, args.schema, table)
        create_table(engine, args.schema, table, str(tsv))
        ingest_tsv(engine, args.schema, table, str(tsv), args.chunksize)

    log.info("All ingestions complete.")


if __name__ == "__main__":
    main()

