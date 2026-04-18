"""
Load ATTOM parcel GeoJSON (FeatureCollection, one Feature per line) into Postgres
under attom_dataset (or POSTGRES_SCHEMA).

Expects files shaped like backend/attom_travis.geojson and backend/attom_willamson.geojson:
  line 1: {"type":"FeatureCollection","features":[
  following lines: {"type":"Feature",...} or ,{"type":"Feature",...}
  last line: ]}

Requires: psycopg2 (via sqlalchemy), python-dotenv.
Optional: PostGIS on the server — if available, geometry is stored as geometry(MultiPolygon,4326);
          otherwise geometry is kept in geom_json (jsonb).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine, text

log = logging.getLogger("ingest_geojson")


def required_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def ensure_schema(engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))


def postgis_available(engine) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT PostGIS_Version();"))
        return True
    except Exception:
        return False


def drop_table(engine, schema: str, table: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE;'))


def create_engine_from_env():
    db_host = required_env("POSTGRES_HOST")
    db_port = required_env("POSTGRES_PORT")
    db_name = required_env("POSTGRES_DATABASE")
    db_user = required_env("POSTGRES_USER")
    db_password = required_env("POSTGRES_PASSWORD")
    user_q = quote_plus(db_user)
    pwd_q = quote_plus(db_password)
    return create_engine(f"postgresql://{user_q}:{pwd_q}@{db_host}:{db_port}/{db_name}")


def table_for_geojson_path(path: Path) -> str:
    stem = path.stem.lower()
    if "travis" in stem:
        return "parcel_polygons_travis"
    if "willamson" in stem or "williamson" in stem:
        return "parcel_polygons_williamson"
    return f"parcel_polygons_{stem}"


def iter_features(path: Path):
    """Yield GeoJSON Feature dicts from the newline-oriented FeatureCollection file."""
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        header = f.readline()
        if not header.strip().startswith('{"type":"FeatureCollection"'):
            raise ValueError(f"{path}: expected FeatureCollection header, got {header[:80]!r}")
        for raw in f:
            line = raw.strip()
            if not line or line == "]}":
                break
            if line.startswith(","):
                line = line[1:]
            yield json.loads(line)


def create_table_and_indexes(conn, schema: str, table: str, use_postgis: bool) -> None:
    geom_col = 'geom geometry(MultiPolygon,4326)' if use_postgis else "geom_json jsonb NOT NULL"
    conn.execute(
        text(
            f"""
            CREATE TABLE "{schema}"."{table}" (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                feature_id text NOT NULL,
                fipsstate text,
                fipscounty text,
                county text,
                apn text,
                apn2 text,
                addrline1 text,
                city text,
                state text,
                zip5 text,
                src_id text,
                latitude double precision,
                longitude double precision,
                {geom_col},
                ingested_at timestamptz NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
            )
            """
        )
    )
    conn.execute(
        text(f'CREATE UNIQUE INDEX "{table}_feature_id_uq" ON "{schema}"."{table}" (feature_id)')
    )
    if use_postgis:
        conn.execute(text(f'CREATE INDEX "{table}_geom_gix" ON "{schema}"."{table}" USING GIST (geom)'))
    else:
        conn.execute(
            text(f'CREATE INDEX "{table}_geom_json_gix" ON "{schema}"."{table}" USING GIN (geom_json)')
        )


def ingest_file(
    engine,
    schema: str,
    path: Path,
    *,
    replace: bool,
    chunksize: int,
    dry_run: bool,
) -> tuple[str, int]:
    table = table_for_geojson_path(path)
    if dry_run:
        n = 0
        for _ in iter_features(path):
            n += 1
            if n % 100_000 == 0:
                log.info("%s: counted %s features (dry run)...", path.name, n)
        log.info("%s -> %s.%s : %s features (dry run)", path.name, schema, table, n)
        return table, n

    use_postgis = postgis_available(engine)
    if not use_postgis:
        log.warning("PostGIS not available; storing geometry as jsonb (geom_json).")

    if replace:
        drop_table(engine, schema, table)

    with engine.begin() as conn:
        exists = conn.execute(
            text(
                """
                select 1 from information_schema.tables
                where table_schema = :s and table_name = :t
                """
            ),
            {"s": schema, "t": table},
        ).scalar()
        if exists:
            log.info("Table %s.%s already exists; use --replace to reload", schema, table)
            return table, 0

        create_table_and_indexes(conn, schema, table, use_postgis)

    cols = (
        "id, feature_id, fipsstate, fipscounty, county, apn, apn2, addrline1, "
        "city, state, zip5, src_id, latitude, longitude"
    )
    if use_postgis:
        insert_sql = (
            f'INSERT INTO "{schema}"."{table}" ({cols}, geom, ingested_at) VALUES ('
            "%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)), %s::timestamptz)"
        )
    else:
        insert_sql = (
            f'INSERT INTO "{schema}"."{table}" ({cols}, geom_json, ingested_at) VALUES ('
            "%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s::jsonb, %s::timestamptz)"
        )
    ingested_at = datetime.now(timezone.utc)

    batch: list[dict] = []
    total = 0

    def row_args(r: dict) -> tuple:
        t = (
            r["id"],
            r["feature_id"],
            r["fipsstate"],
            r["fipscounty"],
            r["county"],
            r["apn"],
            r["apn2"],
            r["addrline1"],
            r["city"],
            r["state"],
            r["zip5"],
            r["src_id"],
            r["latitude"],
            r["longitude"],
        )
        return t + (r["geom_json"], r["ingested_at"])

    def flush():
        nonlocal batch, total
        if not batch:
            return
        argslist = [row_args(r) for r in batch]
        raw = engine.raw_connection()
        try:
            cur = raw.cursor()
            execute_batch(cur, insert_sql, argslist, page_size=min(250, max(1, len(argslist))))
            raw.commit()
        finally:
            raw.close()
        total += len(batch)
        batch = []
        log.info("%s.%s: inserted %s rows...", schema, table, total)

    skipped_id = 0
    for feat in iter_features(path):
        if feat.get("type") != "Feature":
            raise ValueError(f"{path}: expected Feature, got {feat.get('type')!r}")
        props = feat.get("properties") or {}
        geom = feat.get("geometry")
        if not geom:
            continue
        fid = str(props.get("id") or "").strip()
        if not fid:
            skipped_id += 1
            continue
        row = {
            "id": str(uuid.uuid4()),
            "feature_id": fid,
            "fipsstate": props.get("fipsstate"),
            "fipscounty": props.get("fipscounty"),
            "county": props.get("county"),
            "apn": props.get("apn"),
            "apn2": props.get("apn2"),
            "addrline1": props.get("addrline1"),
            "city": props.get("city"),
            "state": props.get("state"),
            "zip5": props.get("zip5"),
            "src_id": props.get("src_id"),
            "latitude": props.get("latitude"),
            "longitude": props.get("longitude"),
            "geom_json": json.dumps(geom, separators=(",", ":")),
            "ingested_at": ingested_at,
        }
        batch.append(row)
        if len(batch) >= chunksize:
            flush()
    flush()

    if skipped_id:
        log.warning("%s: skipped %s features with empty properties.id", path.name, skipped_id)
    log.info("Done %s.%s : %s rows (postgis=%s)", schema, table, total, use_postgis)
    return table, total


def main() -> None:
    # Prefer .env next to this script (backend/.env) so cwd does not matter.
    load_dotenv(Path(__file__).resolve().parent / ".env")
    load_dotenv()
    parser = argparse.ArgumentParser(description="Ingest ATTOM parcel GeoJSON into Postgres.")
    backend_dir = Path(__file__).resolve().parent
    default_travis = backend_dir / "attom_travis.geojson"
    default_will = backend_dir / "attom_willamson.geojson"
    parser.add_argument(
        "paths",
        nargs="*",
        default=[str(default_travis), str(default_will)],
        help="GeoJSON files (default: attom_travis.geojson and attom_willamson.geojson in backend/)",
    )
    parser.add_argument("--schema", default=os.getenv("POSTGRES_SCHEMA", "attom_dataset"))
    parser.add_argument("--replace", action="store_true", help="Drop target tables before load")
    parser.add_argument("--chunksize", type=int, default=500, help="Rows per transaction batch")
    parser.add_argument("--dry-run", action="store_true", help="Only count features; no DB writes")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    paths = [Path(p) for p in args.paths]
    for p in paths:
        if not p.is_file():
            raise SystemExit(f"Missing file: {p}")

    if args.dry_run:
        for p in paths:
            ingest_file(None, args.schema, p, replace=False, chunksize=args.chunksize, dry_run=True)
        return

    engine = create_engine_from_env()
    ensure_schema(engine, args.schema)

    for p in paths:
        ingest_file(engine, args.schema, p, replace=args.replace, chunksize=args.chunksize, dry_run=False)


if __name__ == "__main__":
    main()
