import os
import re
from typing import Any

import sqlalchemy as sa
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import text


load_dotenv()


def required_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var {name}")
    return v


DB_HOST = required_env("POSTGRES_HOST")
DB_PORT = required_env("POSTGRES_PORT")
DB_NAME = required_env("POSTGRES_DATABASE")
DB_USER = required_env("POSTGRES_USER")
DB_PASSWORD = required_env("POSTGRES_PASSWORD")
DB_SCHEMA = os.getenv("POSTGRES_SCHEMA", "attom_dataset")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = sa.create_engine(DATABASE_URL, pool_pre_ping=True)


app = FastAPI(title="ATTOM Data API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class TablesResponse(BaseModel):
    schema: str
    tables: list[str]


class PageResponse(BaseModel):
    schema: str
    table: str
    page: int
    page_size: int
    total_rows: int
    columns: list[str]
    rows: list[dict[str, Any]]


def list_tables(schema: str) -> list[str]:
    sql = text(
        """
        select table_name
        from information_schema.tables
        where table_schema = :schema
          and table_type = 'BASE TABLE'
        order by table_name
        """
    )
    with engine.connect() as conn:
        return [r[0] for r in conn.execute(sql, {"schema": schema}).fetchall()]


HIDDEN_COLS = {"id", "transactionid", "transaction_id", "attom_id", "attomid"}


_ident_re = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def quote_ident(ident: str) -> str:
    # Very strict identifier validation to avoid injection.
    if not _ident_re.match(ident):
        raise ValueError(f"Invalid identifier: {ident}")
    return '"' + ident.replace('"', '""') + '"'


def list_columns(schema: str, table: str) -> list[str]:
    insp = sa.inspect(engine)
    cols = [c["name"] for c in insp.get_columns(table, schema=schema)]
    return [c for c in cols if c.lower() not in HIDDEN_COLS]


@app.get("/health")
def health() -> dict[str, str]:
    with engine.connect() as conn:
        conn.execute(text("select 1"))
    return {"status": "ok"}


@app.get("/tables", response_model=TablesResponse)
def get_tables(schema: str = Query(default=DB_SCHEMA)) -> TablesResponse:
    return TablesResponse(schema=schema, tables=list_tables(schema))


@app.get("/tables/{table}/rows", response_model=PageResponse)
def get_table_rows(
    table: str,
    schema: str = Query(default=DB_SCHEMA),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=500),
    q: str | None = Query(default=None, description="Search across any column (case-insensitive)"),
    f: list[str] | None = Query(
        default=None,
        description="Column-wise filters. Repeat param: f=column=value (case-insensitive contains).",
    ),
) -> PageResponse:
    tables = set(list_tables(schema))
    if table not in tables:
        raise HTTPException(status_code=404, detail=f"Unknown table: {schema}.{table}")

    offset = (page - 1) * page_size

    try:
        visible_cols = list_columns(schema, table)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not visible_cols:
        return PageResponse(
            schema=schema,
            table=table,
            page=page,
            page_size=page_size,
            total_rows=0,
            columns=[],
            rows=[],
        )

    params: dict[str, Any] = {"limit": page_size, "offset": offset}
    clauses: list[str] = []

    if q and q.strip():
        params["q"] = f"%{q.strip()}%"
        concat = "concat_ws(' ', " + ", ".join([f"cast({quote_ident(c)} as text)" for c in visible_cols]) + ")"
        clauses.append(f"({concat} ILIKE :q)")

    if f:
        # Group filters by column: OR within a column, AND across columns.
        visible_lower = {c.lower(): c for c in visible_cols}
        grouped: dict[str, list[str]] = {}
        for raw in f:
            if "=" not in raw:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid filter format: {raw} (expected column=value)",
                )
            col_in, val = raw.split("=", 1)
            col_in = col_in.strip()
            val = val.strip()
            if not col_in:
                raise HTTPException(status_code=400, detail=f"Invalid filter column: {raw}")
            if col_in.lower() not in visible_lower:
                raise HTTPException(status_code=400, detail=f"Unknown/hidden filter column: {col_in}")
            col = visible_lower[col_in.lower()]
            grouped.setdefault(col, []).append(val)

        idx = 0
        for col, values in grouped.items():
            ors: list[str] = []
            for v in values:
                key = f"f{idx}"
                idx += 1
                params[key] = f"%{v}%"
                ors.append(f"(cast({quote_ident(col)} as text) ILIKE :{key})")
            if ors:
                clauses.append("(" + " OR ".join(ors) + ")")

    where_sql = f" where ({' AND '.join(clauses)})" if clauses else ""

    with engine.connect() as conn:
        total = int(
            conn.execute(
                text(f"select count(*) from {quote_ident(schema)}.{quote_ident(table)}{where_sql}"),
                params,
            ).scalar()
        )

        select_cols = ", ".join([quote_ident(c) for c in visible_cols])
        result = conn.execute(
            text(
                f"select {select_cols} from {quote_ident(schema)}.{quote_ident(table)}{where_sql} limit :limit offset :offset"
            ),
            params,
        )
        cols = list(result.keys())
        rows = [dict(zip(cols, r)) for r in result.fetchall()]

    return PageResponse(
        schema=schema,
        table=table,
        page=page,
        page_size=page_size,
        total_rows=total,
        columns=cols,
        rows=rows,
    )

