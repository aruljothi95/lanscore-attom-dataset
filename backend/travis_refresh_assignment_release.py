import os

import sqlalchemy as sa
from dotenv import load_dotenv
from sqlalchemy import text


def required_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def norm_expr(sql_ident: str) -> str:
    return f"upper(regexp_replace(cast({sql_ident} as text), '[^0-9A-Za-z]', '', 'g'))"


def qident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def list_columns(conn, schema: str, table: str) -> list[str]:
    rows = conn.execute(
        text(
            """
            select column_name
            from information_schema.columns
            where table_schema=:s and table_name=:t
            order by ordinal_position
            """
        ),
        {"s": schema, "t": table},
    ).fetchall()
    return [r[0] for r in rows]


def main() -> None:
    load_dotenv()
    host = required_env("POSTGRES_HOST")
    port = required_env("POSTGRES_PORT")
    db = required_env("POSTGRES_DATABASE")
    user = required_env("POSTGRES_USER")
    pwd = required_env("POSTGRES_PASSWORD")
    attom_schema = os.getenv("POSTGRES_SCHEMA", "attom_dataset")
    travis_schema = "travis"

    engine = sa.create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}", pool_pre_ping=True)

    with engine.begin() as conn:
        src_table = "assignment_release"
        dst_table = "assignment_release"
        src_cols = list_columns(conn, attom_schema, src_table)

        # Recreate travis.assignment_release
        conn.execute(text(f"drop table if exists {qident(travis_schema)}.{qident(dst_table)};"))

        col_defs = ["property_id text", "match_rule text"] + [f"{qident(c)} text" for c in src_cols]
        conn.execute(
            text(
                f"""
                create table {qident(travis_schema)}.{qident(dst_table)} (
                  {", ".join(col_defs)}
                );
                """
            )
        )

        select_src_cols = ", ".join([f"o.{qident(c)}::text as {qident(c)}" for c in src_cols])
        insert_cols = ", ".join(["property_id", "match_rule"] + [qident(c) for c in src_cols])

        selects = []

        # Prefer attomid join if present
        if "attomid" in src_cols:
            selects.append(
                f"""
                select pm.property_id, 'attomid' as match_rule, {select_src_cols}
                from {qident(travis_schema)}.property_map pm
                join {qident(attom_schema)}.{qident(src_table)} o
                  on o.attomid::text = pm.tax_attom_id
                """
            )

        # Parcel fallback join if parcelnumberraw exists
        if "parcelnumberraw" in src_cols:
            selects.append(
                f"""
                select pm.property_id, 'parcelnumberraw' as match_rule, {select_src_cols}
                from {qident(travis_schema)}.property_map pm
                join {qident(attom_schema)}.{qident(src_table)} o
                  on pm.apn_norm = {norm_expr(f"o.{qident('parcelnumberraw')}")}
                """
            )

        if not selects:
            raise RuntimeError("No supported join columns found for assignment_release")

        union_sql = " union all ".join(selects)
        conn.execute(
            text(
                f"""
                insert into {qident(travis_schema)}.{qident(dst_table)} ({insert_cols})
                select distinct * from (
                  {union_sql}
                ) s
                """
            )
        )

        conn.execute(text(f"create index if not exists assignment_release_property_id_idx on {qident(travis_schema)}.{qident(dst_table)}(property_id);"))

        # Dedup ignoring match_rule, keeping attomid first
        conn.execute(
            text(
                f"""
                with ranked as (
                  select
                    ctid,
                    row_number() over (
                      partition by property_id, {", ".join([qident(c) for c in src_cols])}
                      order by
                        case when match_rule = 'attomid' then 0 else 1 end,
                        ctid
                    ) as rn
                  from {qident(travis_schema)}.{qident(dst_table)}
                )
                delete from {qident(travis_schema)}.{qident(dst_table)} t
                using ranked r
                where t.ctid = r.ctid and r.rn > 1
                """
            )
        )

    print("Refreshed travis.assignment_release (joins: attomid + parcelnumberraw).")


if __name__ == "__main__":
    main()

