import argparse
import os

import sqlalchemy as sa
from dotenv import load_dotenv
from sqlalchemy import text


def required_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


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


def count_duplicates(conn, schema: str, table: str, cols: list[str]) -> int:
    part = ", ".join([qident(c) for c in cols])
    sql = text(
        f"""
        select count(*)::bigint
        from (
          select 1
          from {qident(schema)}.{qident(table)}
          group by {part}
          having count(*) > 1
        ) g
        """
    )
    return int(conn.execute(sql).scalar())


def count_duplicate_rows_to_delete(conn, schema: str, table: str, cols: list[str]) -> int:
    part = ", ".join([qident(c) for c in cols])
    sql = text(
        f"""
        select count(*)::bigint
        from (
          select
            row_number() over (partition by {part} order by ctid) as rn
          from {qident(schema)}.{qident(table)}
        ) x
        where x.rn > 1
        """
    )
    return int(conn.execute(sql).scalar())


def dedup(conn, schema: str, table: str, cols: list[str]) -> int:
    part = ", ".join([qident(c) for c in cols])
    sql = text(
        f"""
        with ranked as (
          select
            ctid,
            row_number() over (partition by {part} order by ctid) as rn
          from {qident(schema)}.{qident(table)}
        )
        delete from {qident(schema)}.{qident(table)} t
        using ranked r
        where t.ctid = r.ctid
          and r.rn > 1
        """
    )
    res = conn.execute(sql)
    # rowcount may be -1 for some drivers; fallback to recompute if needed
    return int(res.rowcount) if res.rowcount is not None and res.rowcount >= 0 else -1


def main() -> None:
    load_dotenv()
    url = "postgresql://%s:%s@%s:%s/%s" % (
        required_env("POSTGRES_USER"),
        required_env("POSTGRES_PASSWORD"),
        required_env("POSTGRES_HOST"),
        required_env("POSTGRES_PORT"),
        required_env("POSTGRES_DATABASE"),
    )

    parser = argparse.ArgumentParser(description="Remove duplicate rows in travis tables.")
    parser.add_argument("--execute", action="store_true", help="Actually delete duplicates (default: report only)")
    parser.add_argument(
        "--ignore-match-rule",
        action="store_true",
        help="Treat rows as duplicates even if only match_rule differs (keep one row).",
    )
    args = parser.parse_args()

    engine = sa.create_engine(url, pool_pre_ping=True)
    schema = "travis"
    tables = ["assignment_release", "foreclosure", "loan_originator", "recorder"]

    with engine.begin() as conn:
        for t in tables:
            all_cols = list_columns(conn, schema, t)
            cols = list(all_cols)
            if not cols:
                print(f"{schema}.{t}: no columns?")
                continue

            if args.ignore_match_rule and "match_rule" in cols:
                cols = [c for c in cols if c != "match_rule"]

            dup_groups = count_duplicates(conn, schema, t, cols)
            dup_rows = count_duplicate_rows_to_delete(conn, schema, t, cols)
            print(f"{schema}.{t}: duplicate_groups={dup_groups:,} duplicate_rows_to_delete={dup_rows:,}")

            if args.execute and dup_rows > 0:
                # If we are ignoring match_rule and the table has match_rule,
                # prefer keeping rows where match_rule = 'attom_id' (best linkage).
                if args.ignore_match_rule and "match_rule" in all_cols:
                    part = ", ".join([qident(c) for c in cols])
                    sql = text(
                        f"""
                        with ranked as (
                          select
                            ctid,
                            row_number() over (
                              partition by {part}
                              order by
                                case when match_rule = 'attom_id' then 0 else 1 end,
                                ctid
                            ) as rn
                          from {qident(schema)}.{qident(t)}
                        )
                        delete from {qident(schema)}.{qident(t)} tt
                        using ranked r
                        where tt.ctid = r.ctid
                          and r.rn > 1
                        """
                    )
                    res = conn.execute(sql)
                    deleted = int(res.rowcount) if res.rowcount is not None and res.rowcount >= 0 else -1
                else:
                    deleted = dedup(conn, schema, t, cols)
                if deleted == -1:
                    # rowcount unknown; re-check
                    dup_rows_after = count_duplicate_rows_to_delete(conn, schema, t, cols)
                    print(f"{schema}.{t}: deleted (rowcount unknown), remaining_duplicate_rows={dup_rows_after:,}")
                else:
                    print(f"{schema}.{t}: deleted_rows={deleted:,}")


if __name__ == "__main__":
    main()

