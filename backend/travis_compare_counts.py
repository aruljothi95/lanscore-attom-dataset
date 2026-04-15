import os
from dataclasses import dataclass

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


@dataclass(frozen=True)
class Target:
    name: str
    strategy: str
    apn_cols: tuple[str, ...] = ()


TARGETS: list[Target] = [
    Target(name="recorder", strategy="attom+apn", apn_cols=("apnformatted", "apnoriginal")),
    Target(name="loan_originator", strategy="attom+apn", apn_cols=("apnformatted", "apnoriginal")),
    Target(name="foreclosure", strategy="attom+apn", apn_cols=("parcelnumberformatted",)),
    Target(name="assignment_release", strategy="apn", apn_cols=("parcelnumberraw",)),
]


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

    with engine.connect() as conn:
        # Compare per target table
        for tgt in TARGETS:
            dst_table = tgt.name

            # Actual rows inserted (grouped by match_rule)
            actual = {
                r[0]: int(r[1])
                for r in conn.execute(
                    text(
                        f"""
                        select match_rule, count(*)::bigint
                        from {qident(travis_schema)}.{qident(dst_table)}
                        group by match_rule
                        order by match_rule
                        """
                    )
                ).fetchall()
            }

            # Expected counts recomputed using the same joins as travis_build_tables.py
            src_cols = list_columns(conn, attom_schema, dst_table)
            selects: list[tuple[str, str]] = []  # (rule, sql)

            if tgt.strategy in ("attom", "attom+apn"):
                if "attom_id" in src_cols:
                    selects.append(
                        (
                            "attom_id",
                            f"""
                            select pm.property_id, 'attom_id' as match_rule
                            from {qident(travis_schema)}.property_map pm
                            join {qident(attom_schema)}.{qident(dst_table)} o
                              on o.attom_id::text = pm.tax_attom_id
                            """,
                        )
                    )
                elif "attomid" in src_cols:
                    selects.append(
                        (
                            "attomid",
                            f"""
                            select pm.property_id, 'attomid' as match_rule
                            from {qident(travis_schema)}.property_map pm
                            join {qident(attom_schema)}.{qident(dst_table)} o
                              on o.attomid::text = pm.tax_attom_id
                            """,
                        )
                    )

            if tgt.strategy in ("apn", "attom+apn"):
                for apn_col in tgt.apn_cols:
                    if apn_col not in src_cols:
                        continue
                    selects.append(
                        (
                            apn_col,
                            f"""
                            select pm.property_id, '{apn_col}' as match_rule
                            from {qident(travis_schema)}.property_map pm
                            join {qident(attom_schema)}.{qident(dst_table)} o
                              on pm.apn_norm = {norm_expr(f"o.{qident(apn_col)}")}
                            """,
                        )
                    )

            expected = {}
            expected_raw = {}
            for rule, sel in selects:
                # Expected rows if we count distinct (property_id, match_rule) only
                n_distinct = int(
                    conn.execute(
                        text(
                            f"""
                            select count(*)::bigint from (
                              select distinct * from ({sel}) s
                            ) x
                            """
                        )
                    ).scalar()
                )
                expected[rule] = n_distinct

                # Expected raw join rows (includes multiplication if property_map has duplicates)
                n_raw = int(conn.execute(text(f"select count(*)::bigint from ({sel}) s")).scalar())
                expected_raw[rule] = n_raw

            all_rules = sorted(set(actual.keys()) | set(expected.keys()))

            print(f"\n=== travis.{dst_table} ===")
            print("rule\tactual_rows\texpected_distinct(property_id)\texpected_raw_join")
            for r in all_rules:
                a = actual.get(r, 0)
                e = expected.get(r, 0)
                er = expected_raw.get(r, 0)
                print(f"{r}\t{a}\t{e}\t{er}")

        print("\nNote: These counts are comparable to travis builds (via travis.property_map),")
        print("not to parcel_apn_match_report.xlsx (which compares attom_dataset tables directly).")


if __name__ == "__main__":
    main()

