import os
from datetime import datetime

import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv
from sqlalchemy import text


def required_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def main() -> None:
    load_dotenv()

    host = required_env("POSTGRES_HOST")
    port = required_env("POSTGRES_PORT")
    db = required_env("POSTGRES_DATABASE")
    user = required_env("POSTGRES_USER")
    pwd = required_env("POSTGRES_PASSWORD")
    schema = os.getenv("POSTGRES_SCHEMA", "attom_dataset")

    engine = sa.create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}", pool_pre_ping=True)

    # Normalization: uppercase and remove all non-alphanumeric chars
    # (handles dashes/spaces and most APN formatting differences).
    norm_expr = "upper(regexp_replace(cast({x} as text), '[^0-9A-Za-z]', '', 'g'))"

    # Define per-table join candidates: (tax_assessor_column, other_table_column)
    targets = [
        {
            "table": "recorder",
            "pairs": [
                ("parcelnumberformatted", "apnformatted"),
                ("parcelnumberraw", "apnformatted"),
                ("parcelnumberformatted", "apnoriginal"),
                ("parcelnumberraw", "apnoriginal"),
            ],
            "other_attom_col": "attom_id",
        },
        {
            "table": "loan_originator",
            "pairs": [
                ("parcelnumberformatted", "apnformatted"),
                ("parcelnumberraw", "apnformatted"),
                ("parcelnumberformatted", "apnoriginal"),
                ("parcelnumberraw", "apnoriginal"),
            ],
            "other_attom_col": "attom_id",
        },
        {
            "table": "foreclosure",
            "pairs": [
                ("parcelnumberformatted", "parcelnumberformatted"),
                ("parcelnumberraw", "parcelnumberformatted"),
            ],
            "other_attom_col": "attom_id",
        },
        {
            "table": "assignment_release",
            "pairs": [
                ("parcelnumberraw", "parcelnumberraw"),
                ("parcelnumberformatted", "parcelnumberraw"),
            ],
            "other_attom_col": "attomid",
        },
    ]

    summary_rows: list[dict] = []
    samples_rows: list[dict] = []

    with engine.connect() as conn:
        # Speed: temp tables + indexes per run.
        # These are session-local and will be dropped automatically.
        conn.execute(text("set statement_timeout = '0'"))  # allow long joins; tune as needed

        for tgt in targets:
            table = tgt["table"]
            other_attom_col = tgt["other_attom_col"]

            for tax_col, other_col in tgt["pairs"]:
                match_name = f"tax_assessor.{tax_col} = {table}.{other_col}"

                print(f"[RUN] {match_name}", flush=True)

                # Create normalized temp tables with indexes for fast joins.
                conn.execute(text("drop table if exists temp_tax"))
                conn.execute(text("drop table if exists temp_oth"))

                conn.execute(
                    text(
                        f"""
                        create temporary table temp_tax as
                        select
                          t.attom_id as tax_attom_id,
                          {norm_expr.format(x=f"t.{tax_col}")} as tax_norm
                        from {schema}.tax_assessor t
                        where t.{tax_col} is not null and cast(t.{tax_col} as text) <> ''
                        """
                    )
                )
                conn.execute(text("create index on temp_tax (tax_norm)"))

                conn.execute(
                    text(
                        f"""
                        create temporary table temp_oth as
                        select
                          o.{other_attom_col} as other_attom_id,
                          {norm_expr.format(x=f"o.{other_col}")} as oth_norm
                        from {schema}.{table} o
                        where o.{other_col} is not null and cast(o.{other_col} as text) <> ''
                        """
                    )
                )
                conn.execute(text("create index on temp_oth (oth_norm)"))

                sql = text(
                    """
                    select
                      count(*)::bigint as matched_rows,
                      count(distinct t.tax_attom_id)::bigint as distinct_tax_attom_ids,
                      count(distinct o.other_attom_id)::bigint as distinct_other_attom_ids
                    from temp_tax t
                    join temp_oth o
                      on t.tax_norm = o.oth_norm
                    """
                )

                res = conn.execute(sql).mappings().one()
                summary_rows.append(
                    {
                        "target_table": table,
                        "match_rule": match_name,
                        "matched_rows": int(res["matched_rows"]),
                        "distinct_tax_attom_ids": int(res["distinct_tax_attom_ids"]),
                        "distinct_other_attom_ids": int(res["distinct_other_attom_ids"]),
                    }
                )

                # Small sample of matching ATTOM IDs for auditing (first 200).
                sample_sql = text(
                    """
                    select t.tax_attom_id, o.other_attom_id
                    from temp_tax t
                    join temp_oth o on t.tax_norm = o.oth_norm
                    limit 200
                    """
                )
                for r in conn.execute(sample_sql).fetchall():
                    samples_rows.append(
                        {
                            "target_table": table,
                            "match_rule": match_name,
                            "tax_attom_id": r[0],
                            "other_attom_id": r[1],
                        }
                    )

    df_summary = pd.DataFrame(summary_rows).sort_values(["target_table", "match_rule"])
    df_samples = pd.DataFrame(samples_rows)

    out_name = f"parcel_apn_match_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    out_path = os.path.join(os.path.dirname(__file__), out_name)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_summary.to_excel(writer, index=False, sheet_name="Summary")
        df_samples.to_excel(writer, index=False, sheet_name="Samples (first 200)")

    print(out_path)


if __name__ == "__main__":
    main()

