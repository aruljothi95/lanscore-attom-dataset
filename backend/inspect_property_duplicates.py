import os
import sys

import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv
from sqlalchemy import text


def required_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def norm(s: str) -> str:
    return "".join(ch for ch in s.upper() if ch.isalnum())


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python inspect_property_duplicates.py <parcel_id>")
    parcel_id = sys.argv[1]
    parcel_norm = norm(parcel_id)

    load_dotenv()
    url = "postgresql://%s:%s@%s:%s/%s" % (
        required_env("POSTGRES_USER"),
        required_env("POSTGRES_PASSWORD"),
        required_env("POSTGRES_HOST"),
        required_env("POSTGRES_PORT"),
        required_env("POSTGRES_DATABASE"),
    )
    engine = sa.create_engine(url, pool_pre_ping=True)

    with engine.connect() as conn:
        props = conn.execute(
            text(
                """
                select distinct property_id, parcel_id
                from travis.baselayer
                where upper(regexp_replace(cast(parcel_id as text), '[^0-9A-Za-z]', '', 'g')) = :p
                """
            ),
            {"p": parcel_norm},
        ).fetchall()

        if not props:
            print("No baselayer property_id found for parcel_id", parcel_id)
            return

        print("Baselayer matches:")
        for pid, raw in props:
            print(" - property_id:", pid, "| parcel_id:", raw)

        # For each property_id, inspect travis.loan_originator rows
        for pid, _raw in props:
            rows = conn.execute(
                text("select * from travis.loan_originator where property_id = :pid"),
                {"pid": pid},
            ).mappings().all()

            df = pd.DataFrame(rows)
            print(f"\ntravis.loan_originator rows for property_id={pid}: {len(df)}")
            if df.empty:
                continue

            # Check full-row duplicates (including match_rule)
            full_dups = df.duplicated(keep=False)
            print("Full-row duplicates (including match_rule):", int(full_dups.sum()))

            # Check duplicates ignoring match_rule (common duplication cause)
            if "match_rule" in df.columns:
                cols_wo = [c for c in df.columns if c != "match_rule"]
                wo_dups = df.duplicated(subset=cols_wo, keep=False)
                print("Duplicates ignoring match_rule:", int(wo_dups.sum()))
                if wo_dups.any():
                    print("Sample duplicate groups (ignoring match_rule):")
                    grp = df[wo_dups].groupby(cols_wo, dropna=False).size().reset_index(name="count")
                    print(grp.sort_values("count", ascending=False).head(5).to_string(index=False))


if __name__ == "__main__":
    main()

