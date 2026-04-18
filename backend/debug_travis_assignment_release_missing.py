import os
import sys

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


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python debug_travis_assignment_release_missing.py <attom_id>")
    attom_id = sys.argv[1].strip()

    load_dotenv()
    url = "postgresql://%s:%s@%s:%s/%s" % (
        required_env("POSTGRES_USER"),
        required_env("POSTGRES_PASSWORD"),
        required_env("POSTGRES_HOST"),
        required_env("POSTGRES_PORT"),
        required_env("POSTGRES_DATABASE"),
    )
    attom_schema = os.getenv("POSTGRES_SCHEMA", "attom_dataset")
    engine = sa.create_engine(url, pool_pre_ping=True)

    with engine.connect() as conn:
        print(f"ATTOM ID: {attom_id}")

        # 1) Does attom_dataset.assignment_release have it?
        ar_cnt = conn.execute(
            text(f"select count(*)::bigint from {attom_schema}.assignment_release where attomid::text = :a"),
            {"a": attom_id},
        ).scalar()
        print(f"{attom_schema}.assignment_release rows with attomid={attom_id}: {int(ar_cnt):,}")

        # 2) Is it mapped to a travis property_id?
        pm = conn.execute(
            text(
                """
                select property_id, apn_norm
                from travis.property_map
                where tax_attom_id::text = :a
                limit 20
                """
            ),
            {"a": attom_id},
        ).fetchall()
        print(f"travis.property_map rows for tax_attom_id={attom_id}: {len(pm)}")
        for property_id, apn_norm in pm[:5]:
            print(" - property_id:", property_id, "apn_norm:", apn_norm)

        # 3) Is there already a travis.assignment_release row for those property_ids?
        if pm:
            pids = [r[0] for r in pm]
            ar_travis_cnt = conn.execute(
                text(
                    """
                    select count(*)::bigint
                    from travis.assignment_release
                    where property_id = any(:pids)
                    """
                ),
                {"pids": pids},
            ).scalar()
            print(f"travis.assignment_release rows for mapped property_ids: {int(ar_travis_cnt):,}")

        # 4) Check whether the join condition would match (apn_norm vs assignment_release.parcelnumberraw)
        join_cnt = conn.execute(
            text(
                f"""
                select count(*)::bigint
                from travis.property_map pm
                join {attom_schema}.assignment_release a
                  on pm.apn_norm = {norm_expr("a.parcelnumberraw")}
                where pm.tax_attom_id::text = :a
                """
            ),
            {"a": attom_id},
        ).scalar()
        print(f"Rows that would join via parcelnumberraw normalization: {int(join_cnt):,}")

        # 5) If join_cnt=0, show sample parcelnumberraw values for that attomid to debug formatting/missing
        if int(ar_cnt) > 0:
            samples = conn.execute(
                text(f"select parcelnumberraw from {attom_schema}.assignment_release where attomid::text = :a limit 20"),
                {"a": attom_id},
            ).fetchall()
            vals = [r[0] for r in samples]
            print("Sample assignment_release.parcelnumberraw values:", vals[:10])


if __name__ == "__main__":
    main()

