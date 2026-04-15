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


def main() -> None:
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
        # 1) How many tax_assessor parcels match travis.baselayer.parcel_id?
        # We count distinct normalized parcel ids to avoid formatting noise.
        base_match = conn.execute(
            text(
                f"""
                with m as (
                  select
                    {norm_expr("b.parcel_id")} as parcel_norm,
                    t.attom_id as tax_attom_id,
                    b.property_id as property_id
                  from travis.baselayer b
                  join {attom_schema}.tax_assessor t
                    on {norm_expr("b.parcel_id")} = {norm_expr("t.parcelnumberraw")}
                  where b.parcel_id is not null and b.parcel_id <> ''
                    and t.parcelnumberraw is not null and t.parcelnumberraw <> ''
                )
                select
                  count(distinct parcel_norm)::bigint as distinct_matched_parcels,
                  count(distinct tax_attom_id)::bigint as distinct_tax_attom_ids,
                  count(distinct property_id)::bigint as distinct_property_ids
                from m
                """
            )
        ).mappings().one()

        print("=== Tax Assessor <-> Travis Baselayer (parcel_id match) ===")
        print(f"Distinct matched parcels (normalized): {int(base_match['distinct_matched_parcels']):,}")
        print(f"Distinct matched tax_assessor ATTOM IDs: {int(base_match['distinct_tax_attom_ids']):,}")
        print(f"Distinct matched baselayer property_id: {int(base_match['distinct_property_ids']):,}")

        # 2) For those matched parcels/property_ids, how many rows exist in each travis table?
        tables = ["recorder", "loan_originator", "foreclosure", "assignment_release"]
        print("\n=== Counts in travis tables for matched properties ===")
        print("table\trows\tdistinct_property_id\tdistinct_tax_attom_id")

        for t in tables:
            r = conn.execute(
                text(
                    f"""
                    select
                      count(*)::bigint as rows,
                      count(distinct x.property_id)::bigint as distinct_property_id,
                      count(distinct pm.tax_attom_id)::bigint as distinct_tax_attom_id
                    from travis.{t} x
                    left join travis.property_map pm
                      on pm.property_id = x.property_id
                    """
                )
            ).mappings().one()
            print(
                f"{t}\t{int(r['rows']):,}\t{int(r['distinct_property_id']):,}\t{int(r['distinct_tax_attom_id']):,}"
            )


if __name__ == "__main__":
    main()

