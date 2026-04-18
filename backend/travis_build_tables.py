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
    # Normalize Parcel/APN strings: uppercase and remove all non-alphanumeric chars.
    return f"upper(regexp_replace(cast({sql_ident} as text), '[^0-9A-Za-z]', '', 'g'))"


def qident(ident: str) -> str:
    # Basic safe quoting for identifiers that we control (schema/table/column names).
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


def has_unique(conn, schema: str, table: str, column: str) -> bool:
    # True if a UNIQUE or PRIMARY KEY constraint exists that includes exactly this column.
    row = conn.execute(
        text(
            """
            select count(*)::int
            from information_schema.table_constraints tc
            join information_schema.constraint_column_usage ccu
              on ccu.constraint_name = tc.constraint_name
             and ccu.table_schema = tc.table_schema
             and ccu.table_name = tc.table_name
            where tc.table_schema=:s
              and tc.table_name=:t
              and tc.constraint_type in ('UNIQUE', 'PRIMARY KEY')
              and ccu.column_name=:c
            """
        ),
        {"s": schema, "t": table, "c": column},
    ).scalar()
    return bool(row and int(row) > 0)


@dataclass(frozen=True)
class Target:
    name: str
    # join strategy against property_map
    # - "attom": join on attom_id
    # - "apn": join on normalized APN/parcel columns
    strategy: str
    apn_cols: tuple[str, ...] = ()


TARGETS: list[Target] = [
    Target(name="recorder", strategy="attom+apn", apn_cols=("apnformatted", "apnoriginal")),
    Target(name="loan_originator", strategy="attom+apn", apn_cols=("apnformatted", "apnoriginal")),
    Target(name="foreclosure", strategy="attom+apn", apn_cols=("parcelnumberformatted",)),
    # assignment_release has 'attomid' and parcelnumberraw (sometimes unreliable); prefer attomid when available
    Target(name="assignment_release", strategy="attom+apn", apn_cols=("parcelnumberraw",)),
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

    with engine.begin() as conn:
        conn.execute(text(f"create schema if not exists {qident(travis_schema)};"))

        # 1) Check baselayer uniqueness for FK safety
        baselayer_table = "baselayer"
        baselayer_cols = list_columns(conn, travis_schema, baselayer_table)
        if "property_id" not in baselayer_cols or "parcel_id" not in baselayer_cols:
            raise RuntimeError("travis.baselayer must contain property_id and parcel_id")

        property_id_unique = has_unique(conn, travis_schema, baselayer_table, "property_id")

        # 2) Create property_map: baselayer -> tax_assessor via parcel_id == parcelnumberraw (normalized)
        conn.execute(text(f"drop table if exists {qident(travis_schema)}.property_map;"))
        conn.execute(
            text(
                f"""
                create table {qident(travis_schema)}.property_map as
                select distinct
                  b.property_id::text as property_id,
                  t.attom_id::text as tax_attom_id,
                  {norm_expr("b.parcel_id")} as apn_norm
                from {qident(travis_schema)}.{qident(baselayer_table)} b
                join {qident(attom_schema)}.tax_assessor t
                  on {norm_expr("b.parcel_id")} = {norm_expr("t.parcelnumberraw")}
                where b.property_id is not null and b.property_id <> ''
                  and b.parcel_id is not null and b.parcel_id <> ''
                  and t.attom_id is not null and t.attom_id <> ''
                """
            )
        )
        conn.execute(text(f"create index if not exists property_map_property_id_idx on {qident(travis_schema)}.property_map(property_id);"))
        conn.execute(text(f"create index if not exists property_map_tax_attom_id_idx on {qident(travis_schema)}.property_map(tax_attom_id);"))
        conn.execute(text(f"create index if not exists property_map_apn_norm_idx on {qident(travis_schema)}.property_map(apn_norm);"))

        # FK from property_map.property_id -> baselayer.property_id if unique exists
        if property_id_unique:
            conn.execute(
                text(
                    f"""
                    alter table {qident(travis_schema)}.property_map
                    add constraint property_map_property_id_fk
                    foreign key (property_id)
                    references {qident(travis_schema)}.{qident(baselayer_table)}(property_id)
                    """
                )
            )

        # 3) Create target tables in travis schema and load
        for tgt in TARGETS:
            src_table = tgt.name
            src_cols = list_columns(conn, attom_schema, src_table)

            # Build CREATE TABLE with property_id + match_rule + all src columns (as TEXT) for safety.
            dst_table = tgt.name
            conn.execute(text(f"drop table if exists {qident(travis_schema)}.{qident(dst_table)};"))

            col_defs = ['property_id text', 'match_rule text']
            for c in src_cols:
                col_defs.append(f"{qident(c)} text")

            conn.execute(
                text(
                    f"""
                    create table {qident(travis_schema)}.{qident(dst_table)} (
                      {", ".join(col_defs)}
                    );
                    """
                )
            )

            # Helper to select all src cols as text in stable order
            select_src_cols = ", ".join([f"o.{qident(c)}::text as {qident(c)}" for c in src_cols])
            insert_cols = ", ".join(["property_id", "match_rule"] + [qident(c) for c in src_cols])

            selects: list[str] = []

            if tgt.strategy in ("attom", "attom+apn"):
                # Join by ATTOM ID when available
                if "attom_id" in src_cols:
                    selects.append(
                        f"""
                        select pm.property_id, 'attom_id' as match_rule, {select_src_cols}
                        from {qident(travis_schema)}.property_map pm
                        join {qident(attom_schema)}.{qident(src_table)} o
                          on o.attom_id::text = pm.tax_attom_id
                        """
                    )
                elif "attomid" in src_cols:
                    selects.append(
                        f"""
                        select pm.property_id, 'attomid' as match_rule, {select_src_cols}
                        from {qident(travis_schema)}.property_map pm
                        join {qident(attom_schema)}.{qident(src_table)} o
                          on o.attomid::text = pm.tax_attom_id
                        """
                    )

            if tgt.strategy in ("apn", "attom+apn"):
                # Join by normalized APN/parcel columns
                for apn_col in tgt.apn_cols:
                    if apn_col not in src_cols:
                        continue
                    selects.append(
                        f"""
                        select pm.property_id, '{apn_col}' as match_rule, {select_src_cols}
                        from {qident(travis_schema)}.property_map pm
                        join {qident(attom_schema)}.{qident(src_table)} o
                          on pm.apn_norm = {norm_expr(f"o.{qident(apn_col)}")}
                        """
                    )

            if not selects:
                # No supported join path.
                continue

            # Insert distinct rows (property_id + all cols + match_rule).
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

            conn.execute(text(f"create index if not exists {dst_table}_property_id_idx on {qident(travis_schema)}.{qident(dst_table)}(property_id);"))

            if property_id_unique:
                conn.execute(
                    text(
                        f"""
                        alter table {qident(travis_schema)}.{qident(dst_table)}
                        add constraint {dst_table}_property_id_fk
                        foreign key (property_id)
                        references {qident(travis_schema)}.{qident(baselayer_table)}(property_id)
                        """
                    )
                )

        print("Done. Created travis.property_map and travis tables for targets.")
        print(f"FKs added: {'yes' if property_id_unique else 'no (baselayer.property_id not unique/PK)'}")


if __name__ == "__main__":
    main()

