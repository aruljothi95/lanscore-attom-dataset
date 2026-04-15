import os

import sqlalchemy as sa
from dotenv import load_dotenv
from sqlalchemy import text


def main() -> None:
    load_dotenv()
    url = "postgresql://%s:%s@%s:%s/%s" % (
        os.getenv("POSTGRES_USER"),
        os.getenv("POSTGRES_PASSWORD"),
        os.getenv("POSTGRES_HOST"),
        os.getenv("POSTGRES_PORT"),
        os.getenv("POSTGRES_DATABASE"),
    )
    schema = os.getenv("POSTGRES_SCHEMA", "attom_dataset")
    engine = sa.create_engine(url)

    tables = ["tax_assessor", "recorder", "loan_originator", "foreclosure", "assignment_release"]
    with engine.connect() as conn:
        for t in tables:
            print(f"--- {schema}.{t} ---")
            rows = conn.execute(
                text(
                    """
                    select column_name
                    from information_schema.columns
                    where table_schema=:s and table_name=:t
                    order by ordinal_position
                    """
                ),
                {"s": schema, "t": t},
            ).fetchall()
            print(", ".join(r[0] for r in rows[:80]))
            if len(rows) > 80:
                print(f"... ({len(rows)} columns)")


if __name__ == "__main__":
    main()

