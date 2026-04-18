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
    engine = sa.create_engine(url, pool_pre_ping=True)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                select table_name
                from information_schema.tables
                where table_schema='travis' and table_type='BASE TABLE'
                order by table_name
                """
            )
        ).fetchall()
        print([r[0] for r in rows])


if __name__ == "__main__":
    main()

