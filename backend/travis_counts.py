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
    engine = sa.create_engine(url)
    tables = ["property_map", "recorder", "loan_originator", "foreclosure", "assignment_release"]
    with engine.connect() as conn:
        for t in tables:
            n = conn.execute(text(f"select count(*) from travis.{t}")).scalar()
            print(f"travis.{t}: {n}")


if __name__ == "__main__":
    main()

