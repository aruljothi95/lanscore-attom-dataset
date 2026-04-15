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

    tables = [
        "assignment_release",
        "foreclosure",
        "loan_originator",
        "property_deletes",
        "property_to_boundarymatch_parcel",
        "recorder",
        "recorder_deletes",
        "tax_assessor",
    ]

    with engine.connect() as conn:
        for t in tables:
            try:
                n = conn.execute(text(f"select count(*) from {schema}.{t}")).scalar()
                print(f"{schema}.{t}: {n}")
            except Exception as e:
                print(f"{schema}.{t}: ERROR: {e}")


if __name__ == "__main__":
    main()

