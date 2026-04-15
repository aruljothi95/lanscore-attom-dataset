import os

import sqlalchemy as sa
from dotenv import load_dotenv
from sqlalchemy import text


def main() -> None:
    load_dotenv()
    u = "postgresql://%s:%s@%s:%s/%s" % (
        os.getenv("POSTGRES_USER"),
        os.getenv("POSTGRES_PASSWORD"),
        os.getenv("POSTGRES_HOST"),
        os.getenv("POSTGRES_PORT"),
        os.getenv("POSTGRES_DATABASE"),
    )
    e = sa.create_engine(u)
    with e.connect() as c:
        print(c.execute(text("select 1")).scalar())


if __name__ == "__main__":
    main()

