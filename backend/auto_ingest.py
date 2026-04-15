import os
import uuid
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
# from sqlalchemy import create_engine
from sqlalchemy import text
from io import StringIO

from schema_builder import infer_schema, generate_create_table_sql, clean_col

load_dotenv()

# ---------------- DB ----------------
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DATABASE")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_SCHEMA = os.getenv("POSTGRES_SCHEMA", "attom_dataset")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ingest")

CHUNK_SIZE = 50000


# ---------------- TABLE NAME ----------------
def get_table_name(file_path):
    name = file_path.lower()

    if "foreclosure" in name:
        return "attom_dataset.foreclosure"
    if "loanoriginator" in name:
        return "attom_dataset.loan_originator"
    if "taxassessor" in name:
        return "attom_dataset.tax_assessor"

    return "attom_dataset.generic_table"


# ---------------- CREATE TABLE ----------------
# def create_table(file_path, table_name):
#     schema = infer_schema(file_path)
#     sql = generate_create_table_sql(schema, table_name)

#     with engine.connect() as conn:
#         conn.execute(sql)

#     log.info(f"Table ready: {table_name}")

def create_table(file_path, table_name):
    schema = infer_schema(file_path)
    sql = generate_create_table_sql(schema, table_name)

    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

    log.info(f"Table ready: {table_name}")

# ---------------- TRANSFORM ----------------
def transform(df):
    df.columns = [clean_col(c) for c in df.columns]

    df.insert(0, "id", [str(uuid.uuid4()) for _ in range(len(df))])
    df["ingested_at"] = datetime.utcnow()

    df = df.where(pd.notnull(df), None)
    return df


# ---------------- INGEST ----------------
def ingest(file_path):
    table = get_table_name(file_path)

    log.info(f"FILE: {file_path}")
    log.info(f"TABLE: {table}")

    # STEP 1: CREATE TABLE AUTOMATICALLY
    create_table(file_path, table.split(".")[1])

    reader = pd.read_csv(
        file_path,
        sep="\t",
        chunksize=CHUNK_SIZE,
        dtype=str,
        low_memory=False
    )

    for i, chunk in enumerate(reader):
        try:
            log.info(f"Chunk {i}")

            df = transform(chunk)

            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cols = ",".join(df.columns)

            query = f"""
                COPY {table} ({cols})
                FROM STDIN WITH CSV
            """

            with engine.raw_connection() as conn:
                cur = conn.cursor()
                cur.copy_expert(query, buffer)
                conn.commit()

            log.info(f"Chunk {i} done")

        except Exception as e:
            log.error(f"Chunk {i} failed: {e}")
            break

    log.info("INGESTION COMPLETE")


# ---------------- RUN ----------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)

    args = parser.parse_args()

    ingest(args.file)