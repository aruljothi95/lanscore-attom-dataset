import os
import uuid
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

# ---------------- DB CONFIG ----------------
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DATABASE")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_SCHEMA = os.getenv("POSTGRES_SCHEMA", "attom_dataset")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ingest")

# ---------------- CONFIG ----------------
CHUNKSIZE = 50000


# ---------------- TABLE ROUTER ----------------
def get_table_name(file_path: str):
    name = os.path.basename(file_path).upper()

    if "FORECLOSURE" in name:
        return "foreclosure"
    elif "LOANORIGINATOR" in name:
        return "loan_originator"
    elif "TAXASSESSOR" in name:
        return "tax_assessor"
    else:
        return "recorder"


# ---------------- CHECKPOINT ----------------
def checkpoint_file(file_path):
    safe = os.path.basename(file_path).replace(".", "_")
    return f"checkpoint_{safe}.txt"


def save_checkpoint(path, chunk_id):
    with open(checkpoint_file(path), "w") as f:
        f.write(str(chunk_id))


def load_checkpoint(path):
    cf = checkpoint_file(path)
    if not os.path.exists(cf):
        return 0
    with open(cf, "r") as f:
        return int(f.read().strip())


# ---------------- CLEAN COLUMNS ----------------
def clean_columns(df):
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.replace("[", "", regex=False)
        .str.replace("]", "", regex=False)
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    return df


# ---------------- TRANSFORM ----------------
def transform(df):
    df = df.copy()

    df = clean_columns(df)

    df.insert(0, "id", [str(uuid.uuid4()) for _ in range(len(df))])
    df["ingested_at"] = datetime.utcnow()

    df = df.where(pd.notnull(df), None)

    return df


# ---------------- INGEST ----------------
def ingest(file_path, file_type="tsv"):
    table_name = get_table_name(file_path)
    start_chunk = load_checkpoint(file_path)

    log.info(f"🚀 FILE: {file_path}")
    log.info(f"📦 TABLE: {table_name}")
    log.info(f"♻️ RESUME FROM CHUNK: {start_chunk}")

    if file_type == "tsv":
        reader = pd.read_csv(file_path, sep="\t", chunksize=CHUNKSIZE, low_memory=False)
    else:
        reader = pd.read_csv(file_path, chunksize=CHUNKSIZE, low_memory=False)

    for i, chunk in enumerate(reader):

        if i < start_chunk:
            continue

        try:
            log.info(f"Processing chunk {i}")

            df = transform(chunk)

            from io import StringIO
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            with engine.raw_connection() as conn:
                cursor = conn.cursor()

                cols = ",".join(df.columns)

                query = f"""
                    COPY {DB_SCHEMA}.{table_name} ({cols})
                    FROM STDIN WITH CSV
                """

                cursor.copy_expert(query, buffer)
                conn.commit()

            save_checkpoint(file_path, i + 1)

            log.info(f"Chunk {i} committed successfully")

        except Exception as e:
            log.error(f"Chunk {i} FAILED: {e}")
            continue

    log.info("✅ INGESTION COMPLETE")


# ---------------- ENTRY ----------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--filetype", default="tsv")

    args = parser.parse_args()

    ingest(args.file, args.filetype)