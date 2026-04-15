# import pandas as pd
# import re
# import os
# from sqlalchemy import create_engine, text
# from dotenv import load_dotenv

# load_dotenv()

# DB_HOST = os.getenv("POSTGRES_HOST")
# DB_PORT = os.getenv("POSTGRES_PORT")
# DB_NAME = os.getenv("POSTGRES_DATABASE")
# DB_USER = os.getenv("POSTGRES_USER")
# DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
# DB_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")

# engine = create_engine(
#     f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# )

# TABLE_NAME = "recorder"
# FILE_PATH = "NIKASHA_LOANORIGINATORANALYTICS_0001.tsv"


# # ───────────────── sanitize column names ─────────────────
# def clean(col):
#     col = col.strip()
#     col = col.replace("[", "").replace("]", "")
#     col = col.replace('"', "")
#     col = re.sub(r"\s+", "_", col)
#     col = re.sub(r"[^a-zA-Z0-9_]", "", col)
#     return col.lower()


# def create_table_from_tsv(file_path):
#     df = pd.read_csv(file_path, sep="\t", nrows=1)

#     original_cols = df.columns.tolist()
#     cleaned_cols = [clean(c) for c in original_cols]

#     columns_sql = []

#     # system columns
#     columns_sql.append("id UUID PRIMARY KEY")
#     columns_sql.append("ingested_at TIMESTAMP")

#     # TSV columns → TEXT (safe for huge ingestion)
#     for col in cleaned_cols:
#         columns_sql.append(f"{col} TEXT")

#     ddl = f"""
#     DROP TABLE IF EXISTS {DB_SCHEMA}.{TABLE_NAME};

#     CREATE TABLE {DB_SCHEMA}.{TABLE_NAME} (
#         {",".join(columns_sql)}
#     );
#     """

#     with engine.begin() as conn:
#         conn.execute(text(ddl))

#     print("✅ Table created successfully with", len(cleaned_cols), "columns")


# if __name__ == "__main__":
#     create_table_from_tsv(FILE_PATH)


import os
import uuid
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from io import StringIO

# ---------------- ENV ----------------
load_dotenv()

DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DATABASE")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

TABLE_NAME = "recorder"
CHECKPOINT_FILE = "ingest_checkpoint.txt"

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ingest")

engine = create_engine(DATABASE_URL)

CHUNKSIZE = 100_000


# ---------------- CHECKPOINT ----------------
def save_checkpoint(chunk_id):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(chunk_id))


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return 0
    with open(CHECKPOINT_FILE, "r") as f:
        return int(f.read().strip())


# ---------------- COLUMN CLEANING ----------------
def clean_columns(df):
    df.columns = (
        df.columns
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

    # CLEAN HEADERS (CRITICAL FIX)
    df = clean_columns(df)

    # ADD SYSTEM COLUMNS
    df.insert(0, "id", [str(uuid.uuid4()) for _ in range(len(df))])
    df["ingested_at"] = datetime.utcnow()

    # NULL SAFETY
    df = df.where(pd.notnull(df), None)

    return df


# ---------------- INGEST ----------------
def ingest(file_path, file_type="tsv"):
    start_chunk = load_checkpoint()
    log.info(f"Resuming from chunk: {start_chunk}")

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

            # COPY BUFFER
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cols = ",".join(df.columns)

            sql = f"""
                COPY {DB_SCHEMA}.{TABLE_NAME} ({cols})
                FROM STDIN WITH CSV
            """

            with engine.raw_connection() as conn:
                cursor = conn.cursor()

                try:
                    cursor.copy_expert(sql, buffer)
                    conn.commit()
                    log.info(f"Chunk {i} committed successfully")
                    save_checkpoint(i + 1)

                except Exception as e:
                    conn.rollback()
                    log.error(f"Chunk {i} FAILED: {e}")
                    raise

        except Exception as e:
            log.error(f"Fatal error in chunk {i}: {e}")
            continue

    log.info("INGESTION COMPLETE")


# ---------------- ENTRY ----------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--filetype", default="tsv")

    args = parser.parse_args()

    print("🚀 STARTING INGESTION")
    print("FILE:", args.file)
    print("TYPE:", args.filetype)

    ingest(args.file, args.filetype)