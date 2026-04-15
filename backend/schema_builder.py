import pandas as pd
import re

def clean_col(col):
    col = str(col).strip().lower()
    col = re.sub(r"\[|\]", "", col)
    col = re.sub(r"[^\w]", "_", col)
    col = re.sub(r"_+", "_", col)
    return col


def infer_schema(file_path, sep="\t"):
    df = pd.read_csv(file_path, sep=sep, nrows=1000, dtype=str)
    columns = [clean_col(c) for c in df.columns]
    return columns


def generate_create_table_sql(schema, table_name):
    cols_sql = []

    cols_sql.append("id UUID PRIMARY KEY")

    for col in schema:
        if col.strip() == "":
            continue
        cols_sql.append(f"{col} TEXT")

    cols_sql.append("ingested_at TIMESTAMP")

    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {", ".join(cols_sql)}
    );
    """

    return sql