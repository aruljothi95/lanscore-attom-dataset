import pandas as pd

df = pd.read_csv(
    "NIKASHA_LOANORIGINATORANALYTICS_0001.tsv",
    sep="\t",
    engine="python",
    on_bad_lines="warn"
)

print("ROW COUNT:", len(df))
print("COLUMNS:", len(df.columns))