import pandas as pd

input_file = "NIKASHA_TAXASSESSOR_0001.txt"
output_file = "NIKASHA_TAXASSESSOR_0001.xlsx"

chunks = []

# Use python engine (more memory-safe)
for chunk in pd.read_csv(
    input_file,
    delimiter="\t",       # change if needed
    chunksize=5000,
    engine="python",      # important fix
    encoding="latin1"     # prevents decode errors
):
    chunks.append(chunk)

df = pd.concat(chunks, ignore_index=True)

print(f"Columns: {len(df.columns)}")

df.to_excel(output_file, index=False)

print("✅ Excel file created:", output_file)