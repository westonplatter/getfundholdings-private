import pandas as pd

# Read the holdings file
df = pd.read_csv(
    "data/holdings_enriched_JEPQ_0001485894_S000076132_20250331_20250712_135249.csv"
)

# Filter for Wells Fargo entries (case insensitive)
wells_fargo_df = df[df["name"].str.contains("Wells Fargo", case=False, na=False)]

print("Wells Fargo Holdings:")
print("=" * 80)
print(f"Found {len(wells_fargo_df)} Wells Fargo entries\n")

# Display the filtered dataframe
print("Original DataFrame (Wells Fargo entries):")
print(wells_fargo_df)
print("\n" + "=" * 80 + "\n")

# Transpose the dataframe
transposed_df = wells_fargo_df.transpose()

print("Transposed DataFrame:")
print(transposed_df)
