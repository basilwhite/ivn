import pandas as pd

df = pd.read_excel('Components.xlsx')
print("=" * 60)
print("COMPONENTS.XLSX STRUCTURE ANALYSIS")
print("=" * 60)
print(f"\nColumns ({len(df.columns)}):")
for col in df.columns:
    print(f"  - {col}")

print(f"\nTotal rows: {len(df)}")

print("\nColumn data types:")
for col in df.columns:
    print(f"  {col}: {df[col].dtype}")

print("\nSample values (first 3 rows):")
for i, row in df.head(3).iterrows():
    print(f"\n  Row {i}:")
    for col in df.columns:
        val = str(row[col])[:80] if pd.notna(row[col]) else "NaN"
        print(f"    {col}: {val}")

print("\n\nAll column names for copy-paste:")
print(list(df.columns))
