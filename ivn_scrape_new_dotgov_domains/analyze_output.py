import pandas as pd

df = pd.read_excel('ivn_discovered_alignments.xlsx')

print("=" * 60)
print("OUTPUT FILE ANALYSIS: ivn_discovered_alignments.xlsx")
print("=" * 60)

print(f"\nTotal rows: {len(df)}")
print(f"Columns: {list(df.columns)}")

print(f"\nUnique values:")
print(f"  - Domains: {df['Domain'].nunique()}")
print(f"  - Components: {df['Component'].nunique()}")
print(f"  - Source URLs: {df['Source'].nunique()}")
print(f"  - Document URLs: {df['URL'].nunique()}")

print(f"\nTop 10 domains by document count:")
for dom, cnt in df['Domain'].value_counts().head(10).items():
    print(f"  {dom}: {cnt}")

print(f"\nDocument types (by extension):")
exts = df['URL'].str.lower().str.extract(r'\.([a-zA-Z0-9]+)(?:\?|$)')[0].value_counts()
for ext, cnt in exts.head(10).items():
    print(f"  .{ext}: {cnt}")

print(f"\nSample Component values:")
for comp in df['Component'].value_counts().head(5).index:
    print(f"  - {comp[:60]}")

print(f"\nSample data (first 3 rows):")
for i, row in df.head(3).iterrows():
    print(f"  Row {i}:")
    print(f"    URL: {row['URL'][:70]}...")
    print(f"    Source: {row['Source'][:70]}...")
    print(f"    Domain: {row['Domain']}")
    print(f"    Component: {row['Component'][:50]}")
    print(f"    DiscoveredAt: {row['DiscoveredAt']}")
