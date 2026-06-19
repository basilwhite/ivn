import pandas as pd

# Load both files
comp = pd.read_excel('Components.xlsx')
out = pd.read_excel('ivn_discovered_alignments.xlsx')

print("="*70)
print("DEEP COMPARISON: Components.xlsx vs ivn_discovered_alignments.xlsx")
print("="*70)

print(f"\n1. ROW COUNTS:")
print(f"   Components.xlsx: {len(comp)} rows")
print(f"   Output file: {len(out)} rows")
print(f"   DIFFERENCE: {len(comp) - len(out)} rows missing")

print(f"\n2. COLUMNS:")
print(f"   Components.xlsx: {list(comp.columns)}")
print(f"   Output file: {list(out.columns)}")
print(f"   Match: {list(comp.columns) == list(out.columns)}")

print(f"\n3. COMPONENT_DESCRIPTION ANALYSIS:")
print(f"   Components.xlsx - sample descriptions:")
for i, desc in enumerate(comp['component_description'].dropna().head(3)):
    print(f"   [{i}] {str(desc)[:100]}...")
print(f"\n   Output file - sample descriptions:")
for i, desc in enumerate(out['component_description'].dropna().head(3)):
    print(f"   [{i}] {str(desc)[:100]}...")

print(f"\n4. COMPONENT_NAME ANALYSIS:")
print(f"   Components.xlsx - sample names:")
for name in comp['component_name'].dropna().head(5):
    print(f"   - {name}")
print(f"\n   Output file - sample names:")
for name in out['component_name'].dropna().head(5):
    print(f"   - {name}")

print(f"\n5. SOURCE_ID ANALYSIS:")
print(f"   Components.xlsx unique source_ids: {comp['source_id'].nunique()}")
print(f"   Output file unique source_ids: {out['source_id'].nunique()}")
print(f"\n   Components.xlsx sample source_ids:")
for sid in comp['source_id'].dropna().unique()[:5]:
    print(f"   - {sid}")

print(f"\n6. FETCH_STATUS VALUES:")
print(f"   Components.xlsx: {comp['fetch_status'].value_counts().to_dict()}")
print(f"   Output file: {out['fetch_status'].value_counts().to_dict()}")

print(f"\n7. KEY INSIGHT - What's in Components.xlsx that ISN'T scraped data?")
print(f"   Components.xlsx appears to contain CURATED component definitions,")
print(f"   not just discovered document URLs.")
print(f"\n   Sample full row from Components.xlsx:")
print(comp.iloc[0].to_string())

print(f"\n8. CRITICAL DIFFERENCE:")
print(f"   Components.xlsx component_description length stats:")
print(f"   - Mean: {comp['component_description'].str.len().mean():.0f} chars")
print(f"   - Max: {comp['component_description'].str.len().max():.0f} chars")
print(f"\n   Output file component_description length stats:")
print(f"   - Mean: {out['component_description'].str.len().mean():.0f} chars")
print(f"   - Max: {out['component_description'].str.len().max():.0f} chars")
