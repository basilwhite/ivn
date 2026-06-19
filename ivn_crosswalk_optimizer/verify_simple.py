"""Simple verification - writes results to a text file."""
import pandas as pd

output_file = r'c:\Users\basil.white\USDA\MRP P PM Knowledge Network - IVN_Document_Library\Python\scripts\ivn\ivn_crosswalk_optimizer\verification_results.txt'

# Load the new output file
df = pd.read_csv(r'c:\Users\basil.white\USDA\MRP P PM Knowledge Network - IVN_Document_Library\Python\scripts\ivn\ivn_crosswalk_optimizer\output\crosswalk_v2_20251217_074100.csv')

lines = []
lines.append("=" * 60)
lines.append("OUTPUT VERIFICATION REPORT")
lines.append("=" * 60)

# 1. Check total rows
lines.append(f"\n1. TOTAL ROWS: {len(df)}")

# 2. Check columns (should be 19)
lines.append(f"\n2. COLUMN COUNT: {len(df.columns)}")
for i, col in enumerate(df.columns, 1):
    lines.append(f"   {i:2d}. {col}")

# 3. Check for self-matches
self_matches = df[df['Enabling Component'].str.lower() == df['Dependent Component'].str.lower()]
lines.append(f"\n3. SELF-MATCHES: {len(self_matches)}")

# 4. Check for exact same-source matches
same_source_exact = df[df['Enabling Source'].str.lower() == df['Dependent Source'].str.lower()]
lines.append(f"\n4. SAME-SOURCE (EXACT): {len(same_source_exact)}")

# 5. Check for related-source matches
def is_related_source(s1, s2):
    if pd.isna(s1) or pd.isna(s2):
        return False
    suffixes = ['instructions', 'directives', 'regulations', 'policies', 'guidance']
    s1_base = str(s1).lower()
    s2_base = str(s2).lower()
    for s in suffixes:
        s1_base = s1_base.replace(s, '').strip()
        s2_base = s2_base.replace(s, '').strip()
    return s1_base == s2_base

related_source = df[df.apply(lambda r: is_related_source(r['Enabling Source'], r['Dependent Source']), axis=1)]
lines.append(f"\n5. SAME-SOURCE (RELATED): {len(related_source)}")
if len(related_source) > 0:
    lines.append("   EXAMPLES:")
    for idx, row in related_source.head(5).iterrows():
        lines.append(f"   - {row['Enabling Source']} vs {row['Dependent Source']}")

# 6. Confidence bucket distribution
lines.append("\n6. CONFIDENCE BUCKET DISTRIBUTION:")
bucket_counts = df['Confidence_Bucket'].value_counts()
for bucket in ['High', 'Medium', 'Low']:
    count = bucket_counts.get(bucket, 0)
    pct = count / len(df) * 100 if len(df) > 0 else 0
    lines.append(f"   {bucket}: {count} ({pct:.1f}%)")

# 7. Score statistics
lines.append("\n7. SIMILARITY SCORE STATISTICS:")
lines.append(f"   Min: {df['Similarity_Score'].min():.4f}")
lines.append(f"   Max: {df['Similarity_Score'].max():.4f}")
lines.append(f"   Mean: {df['Similarity_Score'].mean():.4f}")
lines.append(f"   Median: {df['Similarity_Score'].median():.4f}")

# 8. Match direction distribution
lines.append("\n8. MATCH DIRECTION DISTRIBUTION:")
direction_counts = df['Match_Direction'].value_counts()
for direction, count in direction_counts.items():
    lines.append(f"   {direction}: {count}")

# 9. Compare with previous output
lines.append("\n9. COMPARISON WITH PREVIOUS OUTPUT:")
old_df = pd.read_csv(r'c:\Users\basil.white\USDA\MRP P PM Knowledge Network - IVN_Document_Library\Python\scripts\ivn\ivn_crosswalk_optimizer\output\crosswalk_candidates_20251216_113323.csv')
old_self_matches = len(old_df[old_df['Enabling Component'].str.lower() == old_df['Dependent Component'].str.lower()])
lines.append(f"   Previous output: {len(old_df)} rows, {old_self_matches} self-matches")
lines.append(f"   New output: {len(df)} rows, {len(self_matches)} self-matches")
lines.append(f"   Rows removed: {len(old_df) - len(df)}")

lines.append("\n" + "=" * 60)
lines.append("VERIFICATION COMPLETE")
lines.append("=" * 60)

# Summary
issues = []
if len(self_matches) > 0:
    issues.append(f"Self-matches: {len(self_matches)}")
if len(same_source_exact) > 0:
    issues.append(f"Same-source exact: {len(same_source_exact)}")
if len(related_source) > 0:
    issues.append(f"Related-source: {len(related_source)}")
if len(df.columns) != 19:
    issues.append(f"Column count: {len(df.columns)} (expected 19)")

if issues:
    lines.append("\nISSUES FOUND:")
    for issue in issues:
        lines.append(f"  - {issue}")
else:
    lines.append("\nALL CHECKS PASSED!")

# Write to file
with open(output_file, 'w') as f:
    f.write('\n'.join(lines))

print(f"Results written to: {output_file}")
