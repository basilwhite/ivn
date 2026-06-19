def make_recommendation(to_be_desc, comp_desc):
    return f"Leaders should update the component '{comp_desc}' to ensure it delivers on the requirement: '{to_be_desc}'. Communicate compliance by referencing the updated component and providing evidence of progress toward the To-Be requirement."

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import os

# Use the correct file paths
TO_BE_PATH = os.path.join(os.getcwd(), 'To-Be-Crosswalked.xlsx')
COMPONENTS_PATH = os.path.join(os.getcwd(), 'Components.xlsx')
ALIGNMENTS_PATH = os.path.join(os.getcwd(), 'alignments.json')

# Read the relevant sheets (by index if needed)
to_be_df = pd.read_excel(TO_BE_PATH, sheet_name=0)
components_df = pd.read_excel(COMPONENTS_PATH, sheet_name=0)

# Find the component_description columns
to_be_desc_col = [c for c in to_be_df.columns if 'component_description' in c.lower()][0]
components_desc_col = [c for c in components_df.columns if 'component_description' in c.lower()][0]

# Try to get IDs if present
try:
    to_be_id_col = [c for c in to_be_df.columns if 'component_id' in c.lower()][0]
except IndexError:
    to_be_id_col = None
try:
    components_id_col = [c for c in components_df.columns if 'component_id' in c.lower()][0]
except IndexError:
    components_id_col = None

# Vectorize all descriptions
vectorizer = TfidfVectorizer().fit(
    to_be_df[to_be_desc_col].astype(str).tolist() + components_df[components_desc_col].astype(str).tolist()
)
to_be_vecs = vectorizer.transform(to_be_df[to_be_desc_col].astype(str))
components_vecs = vectorizer.transform(components_df[components_desc_col].astype(str))

# Compute similarity
similarity_matrix = cosine_similarity(to_be_vecs, components_vecs)

# Exclude self-mapping pairs (similarity == 1.0)
mask = similarity_matrix < 1.0

# Start with threshold 0.64, halve if needed
threshold = 0.64
pairs = []
while True:
    pairs = np.argwhere((similarity_matrix >= threshold) & mask)
    if len(pairs) > 0 or threshold < 1e-6:
        break
    threshold /= 2

# Get top 40 alignments by similarity
pair_scores = [(i, j, similarity_matrix[i, j]) for i, j in pairs]
pair_scores.sort(key=lambda x: x[2], reverse=True)
top_pairs = pair_scores[:40]

# Prepare output DataFrame with columns from Alignments tab
try:
    alignments_df = pd.read_json(ALIGNMENTS_PATH)
    alignments_cols = alignments_df.columns.tolist()
except Exception:
    alignments_cols = [
        'To-Be_component_id', 'To-Be_component_description',
        'Component_id', 'Component_description', 'similarity']

rows = []
for i, j, score in top_pairs:
    to_be_row = to_be_df.iloc[i]
    comp_row = components_df.iloc[j]
    row = {
        alignments_cols[0]: to_be_row[to_be_id_col] if to_be_id_col else i,
        alignments_cols[1]: to_be_row[to_be_desc_col],
        alignments_cols[2]: comp_row[components_id_col] if components_id_col else j,
        alignments_cols[3]: comp_row[components_desc_col],
        alignments_cols[4]: score
    }
    rows.append(row)

output_df = pd.DataFrame(rows, columns=alignments_cols)
output_df.to_csv('top_40_alignments.csv', index=False)
output_df.to_excel('top_40_alignments.xlsx', index=False)
output_df.to_json('top_40_alignments.json', orient='records', indent=2)

# Leadership report
with open('leadership_alignment_report.md', 'w', encoding='utf-8') as f:
    f.write('# Executive Summary: IVN Component Crosswalk\n\n')
    f.write(f"Similarity threshold used: {threshold:.4f}\n\n")
    f.write('## Top 40 Alignments\n\n')
    for idx, row in output_df.iterrows():
        f.write(f"### Alignment {idx+1}\n")
        f.write(f"- **To-Be Component:** {row[alignments_cols[1]]}\n")
        f.write(f"- **Component:** {row[alignments_cols[3]]}\n")
        f.write(f"- **Similarity Score:** {row[alignments_cols[4]]:.3f}\n")
        f.write(f"- **Recommendation:** Leaders should update the component '{row[alignments_cols[3]]}' to ensure it delivers on the requirement: '{row[alignments_cols[1]]}'. Communicate compliance by referencing the updated component and providing evidence of progress toward the To-Be requirement.\n\n")
    f.write('---\n')
    f.write('This report provides actionable recommendations for leadership to align IVN components with future-state requirements. Each alignment is based on semantic similarity and is specific to the exact component descriptions in the source data.\n')
