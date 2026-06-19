import pandas as pd

# Load the IVN database
ivn_db_path = r"C:\Users\Basil.White\OneDrive - USDA\OCIO-STRATUS Governance Document Working Group - Documents\USDA-IVN-dataset.xlsx"

# Read relevant sheets
to_be_crosswalked = pd.read_excel(ivn_db_path, sheet_name='To-Be-Crosswalked')
components = pd.read_excel(ivn_db_path, sheet_name='Components')
alignments_template = pd.read_excel(ivn_db_path, sheet_name='Alignments', nrows=0)

# Use fuzzy matching for component_description alignment
from difflib import SequenceMatcher

def get_best_matches(source_df, target_df, source_col, target_col, threshold=0.6):
    matches = []
    for i, source_row in source_df.iterrows():
        source_desc = str(source_row[source_col])
        best_score = 0
        best_match = None
        for j, target_row in target_df.iterrows():
            target_desc = str(target_row[target_col])
            score = SequenceMatcher(None, source_desc, target_desc).ratio()
            if score > best_score:
                best_score = score
                best_match = (i, j, score, source_desc, target_desc)
        if best_score >= threshold:
            matches.append(best_match)
    return matches

# Find likely alignments
matches = get_best_matches(to_be_crosswalked, components, 'component_description', 'component_description', threshold=0.6)

# Prepare alignments table using Alignments tab columns
alignments_columns = alignments_template.columns.tolist()
alignments = []
for i, j, score, tobe_desc, comp_desc in matches:
    row = {col: None for col in alignments_columns}
    row['To-Be-Crosswalked component_description'] = tobe_desc
    row['Component component_description'] = comp_desc
    row['SimilarityTimesConfidence'] = score
    alignments.append(row)
alignments_df = pd.DataFrame(alignments, columns=alignments_columns)

# Save alignments table for user review
alignments_df.to_excel('alignments_crosswalk_output.xlsx', index=False)

# Write a leadership report
with open('leadership_report.txt', 'w', encoding='utf-8') as f:
    f.write('IVN Component Crosswalk Leadership Report\n')
    f.write('='*50 + '\n\n')
    for idx, (i, j, score, tobe_desc, comp_desc) in enumerate(matches):
        f.write(f"Alignment {idx+1}:\n")
        f.write(f"- To-Be-Crosswalked component_description: {tobe_desc}\n")
        f.write(f"- Component component_description: {comp_desc}\n")
        f.write(f"- Similarity Score: {score:.2f}\n")
        f.write("Recommendation: Leadership should review and update management of the Component description above to ensure it delivers compliance with the To-Be-Crosswalked description. Communicate compliance by documenting changes and progress in the Alignments tab.\n\n")
print('Alignments and leadership report generated.')
