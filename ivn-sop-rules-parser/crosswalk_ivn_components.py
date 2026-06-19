"""
Crosswalk IVN components between 'To-Be-Crosswalked' and 'Components' tabs, generate alignments, and write a leadership report.
"""
import pandas as pd

# Filepath to IVN database
IVN_DB_PATH = r"C:\Users\Basil.White\OneDrive - USDA\OCIO-STRATUS Governance Document Working Group - Documents\USDA-IVN-dataset.xlsx"

# Load sheets
sheets = pd.read_excel(IVN_DB_PATH, sheet_name=None)
to_be_crosswalked = sheets['To-Be-Crosswalked']
components = sheets['Components']
alignments_template = sheets['Alignments']

# Prepare for fuzzy matching
from difflib import SequenceMatcher

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()


# Find likely alignments (exclude self-mapping, use threshold >0.1, output all Alignments tab columns)
alignments = []
for _, to_be_row in to_be_crosswalked.iterrows():
    to_be_desc = str(to_be_row['component_description'])
    to_be_id = to_be_row.get('component_id')
    to_be_source = str(to_be_row.get('source_id')) if pd.notnull(to_be_row.get('source_id')) else ''
    for _, comp_row in components.iterrows():
        comp_desc = str(comp_row['component_description'])
        comp_id = comp_row.get('component_id')
        comp_source = str(comp_row.get('source_id')) if pd.notnull(comp_row.get('source_id')) else ''
        score = similarity(to_be_desc, comp_desc)
        # Exclude self-mapping (IDs or descriptions must differ)
        if (to_be_id == comp_id or to_be_desc == comp_desc):
            continue
        if score > 0.1:
            # Build alignment row using Alignments tab columns ONLY
            alignment = {col: None for col in alignments_template.columns}
            alignment['Enabling Source'] = to_be_source
            alignment['Enabling Component'] = to_be_id
            alignment['Enabling Component Description'] = to_be_desc
            alignment['Dependent Component'] = comp_id
            alignment['Dependent Component Description'] = comp_desc
            alignment['Dependent Source'] = comp_source
            alignment['similarity'] = score
            alignment['confidence'] = None
            alignment['alignment_rationale'] = f"Fuzzy match between To-Be and Component descriptions (score={score:.3f})"
            alignments.append(alignment)

# Create DataFrame for alignments (Alignments tab columns only)
alignments_df = pd.DataFrame(alignments, columns=alignments_template.columns)

# Save alignments table in Alignments tab format
alignments_df.to_excel('discovered_alignments.xlsx', index=False)

# Leadership report
with open('leadership_alignment_report.md', 'w', encoding='utf-8') as f:
    f.write('# Leadership Alignment Report\n\n')
    f.write('The following component alignments were discovered by crosswalking the To-Be-Crosswalked and Components tabs in the IVN database. Each alignment includes a recommendation for leadership.\n\n')
    for _, row in alignments_df.iterrows():
        f.write(f"## Alignment: To-Be '{row['Enabling Component Description']}' <-> Component '{row['Dependent Component Description']}'\n")
        f.write(f"- To-Be Component ID: {row['Enabling Component']}\n")
        f.write(f"- Component ID: {row['Dependent Component']}\n")
        f.write(f"- Similarity Score: {row['similarity']:.2f}\n")
        f.write('**Recommendation:** Leadership should adjust management of the component "{component}" to ensure compliance with the to-be state "{to_be}". Communicate this compliance by documenting progress and outcomes in regular reports and stakeholder meetings.\n\n'.format(
            component=row['Dependent Component Description'],
            to_be=row['Enabling Component Description']
        ))
    f.write('\n---\n')
    f.write('This report was generated automatically by crosswalking the IVN database.\n')
