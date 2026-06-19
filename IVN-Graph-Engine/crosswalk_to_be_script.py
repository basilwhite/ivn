import openpyxl
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Filepaths
xlsx_path = r'C:/Users/Basil.White/OneDrive - USDA/OCIO-STRATUS Governance Document Working Group - Documents/USDA-IVN-dataset.xlsx'
alignments_out = r'alignments_to_be_crosswalked.csv'

# Load workbook and sheets
def load_sheet(sheet_name):
    wb = openpyxl.load_workbook(xlsx_path, read_only=True)
    ws = wb[sheet_name]
    data = list(ws.values)
    header = data[0]
    df = pd.DataFrame(data[1:], columns=header)
    return df

to_be_df = load_sheet('To-Be-Crosswalked')
comps_df = load_sheet('Components')

# Prepare descriptions
crosswalk_results = []
to_be_desc = to_be_df['component_description'].fillna('').tolist()
comps_desc = comps_df['component_description'].fillna('').tolist()

vectorizer = TfidfVectorizer().fit(to_be_desc + comps_desc)
to_be_vecs = vectorizer.transform(to_be_desc)
comps_vecs = vectorizer.transform(comps_desc)

threshold = 0.64
found = False
max_attempts = 10
attempt = 0
while not found and attempt < max_attempts:
    results = []
    for i, tb_vec in enumerate(to_be_vecs):
        sims = cosine_similarity(tb_vec, comps_vecs)[0]
        for j, sim in enumerate(sims):
            if sim < 0.99 and sim >= threshold:
                results.append((i, j, sim))
    if results:
        found = True
    else:
        threshold /= 2
        attempt += 1

# Output alignments in Alignments tab format
alignments = []
for i, j, sim in results:
    alignments.append({
        'M-25-22 Section': '',
        'M-25-22 Title': '',
        'M-25-22 Component': to_be_df.iloc[i]['component_description'],
        'IVN Source Name': comps_df.iloc[j]['component_name'],
        'IVN Source Agency': comps_df.iloc[j]['component_agency'],
        'Alignment Score': round(sim, 3)
    })

alignments_df = pd.DataFrame(alignments)
alignments_df.to_csv(alignments_out, index=False)
print(f"Alignments written to {alignments_out}. Total pairs: {len(alignments)}. Threshold used: {threshold}")

# Leadership report
def write_report():
    with open('leadership_report_to_be_crosswalked.md', 'w', encoding='utf-8') as f:
        f.write('# Leadership Report: To-Be-Crosswalked Alignments\n\n')
        if not alignments:
            f.write('No alignments found.\n')
            return
        for row in alignments:
            f.write(f"## Alignment\n")
            f.write(f"- To-Be-Crosswalked: {row['M-25-22 Component']}\n")
            f.write(f"- Component: {row['IVN Source Name']}\n")
            f.write(f"- Agency: {row['IVN Source Agency']}\n")
            f.write(f"- Similarity Score: {row['Alignment Score']}\n")
            f.write(f"**Recommendation:** Leadership should update management of the component '{row['IVN Source Name']}' to ensure compliance with the goal: '{row['M-25-22 Component']}'. Communicate this compliance clearly to all stakeholders, referencing this alignment.\n\n")
write_report()
print("Leadership report written to leadership_report_to_be_crosswalked.md")
