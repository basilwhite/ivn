import pandas as pd

# Load the IVN database
ivn_db_path = r"C:\Users\Basil.White\OneDrive - USDA\OCIO-STRATUS Governance Document Working Group - Documents\USDA-IVN-dataset.xlsx"

# Read the Alignments tab
alignments = pd.read_excel(ivn_db_path, sheet_name='Alignments')

# Show the first 10 rows and all columns
with open('alignments_tab_sample.txt', 'w', encoding='utf-8') as f:
    f.write(alignments.head(10).to_string())
print('Sample from Alignments tab written to alignments_tab_sample.txt')
