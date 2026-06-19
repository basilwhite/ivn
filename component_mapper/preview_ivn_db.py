import pandas as pd

# Load the IVN database
ivn_db_path = r"C:\Users\Basil.White\OneDrive - USDA\OCIO-STRATUS Governance Document Working Group - Documents\USDA-IVN-dataset.xlsx"

# Read all sheet names
excel_file = pd.ExcelFile(ivn_db_path)
print("Sheet names:", excel_file.sheet_names)

# Preview the first few rows of each relevant sheet
tabs_to_preview = ['To-Be-Crosswalked', 'Components', 'Alignments']
for tab in tabs_to_preview:
    print(f"\nPreview of '{tab}' tab:")
    df = pd.read_excel(ivn_db_path, sheet_name=tab)
    print(df.head())
