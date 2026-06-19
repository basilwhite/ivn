import pandas as pd

def list_columns(file_path, sheet_name):
    """
    Lists the columns of a specific sheet in an Excel file.
    """
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    print(f"Columns in '{sheet_name}':")
    for col in df.columns:
        print(f"- {col}")

if __name__ == "__main__":
    ivn_database_file = r"C:\Users\Basil.White\OneDrive - USDA\OCIO-STRATUS Governance Document Working Group - Documents\USDA-IVN-dataset.xlsx"
    list_columns(ivn_database_file, 'Components')
