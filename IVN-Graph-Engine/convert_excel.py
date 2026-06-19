import sys
import subprocess

def install_package(package):
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    except subprocess.CalledProcessError as e:
        print(f"Failed to install {package}: {e}")
        sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("Pandas not found. Installing...")
    install_package("pandas")
    import pandas as pd

try:
    import openpyxl
except ImportError:
    print("openpyxl not found. Installing...")
    install_package("openpyxl")
    import openpyxl

def convert_excel_to_csv(excel_path, csv_path):
    """
    Converts an Excel file to a CSV file.

    Args:
        excel_path (str): The file path of the Excel file.
        csv_path (str): The file path to save the CSV file.
    """
    try:
        df = pd.read_excel(excel_path)
        df.to_csv(csv_path, index=False)
        print(f"Successfully converted {excel_path} to {csv_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    excel_file = "C:\\Users\\Basil.White\\OneDrive - USDA\\OCIO-STRATUS Governance Document Working Group - Documents\\USDA-IVN-dataset.xlsx"
    csv_file = "c:\\Users\\Basil.White\\OneDrive - USDA\\OCIO-STRATUS Governance Document Working Group - Documents\\Python\\scripts\\ivn\\IVN-Graph-Engine\\USDA-IVN-dataset.csv"
    convert_excel_to_csv(excel_file, csv_file)
