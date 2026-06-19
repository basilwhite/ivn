import openpyxl
import os

def get_excel_data(file_path):
    """
    Reads an Excel file and returns the data as a list of lists.
    """
    if not os.path.exists(file_path):
        return f"Error: File not found at {file_path}"
    workbook = openpyxl.load_workbook(file_path)
    sheet = workbook.active
    data = []
    for row in sheet.iter_rows(values_only=True):
        data.append(list(row))
    return data

if __name__ == "__main__":
    print(os.getcwd())
    print(os.listdir())
    excel_path = "Americas-AI-Action-Plan-IVN-Inventory.xlsx"
    excel_data = get_excel_data(excel_path)
    if isinstance(excel_data, str):
        print(excel_data)
    else:
        for row in excel_data:
            print(row)
