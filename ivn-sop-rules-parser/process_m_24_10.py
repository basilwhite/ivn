from pdf_parser import parse_pdf, get_excel_data
import json

def main():
    pdf_path = "M-24-10.pdf"
    text = parse_pdf(pdf_path)
    
    if text.startswith("Error:") or text.startswith("An error occurred:"):
        print(text)
        return

    # For now, just save the extracted text to a file to inspect.
    with open("extracted_text.txt", "w", encoding="utf-8") as f:
        f.write(text)
    
    print("Successfully extracted text from PDF and saved to extracted_text.txt")

    ivn_db_path = "C:\\Users\\Basil.White\\OneDrive - USDA\\OCIO-STRATUS Governance Document Working Group - Documents\\USDA-IVN-dataset.xlsx"
    ivn_data = get_excel_data(ivn_db_path)

    if isinstance(ivn_data, str) and (ivn_data.startswith("Error:") or ivn_data.startswith("An error occurred:")):
        print(ivn_data)
        return

    # Print the header and the first 5 rows to understand the structure
    if ivn_data:
        print("IVN Database Header:")
        print(ivn_data[0])
        print("\nFirst 5 rows of IVN Database:")
        for row in ivn_data[1:6]:
            print(row)

if __name__ == "__main__":
    main()
