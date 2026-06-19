import pandas as pd

# The user provided the path with quotes, so I will remove them.
file_path = r"C:\Users\Basil.White\OneDrive - USDA\OCIO-STRATUS Governance Document Working Group - Documents\USDA-IVN-dataset.xlsx"

try:
    # Read the Excel file
    df = pd.read_excel(file_path)

    # Get the first 5 rows of the dataframe
    head = df.head()

    # Get the column names
    columns = df.columns.tolist()

    # Get basic info about the dataframe
    info = df.info()

    # Save the initial analysis to a file
    with open("ivn_database_analysis.txt", "w") as f:
        f.write("Initial Analysis of the IVN Database\n")
        f.write("=====================================\n\n")
        f.write("File Path: {}\n\n".format(file_path))
        f.write("First 5 Rows:\n")
        f.write(head.to_string())
        f.write("\n\n")
        f.write("Columns:\n")
        f.write(str(columns))
        f.write("\n\n")
        f.write("Info:\n")
        f.write(str(df.info()))


    print("Initial analysis of the IVN database has been saved to ivn_database_analysis.txt")

except FileNotFoundError:
    print(f"Error: The file was not found at the specified path: {file_path}")
except Exception as e:
    print(f"An error occurred: {e}")
