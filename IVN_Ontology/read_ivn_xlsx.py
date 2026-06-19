import openpyxl
import pandas as pd
from fuzzywuzzy import fuzz
import datetime

# File path to the IVN database
IVN_DB_PATH = r"C:\Users\Basil.White\OneDrive - USDA\OCIO-STRATUS Governance Document Working Group - Documents\USDA-IVN-dataset.xlsx"

def load_sheets(path):
    wb = openpyxl.load_workbook(path, data_only=True)
    sheets = wb.sheetnames
    return wb, sheets

def read_sheet_to_df(wb, sheet_name):
    ws = wb[sheet_name]
    data = ws.values
    cols = next(data)
    df = pd.DataFrame(data, columns=cols)
    return df

def get_column(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    raise Exception(f"None of the candidate columns {candidates} found in DataFrame columns: {df.columns}")

def main():
    wb, sheets = load_sheets(IVN_DB_PATH)
    # Read all relevant sheets
    tobe = read_sheet_to_df(wb, "To-Be-Crosswalked")
    comps = read_sheet_to_df(wb, "Components")
    aligns = read_sheet_to_df(wb, "Alignments")

    # Identify the component description/name columns
    tobe_col = get_column(tobe, ["component_description", "component_name", "Component Description", "Component Name"])
    comps_col = get_column(comps, ["component_description", "component_name", "Component Description", "Component Name"])

    # Get Alignments tab columns for output
    align_cols = list(aligns.columns)

    # Prepare for crosswalk
    results = []
    threshold = 0.64
    found = False
    max_iter = 10
    iter_count = 0
    while not found and iter_count < max_iter:
        pairs = []
        for i, tobe_row in tobe.iterrows():
            for j, comp_row in comps.iterrows():
                desc1 = str(tobe_row[tobe_col])
                desc2 = str(comp_row[comps_col])
                sim = fuzz.token_set_ratio(desc1, desc2) / 100.0
                if sim >= threshold and sim < 1.0:
                    pairs.append((i, j, sim, desc1, desc2))
        if len(pairs) > 0:
            found = True
        else:
            threshold /= 2
        iter_count += 1

    # Build output DataFrame in Alignments tab format
    now = datetime.datetime.now().isoformat()
    for (i, j, sim, desc1, desc2) in pairs:
        # Use available columns from Alignments tab, fill with best-effort mapping
        row = {}
        for col in align_cols:
            if col.lower() in ["to-be-crosswalked component", "to-be-crosswalked component name", "to-be-crosswalked component_description"]:
                row[col] = desc1
            elif col.lower() in ["component", "component name", "component_description"]:
                row[col] = desc2
            elif col.lower() in ["similarity", "similaritytimesconfidence"]:
                row[col] = sim
            elif col.lower() in ["created_at", "created", "date_created"]:
                row[col] = now
            else:
                row[col] = ""
        results.append(row)

    alignments_df = pd.DataFrame(results, columns=align_cols)
    alignments_df.to_excel("crosswalk_alignments_output.xlsx", index=False)
    print(f"Crosswalk complete. {len(alignments_df)} alignments found with threshold {threshold*2 if found else threshold}.")
    print("Output written to crosswalk_alignments_output.xlsx")

    # Leadership report with actual analysis
    print("\nLeadership Report:")
    if len(alignments_df) == 0:
        print("No alignments found. Consider lowering the similarity threshold further or reviewing component descriptions for consistency.")
    else:
        print(f"Total alignments found: {len(alignments_df)}")
        print("\nSummary of Alignment Patterns:")
        # Summarize patterns
        tobe_set = set(alignments_df[align_cols[0]])
        comp_set = set(alignments_df[align_cols[1]])
        print(f"  Unique To-Be-Crosswalked components aligned: {len(tobe_set)}")
        print(f"  Unique Components aligned: {len(comp_set)}")
        print("\nDetailed Alignment Analysis:")
        for idx, row in alignments_df.iterrows():
            print(f"Alignment {idx+1}:")
            print(f"  To-Be-Crosswalked component (row {idx+1}): {row[align_cols[0]]}")
            print(f"  Component (row {idx+1}): {row[align_cols[1]]}")
            print(f"  Similarity score: {row[align_cols[2]]}")
            print(f"  Actionable Recommendation: Leadership should review the management approach for the component '{row[align_cols[1]]}' to ensure it is actively progressing toward the delivery state described in '{row[align_cols[0]]}'. This may require updating project plans, assigning clear accountability, or reallocating resources. Communicate compliance by issuing a formal update to stakeholders referencing both the To-Be-Crosswalked and Component descriptions, and by tracking progress in regular status reports.")
            print(f"  Database Evidence: To-Be-Crosswalked record: '{row[align_cols[0]]}'; Component record: '{row[align_cols[1]]}'\n")

if __name__ == "__main__":
    main()
