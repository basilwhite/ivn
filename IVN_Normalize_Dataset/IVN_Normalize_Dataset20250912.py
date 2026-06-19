# Prompt for novice LLM:
# The script should:
# - Read the input Excel file and use the column names from the first sheet in the input Excel file, regardless of the name of the sheet.
# - Look for the following columns: Enabling Source, Enabling Component, Enabling Component Description, Dependent Component, Dependent Component Description, Dependent Source.
# - Extract components from columns: Enabling Component/Enabling Component Description and Dependent Component/Dependent Component Description.
# - Create a Components table with a source_id, component_name, component_description, and a unique component_id for each component based on its source, component and component description.
# - Add component IDs to the alignment rows.
# - Create an Alignments table with enabling_component/component_id and dependent_component/component_id.
# - Create a Sources table with both source_id and source_name.
# - Save the results to a new Excel file with a timestamp suffix in the format YYYYMMDDHHMM in the same location as the script.
# - Print the output filename when done.

import pandas as pd
import hashlib
from pathlib import Path
import datetime

# === Step 1: Configuration ===
INPUT_FILENAME = "ivntest.xlsx"

# === Step 2: Load Excel File (first sheet only) ===
input_path = Path(__file__).parent / INPUT_FILENAME
df = pd.read_excel(input_path, sheet_name=0)
df.columns = df.columns.str.strip().str.lower()  # Normalize column names and strip spaces
print("Columns in input file:", list(df.columns))

# Map expected columns to normalized names
col_map = {
    "enabling source": "enabling_source",
    "enabling component": "enabling_component",
    "enabling component description": "enabling_component_description",
    "dependent component": "dependent_component",
    "dependent component description": "dependent_component_description",
    "dependent source": "dependent_source"
}

# Ensure columns exist
for col in col_map:
    if col_map[col] not in df.columns:
        raise KeyError(f"Column '{col}' not found in input file.")

# === Step 3: Generate Unique Component IDs ===
def generate_component_id(source, name, description):
    key = f"{source}|{name}|{description}"
    return "C" + hashlib.md5(key.strip().lower().encode()).hexdigest()[:8]

# === Step 4: Extract Components ===
enabling = df[[col_map["enabling_source"], col_map["enabling_component"], col_map["enabling_component_description"]]].copy()
enabling = enabling.rename(columns={
    col_map["enabling_source"]: "source",
    col_map["enabling_component"]: "component_name",
    col_map["enabling_component_description"]: "component_description"
})
enabling["component_id"] = enabling.apply(
    lambda row: generate_component_id(row["source"], row["component_name"], row["component_description"]), axis=1
)

dependent = df[[col_map["dependent_source"], col_map["dependent_component"], col_map["dependent_component_description"]]].copy()
dependent = dependent.rename(columns={
    col_map["dependent_source"]: "source",
    col_map["dependent_component"]: "component_name",
    col_map["dependent_component_description"]: "component_description"
})
dependent["component_id"] = dependent.apply(
    lambda row: generate_component_id(row["source"], row["component_name"], row["component_description"]), axis=1
)

components = pd.concat([enabling, dependent], ignore_index=True)
components = components.drop_duplicates(subset=["component_id"]).reset_index(drop=True)

# === Step 5: Create Sources Table ===
sources_1 = df[[col_map["enabling_source"]]].rename(columns={col_map["enabling_source"]: "source_name"})
sources_2 = df[[col_map["dependent_source"]]].rename(columns={col_map["dependent_source"]: "source_name"})
all_sources = pd.concat([sources_1, sources_2], ignore_index=True)
all_sources = all_sources.drop_duplicates().dropna().reset_index(drop=True)
all_sources["source_id"] = ["S" + str(i).zfill(3) for i in range(1, len(all_sources) + 1)]

# Merge source_id into Components table
components = components.merge(all_sources, left_on="source", right_on="source_name", how="left")
components = components[["source_id", "component_name", "component_description", "component_id"]]

# === Step 6: Add Component IDs to Alignment Rows ===
df["enabling_component_id"] = enabling["component_id"]
df["dependent_component_id"] = dependent["component_id"]

# Create Alignments table
alignments = df[[col_map["enabling_component"], "enabling_component_id", col_map["dependent_component"], "dependent_component_id"]].copy()
alignments = alignments.rename(columns={
    col_map["enabling_component"]: "enabling_component",
    col_map["dependent_component"]: "dependent_component"
})

# === Step 7: Save to New Excel File ===
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")
output_filename_with_ts = f"IVN_Normalized_{timestamp}.xlsx"
output_path = Path(__file__).parent / output_filename_with_ts
with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
    components.to_excel(writer, index=False, sheet_name="Components")
    alignments.to_excel(writer, index=False, sheet_name="Alignments")
    all_sources.to_excel(writer, index=False, sheet_name="Sources")

print(f"✅ Saved normalized dataset to: {output_filename_with_ts}")