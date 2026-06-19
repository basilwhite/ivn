# similarity_scores.py
#
# PROMPT FOR LLM (for future maintainers and reviewers):
# ---------------------------------------------------------------------------------
# Create a Python script that performs semantic similarity matching between two sets of components from an Excel file
# to help identify and document plausible relationships between them.
#
# Instructions:
# 1. Load Data:
#    - Load three sheets from ivntest.xlsx:
#      * Components (reference set)
#      * ToBeCrosswalked (components to align)
#      * Dataset (for URL lookup; note: sheet name is 'Dataset')
# 2. Extract Components:
#    - From Components: extract tuples of (component_name, component_description, source)
#    - From ToBeCrosswalked: extract tuples of (Component, Component Description, Source, Component URL)
# 3. Build URL Lookup:
#    - Build a dictionary mapping component names to their URLs using the Dataset sheet.
# 4. Batch Similarity Calculation:
#    - Use TfidfVectorizer to vectorize all unaligned and reference component descriptions in batch (fit once).
#    - Compute the cosine similarity matrix between all unaligned and reference descriptions at once.
#    - Only keep pairs where the similarity score is greater than or equal to a user-specified threshold.
# 5. Efficient Output Construction:
#    - For each pair above the threshold, build a result dictionary with the following columns in this exact order:
#      1. Unaligned Component
#      2. Unaligned Source
#      3. Unaligned Component Description
#      4. Unaligned Component URL
#      5. Reference Component Source
#      6. Reference Component
#      7. Reference Component Description
#      8. Reference Component URL
#      9. Justification (e.g., "'A' and 'B' have a semantic similarity score of 0.8123.")
#      10. Similarity Score
#    - Exclude pairs where the sources for both components are the same.
# 6. Progress Bar:
#    - Show a progress bar in the terminal as it processes all pairs.
# 7. Output:
#    - Collect all results into a pandas DataFrame.
#    - Save the results as a timestamped CSV file in the script directory, using UTF-8-SIG encoding.
#
# Additional Guidance:
# - Do not use nested Python loops for similarity calculation; use matrix operations.
# - Only use loops for filtering and building the output list after the similarity matrix is computed.
# - Handle missing or empty fields gracefully.
# - Ensure the script works even if there are zero unaligned or reference components.
# - Prompt the user for a similarity threshold, defaulting to 0.4 if not provided or invalid.
# - Ensure all output columns are correctly aligned with the original data.
# - Name the output file as ivn_inferred_causal_output_<timestamp>.csv.
# - Do not fit the vectorizer inside a loop.
# - Do not append to a DataFrame in a loop; build a list of dicts and create the DataFrame once.
# - Use efficient NumPy or pandas operations for thresholding.
# - Validate all column names against the actual Excel sheets.
# - Add comments explaining each major step.
# - Handle exceptions for user input and file operations.
# - Always include the unaligned component source in the output file.
# - If you get a ValueError about a missing worksheet, check the Excel file for the correct sheet names and update the script accordingly.
# ---------------------------------------------------------------------------------

import os
import sys
import time
import pandas as pd
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# --- Utility functions ---

def clean_field(val):
    """Remove tabs and newlines from each field and ensure string type."""
    if pd.isna(val):
        return ""
    return str(val).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')

def clean_dataframe(df):
    """Clean all string fields in a DataFrame."""
    for col in df.columns:
        df[col] = df[col].apply(clean_field)
    return df

def validate_columns(df, required_columns, sheet_name):
    """Ensure all required columns exist in the DataFrame."""
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in '{sheet_name}': {missing}")


def format_eta(seconds):
    """Convert seconds to HH:MM:SS string for progress display."""
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

# --- Paths and loading ---

script_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(script_dir, 'ivntest.xlsx')

# --- Load and clean data ---

df_components = pd.read_excel(input_path, sheet_name='Components')
df_unaligned = pd.read_excel(input_path, sheet_name='ToBeCrosswalked')
df_internal = pd.read_excel(input_path, sheet_name='Dataset')  # <-- updated sheet name

df_components = clean_dataframe(df_components)
df_unaligned = clean_dataframe(df_unaligned)
df_internal = clean_dataframe(df_internal)

# --- Validate columns ---

required_unaligned = ["Component", "Component Description", "Source", "Component URL"]
required_components = ["component_name", "component_description", "source"]
required_internal = ["Enabling Component", "Enabling Component URL", "Dependent Component", "Dependent Component URL"]

validate_columns(df_unaligned, required_unaligned, "ToBeCrosswalked")
validate_columns(df_components, required_components, "Components")
validate_columns(df_internal, required_internal, "Dataset")  # <-- updated sheet name

# --- Extraction functions ---

def extract_unaligned_components(df):
    """Extract tuples from ToBeCrosswalked sheet using strict column names."""
    return [
        (
            row["Component"],
            row["Component Description"],
            row["Source"],
            row["Component URL"]
        )
        for _, row in df.iterrows()
        if pd.notna(row["Component"]) and pd.notna(row["Component Description"])
        and str(row["Component"]).strip() and str(row["Component Description"]).strip()
    ]

def extract_components(df):
    """Extract tuples from Components sheet using strict column names."""
    return [
        (
            row["component_name"],
            row["component_description"],
            row["source"]
        )
        for _, row in df.iterrows()
        if pd.notna(row["component_name"]) and pd.notna(row["component_description"])
        and str(row["component_name"]).strip() and str(row["component_description"]).strip()
    ]

def build_component_url_lookup(df_internal):
    """Build a lookup dictionary for component URLs from Dataset."""
    url_lookup = {}
    for _, row in df_internal.iterrows():
        en_name = row["Enabling Component"]
        en_url = row["Enabling Component URL"]
        dep_name = row["Dependent Component"]
        dep_url = row["Dependent Component URL"]
        if pd.notna(en_name) and str(en_name).strip():
            url_lookup[str(en_name).strip()] = en_url if pd.notna(en_url) else ""
        if pd.notna(dep_name) and str(dep_name).strip():
            url_lookup[str(dep_name).strip()] = dep_url if pd.notna(dep_url) else ""
    return url_lookup

components = extract_components(df_components)
unaligned_components = extract_unaligned_components(df_unaligned)
component_url_lookup = build_component_url_lookup(df_internal)

print(f"Components count: {len(components)}")
print(f"Unaligned Components count: {len(unaligned_components)}")

def get_similarity_threshold(default=0.4):
    """Prompt user for similarity threshold, fallback to default if invalid."""
    try:
        user_input = input(f"Enter similarity threshold (default {default}): ")
        threshold = float(user_input) if user_input.strip() else default
        print(f"Using similarity threshold: {threshold}")
        return threshold
    except Exception:
        print(f"Invalid input. Using default threshold: {default}")
        return default

if __name__ == "__main__":
    sim_threshold = get_similarity_threshold()
    print("Comparing Unaligned Components to Components...")

    if not unaligned_components or not components:
        print("No components to compare. Exiting.")
        sys.exit(0)

    # Unpack fields
    unaligned_names, unaligned_descs, unaligned_sources, unaligned_urls = zip(*unaligned_components)
    component_names, component_descs, component_sources = zip(*components)

    # Fit vectorizer once
    vectorizer = TfidfVectorizer().fit(list(unaligned_descs) + list(component_descs))
    unaligned_vecs = vectorizer.transform(unaligned_descs)
    component_vecs = vectorizer.transform(component_descs)

    # Compute all pairwise similarities at once
    sim_matrix = cosine_similarity(unaligned_vecs, component_vecs)

    # Find all pairs above threshold
    rows, cols = np.where(sim_matrix >= sim_threshold)
    total = len(rows)
    results = []
    start_time = time.time()

    output_columns = [
        "Unaligned Component",
        "Unaligned Source",  # <-- changed column name
        "Unaligned Component Description",
        "Unaligned Component URL",
        "Reference Component Source",
        "Reference Component",
        "Reference Component Description",
        "Reference Component URL",
        "Justification",
        "Similarity Score"
    ]

    for idx, (i, j) in enumerate(zip(rows, cols), 1):
        sim = sim_matrix[i, j]
        # Skip pairs where the sources are the same
        if str(unaligned_sources[i]).strip() == str(component_sources[j]).strip():
            continue
        result = {
            "Unaligned Component": str(unaligned_names[i]),
            "Unaligned Source": str(unaligned_sources[i]),  # <-- changed key
            "Unaligned Component Description": str(unaligned_descs[i]),
            "Unaligned Component URL": str(unaligned_urls[i]),
            "Reference Component Source": str(component_sources[j]),
            "Reference Component": str(component_names[j]),
            "Reference Component Description": str(component_descs[j]),
            "Reference Component URL": str(component_url_lookup.get(str(component_names[j]).strip(), "")),
            "Justification": f"'{unaligned_names[i]}' and '{component_names[j]}' have a semantic similarity score of {sim:.4f}.",
            "Similarity Score": sim
        }
        # Clean all fields in the result
        for key in result:
            result[key] = clean_field(result[key])
        results.append(result)

        # Progress bar
        if idx % 1000 == 0 or idx == total:
            elapsed = time.time() - start_time
            rate = idx / elapsed if elapsed > 0 else 0
            left = total - idx
            eta_seconds = left / rate if rate > 0 else 0
            status = f"Processed: {idx}/{total} | Left: {left} | ETA: {format_eta(eta_seconds)}"
            sys.stdout.write('\r' + ' ' * 80 + '\r')
            sys.stdout.write(status)
            sys.stdout.flush()
    print()

    if not results:
        print("No qualifying pairs found at the chosen threshold. No output file written.")
        sys.exit(0)

    # Build output DataFrame with strict column order
    output_df = pd.DataFrame(results, columns=output_columns)

    # Preview first few rows for verification
    print("Preview of output:")
    print(output_df.head())

    # Save output
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(script_dir, f'ivn_inferred_causal_output_{timestamp}.csv')
    print("Saving output file...")
    output_df.to_csv(output_path, index=False, encoding='utf-8-sig', sep=',')
    print(f"Output saved to: {output_path}")