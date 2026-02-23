# Spec source: see paired prompt file `ivn_components_error_checker_prompt.txt`. Keep this script and the prompt file synchronized.

# ivn_components_error_checker.py automates quality checks for governance datasets, helping users quickly identify and review inconsistencies or potential errors in component descriptions, URLs, and source pairings.

"""
ivn_components_error_checker.py
Description: Identifies potential errors in federal governance deliverables datasets by:
1. Flagging components with multiple non-null descriptions
2. Flagging different components with identical descriptions
3. Flagging identical components with multiple URLs
4. Flagging enabling-dependent component pairs from the same source document
Supports loading the dataset from a local file or a Google Sheets URL.
"""

import pandas as pd
import numpy as np
import re
from tqdm import tqdm
from difflib import SequenceMatcher
import requests
from io import BytesIO
import time
import os
from datetime import datetime

try:
    # rapidfuzz is far faster than difflib for fuzzy ratios; falls back if unavailable
    from rapidfuzz import fuzz

    def similarity_ratio(a, b):
        a = normalize_text(a)
        b = normalize_text(b)
        if not a and not b:
            return 100
        return fuzz.ratio(a, b)
except ImportError:
    def similarity_ratio(a, b):
        """Pure Python implementation of similarity ratio"""
        a = normalize_text(a)
        b = normalize_text(b)
        if not a and not b:
            return 100
        return SequenceMatcher(None, a, b).ratio() * 100

def normalize_text(text):
    """Normalize text for fuzzy matching without external dependencies"""
    if pd.isna(text):
        return ""
    text = str(text).lower()
    text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
    text = re.sub(r'\s+', ' ', text).strip()  # Remove extra spaces
    return text

def get_component_groups(df, source_col, component_col, threshold=95):
    """Create similarity-matched groups for (source, component) pairs with blocking to avoid O(n^2)."""
    groups = {}  # group_id -> representative (source_norm, component_norm)
    buckets = {}  # blocking key -> set of group_ids
    group_map = {}
    group_counter = 0

    # Create normalized versions once
    df['source_norm'] = df[source_col].apply(normalize_text)
    df['component_norm'] = df[component_col].apply(normalize_text)

    def blocking_keys(source_norm, component_norm):
        """Generate lightweight keys so we only compare plausible matches."""
        comp_head = component_norm[:12]
        src_head = source_norm[:12]
        comp_token = component_norm.split(' ', 1)[0]
        src_token = source_norm.split(' ', 1)[0]
        return {
            (comp_head, src_head),
            (comp_token, src_token),
            (component_norm[:5], source_norm[:5]),
        }

    for row in tqdm(df.itertuples(index=True), total=len(df), desc="Grouping components", mininterval=0.5):
        idx = row.Index
        source = row.source_norm
        component = row.component_norm
        if not component:
            continue

        candidate_group_ids = set()
        for key in blocking_keys(source, component):
            candidate_group_ids.update(buckets.get(key, set()))

        matched_group = None
        for group_id in candidate_group_ids:
            group_source, group_component = groups[group_id]
            if similarity_ratio(source, group_source) >= threshold and similarity_ratio(component, group_component) >= threshold:
                matched_group = group_id
                break

        if matched_group is None:
            group_counter += 1
            matched_group = group_counter
            groups[matched_group] = (source, component)
            for key in blocking_keys(source, component):
                buckets.setdefault(key, set()).add(matched_group)

        group_map[idx] = matched_group

    return group_map

def prompt_user_choice(options, prompt_message):
    """Prompt user to choose one option from a list."""
    print(prompt_message)
    for i, option in enumerate(options, 1):
        print(f"{i}: {option}")
    while True:
        try:
            choice = int(input("Enter the number of your choice: "))
            if 1 <= choice <= len(options):
                return options[choice - 1]
        except ValueError:
            pass
        print("Invalid choice. Please try again.")

def file_navigator(start_path):
    """Simple file navigator for selecting a local file."""
    current_path = start_path
    while True:
        items = os.listdir(current_path)
        files = [f for f in items if os.path.isfile(os.path.join(current_path, f))]
        dirs = [d for d in items if os.path.isdir(os.path.join(current_path, d))]
        print(f"\nCurrent directory: {current_path}")
        print("Folders:")
        for i, d in enumerate(dirs, 1):
            print(f"  {i}. [Folder] {d}")
        print("Files:")
        for j, f in enumerate(files, 1):
            print(f"  {j + len(dirs)}. {f}")
        print("  0. Go up one level")
        print("Select a file by number, or folder to enter, or 0 to go up:")
        try:
            choice = int(input("Your choice: "))
            if choice == 0:
                parent = os.path.dirname(current_path)
                if parent == current_path:
                    print("Already at top level.")
                else:
                    current_path = parent
            elif 1 <= choice <= len(dirs):
                current_path = os.path.join(current_path, dirs[choice - 1])
            elif len(dirs) < choice <= len(dirs) + len(files):
                file_choice = files[choice - len(dirs) - 1]
                return os.path.join(current_path, file_choice)
            else:
                print("Invalid choice. Try again.")
        except ValueError:
            print("Invalid input. Try again.")

def resolve_duplicates(df, col, group_col, threshold=95, prompt_message="Choose the correct value:"):
    """Find similar values and prompt user to resolve duplicates with blocking to reduce comparisons."""
    df['norm'] = df[col].apply(normalize_text)

    # First handle exact-normalized duplicates quickly
    exact_groups = df.groupby('norm').indices
    to_process = []
    for norm_val, indices in exact_groups.items():
        original_values = df.loc[indices, col].dropna().unique()
        if len(original_values) > 1:
            to_process.append(indices)

    # Now build fuzzy groups only across distinct normalized strings that look similar
    norms = list(exact_groups.keys())
    buckets = {}

    def bucket_key(val):
        return (val[:10], val.split(' ', 1)[0])

    for norm_val in norms:
        buckets.setdefault(bucket_key(norm_val), []).append(norm_val)

    processed_norms = set()
    for norm_bucket in buckets.values():
        for i, left in enumerate(norm_bucket):
            if left in processed_norms:
                continue
            current_indices = list(exact_groups[left])
            for right in norm_bucket[i + 1:]:
                if similarity_ratio(left, right) >= threshold:
                    current_indices.extend(exact_groups[right])
                    processed_norms.add(right)
            if len(current_indices) > 1:
                to_process.append(current_indices)
            processed_norms.add(left)

    for indices in to_process:
        original_values = df.loc[indices, col].dropna().unique()
        if len(original_values) > 1:
            chosen = prompt_user_choice(list(original_values), f"\n{prompt_message}\nOptions for similar '{col}':")
            df.loc[indices, col] = chosen

    df.drop(columns=['norm'], inplace=True)
    return df

def main():
    start_total = time.time()
    print("FGDEC: Do you want to load the dataset from a local file or a URL?")
    print("1: Local file")
    print("2: URL")
    while True:
        choice = input("Enter 1 for local file or 2 for URL: ").strip()
        if choice == "1":
            script_dir = os.path.dirname(os.path.abspath(__file__))
            file_path = file_navigator(script_dir)
            print(f"FGDEC: Loading dataset from local file: {file_path}")
            start = time.time()
            df = pd.read_excel(file_path, sheet_name=0)
            print(f"FGDEC: Dataset loaded in {time.time() - start:.2f} seconds.")
            break
        elif choice == "2":
            sheet_url = input("Enter the Google Sheets XLSX export URL: ").strip()
            print(f"FGDEC: Loading dataset from URL: {sheet_url}")
            start = time.time()
            response = requests.get(sheet_url)
            df = pd.read_excel(BytesIO(response.content), sheet_name=0)
            print(f"FGDEC: Dataset loaded in {time.time() - start:.2f} seconds.")
            break
        else:
            print("Invalid choice. Please enter 1 or 2.")

    error_columns = [
        'ERROR: Multiple Descriptions',
        'ERROR: Same Description',
        'ERROR: Multiple URLs',
        'ERROR: Same Source Pair'
    ]
    for col in error_columns:
        df[col] = ""

    print("FGDEC: Creating unified component view...")
    start = time.time()
    enabling_df = df[['Enabling Source', 'Enabling Component', 
                      'Enabling Component Description', 'Enabling Component URL']].copy()
    enabling_df.columns = ['Source', 'Component', 'Description', 'URL']
    enabling_df['role'] = 'enabling'

    dependent_df = df[['Dependent Source', 'Dependent Component', 
                       'Dependent Component Description', 'Dependent Component URL']].copy()
    dependent_df.columns = ['Source', 'Component', 'Description', 'URL']
    dependent_df['role'] = 'dependent'

    components_df = pd.concat([enabling_df, dependent_df], ignore_index=True)
    print(f"FGDEC: Unified component view created in {time.time() - start:.2f} seconds.")

    print("FGDEC: Grouping similar components...")
    start = time.time()
    components_df['group_id'] = get_component_groups(components_df, 'Source', 'Component')
    print(f"FGDEC: Component grouping completed in {time.time() - start:.2f} seconds.")

    print("FGDEC: Resolving similar component names...")
    start = time.time()
    components_df = resolve_duplicates(
        components_df, 'Component', 'group_id', prompt_message="Choose the correct component name:"
    )
    print(f"FGDEC: Component name resolution completed in {time.time() - start:.2f} seconds.")

    print("FGDEC: Resolving similar source names...")
    start = time.time()
    components_df = resolve_duplicates(
        components_df, 'Source', 'group_id', prompt_message="Choose the correct source name:"
    )
    print(f"FGDEC: Source name resolution completed in {time.time() - start:.2f} seconds.")

    print("FGDEC: Resolving multiple descriptions...")
    start = time.time()
    components_df = resolve_duplicates(
        components_df, 'Description', 'group_id', prompt_message="Choose the correct description:"
    )
    print(f"FGDEC: Description resolution completed in {time.time() - start:.2f} seconds.")

    print("FGDEC: Checking for multiple descriptions...")
    start = time.time()
    for group_id, group_df in components_df.groupby('group_id'):
        if len(group_df) <= 1:
            continue
        unique_descs = group_df['Description'].dropna().apply(normalize_text).unique()
        if len(unique_descs) > 1:
            for _, row in group_df.iterrows():
                idx = row.name
                if idx < len(df):
                    df.at[idx, 'ERROR: Multiple Descriptions'] = "ENABLING"
                else:
                    orig_idx = idx - len(enabling_df)
                    if orig_idx < len(df):
                        df.at[orig_idx, 'ERROR: Multiple Descriptions'] = "DEPENDENT"
    print(f"FGDEC: Multiple description check completed in {time.time() - start:.2f} seconds.")

    print("FGDEC: Checking for same descriptions...")
    start = time.time()
    desc_map = {}
    for idx, row in components_df.iterrows():
        desc = row['Description']
        if pd.isna(desc) or not str(desc).strip():
            continue
        normalized_desc = normalize_text(desc)
        if normalized_desc not in desc_map:
            desc_map[normalized_desc] = set()
        desc_map[normalized_desc].add(row['group_id'])
    for desc, group_ids in desc_map.items():
        if len(group_ids) > 1:
            comps = components_df[
                components_df['Description'].apply(normalize_text) == desc
            ]
            for _, row in comps.iterrows():
                idx = row.name
                if idx < len(df):
                    df.at[idx, 'ERROR: Same Description'] = "ENABLING"
                else:
                    orig_idx = idx - len(enabling_df)
                    if orig_idx < len(df):
                        df.at[orig_idx, 'ERROR: Same Description'] = "DEPENDENT"
    print(f"FGDEC: Same description check completed in {time.time() - start:.2f} seconds.")

    print("FGDEC: Checking for multiple URLs...")
    start = time.time()
    for group_id, group_df in components_df.groupby('group_id'):
        if len(group_df) <= 1:
            continue
        unique_urls = group_df['URL'].dropna().apply(normalize_text).unique()
        if len(unique_urls) > 1:
            for _, row in group_df.iterrows():
                idx = row.name
                if idx < len(df):
                    df.at[idx, 'ERROR: Multiple URLs'] = "ENABLING"
                else:
                    orig_idx = idx - len(enabling_df)
                    if orig_idx < len(df):
                        df.at[orig_idx, 'ERROR: Multiple URLs'] = "DEPENDENT"
    print(f"FGDEC: Multiple URL check completed in {time.time() - start:.2f} seconds.")

    print("FGDEC: Checking for same source pairs...")
    start = time.time()
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Checking source pairs"):
        if pd.isna(row['Enabling Source']) or pd.isna(row['Dependent Source']):
            continue
        norm_source1 = normalize_text(row['Enabling Source'])
        norm_source2 = normalize_text(row['Dependent Source'])
        if similarity_ratio(norm_source1, norm_source2) > 95:
            df.at[idx, 'ERROR: Same Source Pair'] = "YES"
    print(f"FGDEC: Same source pair check completed in {time.time() - start:.2f} seconds.")

    print("FGDEC: Saving results...")
    start = time.time()
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    output_path = f"error_flagged_dataset_{timestamp}.xlsx"

    readme_rows = [
        {"Section": "Sheets", "Name": "READ_ME", "Description": "This page."},
        {"Section": "Sheets", "Name": "Flagged Dataset", "Description": "Full dataset plus error flag columns."},
        {"Section": "Sheets", "Name": "Multiple Descriptions", "Description": "Rows where the same component group has more than one non-null description."},
        {"Section": "Sheets", "Name": "Same Description", "Description": "Different components that share an identical description."},
        {"Section": "Sheets", "Name": "Multiple URLs", "Description": "Identical components that have more than one URL."},
        {"Section": "Sheets", "Name": "Same Source Pairs", "Description": "Enabling and dependent components that come from the same source document."},
        {"Section": "Flagged Dataset Columns", "Name": "ERROR: Multiple Descriptions", "Description": "Marked ENABLING/DEPENDENT when the component group has conflicting descriptions."},
        {"Section": "Flagged Dataset Columns", "Name": "ERROR: Same Description", "Description": "Marked ENABLING/DEPENDENT when different components share the same description."},
        {"Section": "Flagged Dataset Columns", "Name": "ERROR: Multiple URLs", "Description": "Marked ENABLING/DEPENDENT when identical components list more than one URL."},
        {"Section": "Flagged Dataset Columns", "Name": "ERROR: Same Source Pair", "Description": "YES when enabling and dependent sources are effectively the same document."},
    ]
    readme_df = pd.DataFrame(readme_rows, columns=["Section", "Name", "Description"])

    with pd.ExcelWriter(output_path) as writer:
        readme_df.to_excel(writer, sheet_name='READ_ME', index=False)
        df.to_excel(writer, sheet_name='Flagged Dataset', index=False)
        error_reports = {
            "Multiple Descriptions": df[df['ERROR: Multiple Descriptions'] != ""],
            "Same Description": df[df['ERROR: Same Description'] != ""],
            "Multiple URLs": df[df['ERROR: Multiple URLs'] != ""],
            "Same Source Pairs": df[df['ERROR: Same Source Pair'] != ""]
        }
        for sheet_name, error_df in error_reports.items():
            error_df.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"FGDEC: Results saved to {output_path} in {time.time() - start:.2f} seconds.")

    print(f"FGDEC: Process completed successfully! Total elapsed time: {time.time() - start_total:.2f} seconds.")

if __name__ == "__main__":
    main()
