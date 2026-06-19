# PROMPT FOR NOVICE LLM:
# Write a Python script that automates quality checks for a federal governance dataset exported from Google Sheets or loaded from a local file.
# The script should:
# 1. Ask the user if they want to select a local file or enter a URL for the dataset.
# 2. If local file, provide a file navigator starting from the script's folder.
# 3. Load the dataset from the selected local file or provided Google Sheets XLSX URL.
# 4. Create a unified view of components (enabling and dependent) with columns for Source, Component, Description, and URL.
# 5. Group similar components using fuzzy matching on Source and Component names.
# 6. For each group, interactively prompt the user to resolve inconsistencies in Component names, Source names, Descriptions, and URLs by choosing the correct value.
# 7. Flag errors in the original dataset for:
#    - Components with multiple non-null descriptions
#    - Different components with identical descriptions
#    - Identical components with multiple URLs
#    - Enabling-dependent component pairs from the same source document
# 8. Track and print elapsed time for each major operation using the time module.
# 9. Save the flagged dataset and error reports to an Excel file.
# Use pandas, tqdm, requests, difflib, re, and os. Make the script easy to follow for a novice.
# Update this prompt every time you update the script.

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

def normalize_text(text):
    """Normalize text for fuzzy matching without external dependencies"""
    if pd.isna(text):
        return ""
    text = str(text).lower()
    text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
    text = re.sub(r'\s+', ' ', text).strip()  # Remove extra spaces
    return text

def similarity_ratio(a, b):
    """Pure Python implementation of similarity ratio"""
    a = normalize_text(a)
    b = normalize_text(b)
    if not a and not b:
        return 100
    return SequenceMatcher(None, a, b).ratio() * 100

def get_component_groups(df, source_col, component_col):
    """Create similarity-matched groups for (source, component) pairs"""
    groups = {}
    group_map = {}
    group_counter = 0
    
    # Create normalized versions
    df['source_norm'] = df[source_col].apply(normalize_text)
    df['component_norm'] = df[component_col].apply(normalize_text)
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Grouping components"):
        source = row['source_norm']
        component = row['component_norm']
        matched_group = None
        
        # Skip empty components
        if not component:
            continue
            
        # Check against existing groups
        for group_id, (group_source, group_component) in groups.items():
            source_sim = similarity_ratio(source, group_source)
            comp_sim = similarity_ratio(component, group_component)
            
            if source_sim >= 95 and comp_sim >= 95:
                matched_group = group_id
                break
        
        # Create new group if no match found
        if matched_group is None:
            group_counter += 1
            matched_group = group_counter
            groups[matched_group] = (source, component)
        
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
    """Find similar values and prompt user to resolve duplicates."""
    df['norm'] = df[col].apply(normalize_text)
    groups = {}
    for idx, row in df.iterrows():
        val = row['norm']
        matched = False
        for group_val in groups:
            if similarity_ratio(val, group_val) >= threshold:
                groups[group_val].append(idx)
                matched = True
                break
        if not matched:
            groups[val] = [idx]
    for group_val, indices in groups.items():
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

    print("FGDEC: Resolving multiple URLs...")
    start = time.time()
    components_df = resolve_duplicates(
        components_df, 'URL', 'group_id', prompt_message="Choose the correct URL:"
    )
    print(f"FGDEC: URL resolution completed in {time.time() - start:.2f} seconds.")

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
    with pd.ExcelWriter('error_flagged_dataset.xlsx') as writer:
        df.to_excel(writer, sheet_name='Flagged Dataset', index=False)
        error_reports = {
            "Multiple Descriptions": df[df['ERROR: Multiple Descriptions'] != ""],
            "Same Description": df[df['ERROR: Same Description'] != ""],
            "Multiple URLs": df[df['ERROR: Multiple URLs'] != ""],
            "Same Source Pairs": df[df['ERROR: Same Source Pair'] != ""]
        }
        for sheet_name, error_df in error_reports.items():
            error_df.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"FGDEC: Results saved in {time.time() - start:.2f} seconds.")

    print(f"FGDEC: Process completed successfully! Total elapsed time: {time.time() - start_total:.2f} seconds.")

if __name__ == "__main__":
    main()
