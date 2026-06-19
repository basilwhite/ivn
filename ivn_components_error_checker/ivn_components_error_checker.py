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
import pickle
import os
from datetime import datetime

__all__ = ["main"]

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
        # Check for progress file
        progress_file = "ivn_components_error_checker_progress.pkl"
        progress_exists = os.path.exists(progress_file)
        resume_data = None
        if progress_exists:
            print("FGDEC: Detected unfinished progress from a previous session.")
            while True:
                resume_choice = input("Do you want to continue where you left off (1) or start over (2)? Enter 1 or 2: ").strip()
                if resume_choice in {"1", "2"}:
                    break
                print("Invalid input. Please enter 1 or 2.")
            if resume_choice == "1":
                with open(progress_file, "rb") as f:
                    resume_data = pickle.load(f)
            else:
                os.remove(progress_file)
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
            excel_file = pd.ExcelFile(file_path)
            print(f"FGDEC: Dataset loaded in {time.time() - start:.2f} seconds.")
            break
        elif choice == "2":
            sheet_url = input("Enter the Google Sheets XLSX export URL: ").strip()
            print(f"FGDEC: Loading dataset from URL: {sheet_url}")
            start = time.time()
            response = requests.get(sheet_url)
            excel_file = pd.ExcelFile(BytesIO(response.content))
            print(f"FGDEC: Dataset loaded in {time.time() - start:.2f} seconds.")
            break
        else:
            print("Invalid choice. Please enter 1 or 2.")


    if resume_data is not None:
        df = resume_data['df']
        selected_column = resume_data['selected_column']
        group_state = resume_data['group_state']
        print(f"FGDEC: Resuming duplicate resolution for column '{selected_column}'.")
    else:
        # Always prompt for sheet selection
        print("FGDEC: Sheets found in the dataset:")
        for idx, sheet_name in enumerate(excel_file.sheet_names, 1):
            print(f"  {idx}. {sheet_name}")
        while True:
            sheet_choice = input(f"Enter the number of the sheet to analyze for false unique records (1-{len(excel_file.sheet_names)}): ").strip()
            if sheet_choice.isdigit() and 1 <= int(sheet_choice) <= len(excel_file.sheet_names):
                selected_sheet = excel_file.sheet_names[int(sheet_choice) - 1]
                break
            else:
                print("Invalid choice. Please enter a valid number.")
        df = pd.read_excel(excel_file, sheet_name=selected_sheet)

        print(f"FGDEC: Columns in selected data:")
        for idx, col in enumerate(df.columns, 1):
            print(f"  {idx}. {col}")
        while True:
            col_choice = input(f"Enter the number of the column to analyze for false unique records (1-{len(df.columns)}): ").strip()
            if col_choice.isdigit() and 1 <= int(col_choice) <= len(df.columns):
                selected_column = df.columns[int(col_choice) - 1]
                break
            else:
                print("Invalid choice. Please enter a valid number.")

        print(f"FGDEC: You selected column '{selected_column}'.")
        group_state = None

    # Step 1: Show summary of non-unique values (exact), but do not print all values or prompt for rows
    if resume_data is None:
        value_counts = df[selected_column].value_counts(dropna=False)
        non_unique_count = (value_counts > 1).sum()
        if non_unique_count == 0:
            print(f"FGDEC: All values in column '{selected_column}' are unique.")
        else:
            print(f"FGDEC: There are {non_unique_count} non-unique (exact duplicate) values in column '{selected_column}'. Proceeding to fuzzy/normalized duplicate analysis...")

    # Step 2: Fuzzy/normalized duplicate detection and interactive resolution (apply all fields from canonical row)
    print(f"\nFGDEC: Checking for possible false unique values (fuzzy/normalized duplicates) in '{selected_column}'...")
    df = resolve_duplicates_apply_row(df, selected_column, threshold=90, prompt_message="Choose the canonical record for these similar entries", progress_file=progress_file, group_state=group_state)
    if os.path.exists(progress_file):
        os.remove(progress_file)
    print(f"FGDEC: Fuzzy/normalized duplicate resolution complete. All fields from the canonical record were applied to similar records.")

def resolve_duplicates_apply_row(df, col, threshold=90, prompt_message="Choose the canonical record for these similar entries"):
    df['norm'] = df[col].apply(normalize_text)
    # Build fuzzy groups only across distinct normalized strings that look similar
    exact_groups = df.groupby('norm').indices
    norms = list(exact_groups.keys())
    buckets = {}
    def bucket_key(val):
        return (val[:10], val.split(' ', 1)[0])
    for norm_val in norms:
        buckets.setdefault(bucket_key(norm_val), []).append(norm_val)
    processed_norms = set()
    to_process = []
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

    # Show and resolve one group at a time, with user-driven merge/skip/partial merge
    import pickle
    import sys
    # If resuming, skip already processed groups
    if 'group_state' in locals() and group_state is not None:
        start_group = group_state.get('current_group', 0)
        to_process = group_state['to_process']
    else:
        start_group = 0
        # ...existing code...
    for group_num, indices in enumerate(to_process, 1):
        if group_num <= start_group:
            continue
        rows = df.loc[indices]
        options = rows[col].tolist()
        print(f"\nGroup {group_num} (rows: {list(indices)}):")
        # Show first 200 chars of all fields for each row
        for idx, (row_idx, row) in enumerate(rows.iterrows(), 1):
            preview = []
            for field in df.columns:
                val = str(row[field])
                if len(val) > 200:
                    val = val[:200] + "..."
                preview.append(f"{field} = {val}")
            print(f"{idx}. {col} = {row[col]}\t" + "\t".join(preview))

        print("\nHow do you want to resolve this group?")
        print("1: All values are truly unique (do nothing)")
        print("2: Set ALL values to a single value (merge all)")
        print("3: Set SOME values to a single value (merge subset)")
        print("4: Pause and save progress")
        while True:
            action = input("Enter 1, 2, 3, or 4: ").strip()
            if action in {"1", "2", "3", "4"}:
                break
            print("Invalid choice. Please enter 1, 2, 3, or 4.")

        if action == "4":
            # Save progress and exit
            group_state = {'current_group': group_num, 'to_process': to_process}
            with open(progress_file, "wb") as f:
                pickle.dump({'df': df, 'selected_column': col, 'group_state': group_state}, f)
            print(f"Progress saved. You can resume later.")
            sys.exit(0)
        if action == "1":
            print("No changes made to this group.")
            continue
        elif action == "2":
            chosen_val = prompt_user_choice(options, "Select the value to apply to ALL items in this group:")
            for idx2 in rows.index:
                df.loc[idx2, col] = chosen_val
            print(f"All values in this group set to: {chosen_val}")
        elif action == "3":
            print("Select the numbers of the items to merge (comma-separated, e.g. 1,3,4):")
            while True:
                subset_input = input("Numbers to merge: ").strip()
                try:
                    subset_indices = [int(x)-1 for x in subset_input.split(",") if x.strip().isdigit() and 1 <= int(x) <= len(options)]
                    if subset_indices:
                        break
                except Exception:
                    pass
                print("Invalid input. Please enter valid numbers separated by commas.")
            chosen_val = prompt_user_choice([options[i] for i in subset_indices], "Select the value to apply to the selected items:")
            for i in subset_indices:
                df.loc[rows.index[i], col] = chosen_val
            print(f"Selected items set to: {chosen_val}")
    df.drop(columns=['norm'], inplace=True)
    return df

# Usage:
#   python ivn_components_error_checker.py
#   or from Python REPL:
#       import ivn_components_error_checker
#       ivn_components_error_checker.main()
if __name__ == "__main__":
    main()