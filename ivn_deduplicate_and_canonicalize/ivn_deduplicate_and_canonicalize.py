# filename: ivn_deduplicate_and_canonicalize.py
# last updated: 2025-04-24 05:11 AM

import pandas as pd
import difflib
import json
import time
import os
from datetime import datetime

LOG_FILE = ".ivn_dedupe_log.json"

COLUMN_PAIRS = [
    ("Enabling Component Description", "Dependent Component Description", "Description"),
    ("Enabling Source", "Dependent Source", "Source"),
    ("Enabling Component", "Dependent Component", "Component"),
]

def similarity_score(a, b):
    return difflib.SequenceMatcher(None, a, b).ratio()

def group_similar_strings(values, threshold, prior_groups=None):
    seen = set()
    groups = []
    values = list(values)
    total = len(values)
    start = time.time()

    for idx, val in enumerate(values):
        if val in seen or pd.isna(val):
            continue
        group = [val]
        seen.add(val)
        for other in values:
            if other in seen or pd.isna(other):
                continue
            if similarity_score(val, other) >= threshold:
                group.append(other)
                seen.add(other)
        if len(group) > 1 and (prior_groups is None or group not in prior_groups):
            groups.append(group)

        elapsed = time.time() - start
        percent = (idx + 1) / total * 100
        remaining = (elapsed / (idx + 1)) * (total - idx - 1)
        print(f"[Grouping {percent:5.1f}%] Estimated time left: {remaining:5.1f} sec", end="\r")
    print()
    return groups

def suggest_canonical(group):
    return max(group, key=lambda s: len(s))

def interactive_select(group):
    exclusions = set()
    while True:
        filtered_group = [val for val in group if val not in exclusions]
        if len(filtered_group) < 2:
            print("Only one value remains after exclusions. Skipping group.\n")
            return None, group

        print("\nGroup detected:")
        for i, val in enumerate(filtered_group):
            print(f"{i+1}. {val}")
        suggestion = suggest_canonical(filtered_group)
        suggestion_index = filtered_group.index(suggestion) + 1
        print(f"\nSuggested canonical value: #{suggestion_index}: '{suggestion}'")

        print("Enter:")
        print(" - a number (e.g., 2) to select that item as canonical")
        print(" - a comma-separated list of exclusions (e.g., -2 or -2,-5)")
        print(" - press Enter to accept suggested canonical value")
        print(" - x to skip this group and mark all values as UNIQUE")
        choice = input("Your input: ").strip()

        if choice.lower() == "x":
            return "UNIQUE", group

        elif choice.startswith("-"):
            try:
                indices = [int(c.strip().lstrip("-")) - 1 for c in choice.split(",")]
                for i in indices:
                    if 0 <= i < len(filtered_group):
                        exclusions.add(filtered_group[i])
            except ValueError:
                print("Invalid exclusion format.")
            continue

        elif choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(filtered_group):
                return filtered_group[idx], list(exclusions)

        elif choice == "":
            return suggestion, list(exclusions)

        else:
            print("Invalid input. Try again.")

def apply_canonical_to_df(df, col1, col2, group, canonical, exclusions, pool_name):
    target_group = [val for val in group if val not in exclusions]
    for col in [col1, col2]:
        df[col] = df[col].apply(lambda x: canonical if x in target_group else x)

    if canonical == "UNIQUE":
        tag = f"UNIQUE: {pool_name}"
        for col in [col1, col2]:
            df.loc[df[col].isin(group), "Canonicalization Pool"] = tag
    else:
        for col in [col1, col2]:
            df.loc[df[col].isin(target_group), "Canonicalization Pool"] = pool_name

def load_checkpoint():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            return json.load(f)
    return {"complete": False, "reviewed": {}}

def save_checkpoint(log):
    with open(LOG_FILE, "w") as f:
        json.dump(log, f, indent=2)

def bulk_apply_canonical(df, col1, col2, pool_name, groups, log):
    total = len(groups)
    start_time = time.time()
    reviewed = log["reviewed"].get(pool_name, [])

    for i, group in enumerate(groups, 1):
        group_key = tuple(sorted(group))
        if group_key in reviewed:
            continue

        canonical = suggest_canonical(group)
        apply_canonical_to_df(df, col1, col2, group, canonical, exclusions=[], pool_name=pool_name)
        reviewed.append(group_key)
        log["reviewed"][pool_name] = reviewed
        save_checkpoint(log)

        elapsed = time.time() - start_time
        percent = (i / total) * 100
        remaining = (elapsed / i) * (total - i)
        print(f"[{percent:6.2f}%] Estimated time left: {remaining:.1f} sec")

def main():
    filepath = "ivntest.xlsx"
    # Specify the Components worksheet
    df = pd.read_excel(filepath, sheet_name="Components")

    if "Canonicalization Pool" not in df.columns:
        df["Canonicalization Pool"] = ""

    # Update COLUMN_PAIRS to match Components table columns
    COMPONENT_COLUMN_PAIRS = [
        ("component_name", "component_description", "ComponentInfo"),
        ("source", "component_name", "SourceComponent"),
        # Add other relevant pairs if needed
    ]

    log = load_checkpoint()

    if not log["complete"]:
        if log["reviewed"]:
            resume = input("Resume last deduplication session? (y/n): ").strip().lower()
            if resume != "y":
                log = {"complete": False, "reviewed": {}}
                if os.path.exists(LOG_FILE):
                    os.remove(LOG_FILE)

    reset = input("Recompare previously marked UNIQUE records? (y/n): ").strip().lower()
    if reset == "y":
        df.loc[df["Canonicalization Pool"].str.startswith("UNIQUE:"), "Canonicalization Pool"] = ""

    mode = input("Mode? Type 'i' for interactive or 'b' for bulk: ").strip().lower()
    while mode not in ["i", "b"]:
        mode = input("Please type 'i' for interactive or 'b' for bulk: ").strip().lower()

    while True:
        try:
            threshold = float(input("Enter similarity threshold (0.0–1.0): ").strip())
            if 0.0 <= threshold <= 1.0:
                break
        except ValueError:
            pass
        print("Invalid input. Please enter a number between 0.0 and 1.0.")

    total_checks = len(COMPONENT_COLUMN_PAIRS)
    check_counter = 0
    overall_start = time.time()

    for col1, col2, pool_name in COMPONENT_COLUMN_PAIRS:
        check_counter += 1
        print(f"\n[{check_counter}/{total_checks}] Checking similarity for: {pool_name}")
        pair_start = time.time()

        if pool_name in log.get("reviewed", {}) and log["complete"]:
            print(f"Skipping {pool_name} — already marked complete.")
            continue

        unique_mask = df["Canonicalization Pool"].str.startswith("UNIQUE:")
        values_to_check = pd.concat([
            df.loc[~unique_mask, col1],
            df.loc[~unique_mask, col2]
        ]).dropna().unique()

        prior_groups = log.get("reviewed", {}).get(pool_name, [])
        groups = group_similar_strings(values_to_check, threshold)

        if not groups:
            print("No similar values found.\n")
            continue

        print(f"Found {len(groups)} groups of similar values.\n")

        if mode == "i":
            reviewed = log["reviewed"].get(pool_name, [])
            for group in groups:
                group_key = tuple(sorted(group))
                if group_key in reviewed:
                    continue
                canonical, exclusions = interactive_select(group)
                apply_canonical_to_df(df, col1, col2, group, canonical, exclusions, pool_name)
                reviewed.append(group_key)
                log["reviewed"][pool_name] = reviewed
                save_checkpoint(log)
        else:
            bulk_apply_canonical(df, col1, col2, pool_name, groups, log)

        pair_elapsed = time.time() - pair_start
        overall_elapsed = time.time() - overall_start
        avg_time = overall_elapsed / check_counter
        remaining_time = avg_time * (total_checks - check_counter)
        print(f"[Progress] Estimated total time left: {remaining_time:.1f} sec\n")

    log["complete"] = True
    save_checkpoint(log)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tsv_name = f"normalized_output_{timestamp}.tsv"
    df.to_csv(tsv_name, index=False, encoding="utf-8", sep='\t')
    df.to_excel(filepath, index=False)

    print(f"\n Done! Changes saved to:")
    print(f"- Updated Excel: {filepath}")
    print(f"- New TSV file: {tsv_name}")
    print(f"- Checkpoint log: {LOG_FILE} (marked complete)")

if __name__ == "__main__":
    main()



