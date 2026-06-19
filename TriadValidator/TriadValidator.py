"""
TriadValidator.py
Cryptographic guard for IVN dataset triad integrity.
Ensures each (component_name, description, source) is sacrosanct and uncorrupted.

This version automatically finds and uses the 'Components' sheet in ivntest.xlsx,
and uses the columns: component_name, description, source.

Novice instructions:
- Run this script before and after any data processing.
- If errors appear, follow the ASCII/emoji repair guide below.
- Always "Save As" after each step (File > Save As...).

Requirements:
- Python 3.8+
- pandas
"""

import os
import sys
import hashlib
import pandas as pd
import time

TRIAD_COLUMNS = ['component_name', 'component_description', 'source']  # FIXED: use 'component_description'
HASH_COLUMN = 'TriadHash'
BACKUP_SUFFIX = '_backup'
COMPONENTS_SHEET = 'Components'

def hash_triad(row):
    """Create a SHA256 hash for the triad."""
    triad_str = f"{row['component_name']}||{row['component_description']}||{row['source']}"  # FIXED
    return hashlib.sha256(triad_str.encode('utf-8')).hexdigest()

def backup_file(filepath):
    """Create a backup of the file."""
    backup_path = filepath.replace('.xlsx', f'{BACKUP_SUFFIX}.xlsx')
    if not os.path.exists(backup_path):
        with pd.ExcelFile(filepath) as xls:
            with pd.ExcelWriter(backup_path, engine='openpyxl') as writer:
                for sheet in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=sheet)
                    df.to_excel(writer, sheet_name=sheet, index=False)
        print(f"✅ Backup created: {backup_path}")
    else:
        print(f"ℹ️ Backup already exists: {backup_path}")

def validate_triad_hashes(filepath):
    """Validate triad hashes in the Components sheet."""
    with pd.ExcelFile(filepath) as xls:
        if COMPONENTS_SHEET not in xls.sheet_names:
            print(f"❌ '{COMPONENTS_SHEET}' sheet not found in {filepath}")
            sys.exit(1)
        df = pd.read_excel(xls, sheet_name=COMPONENTS_SHEET)
    errors = []
    total = len(df)
    print(f"🔎 Validating triad hashes in {total} rows...")
    start_time = time.time()
    for idx, row in df.iterrows():
        try:
            expected_hash = hash_triad(row)
            actual_hash = str(row.get(HASH_COLUMN, ''))
            if expected_hash != actual_hash:
                errors.append((idx, {col: row.get(col, '') for col in TRIAD_COLUMNS}, actual_hash, expected_hash))
        except Exception as e:
            errors.append((idx, {col: row.get(col, '') for col in TRIAD_COLUMNS}, 'ERROR', f'Exception: {e}'))
        if (idx + 1) % 10 == 0 or idx == total - 1:
            elapsed = time.time() - start_time
            done = idx + 1
            left = total - done
            avg_time = elapsed / done if done else 0
            est_left = avg_time * left
            mins, secs = divmod(int(est_left), 60)
            print(f"  Progress: {done}/{total} rows | {left} left | Est. {mins}m {secs}s remaining")
    return errors

def add_triad_hashes(filepath):
    """Add triad hashes to the Components sheet."""
    with pd.ExcelFile(filepath) as xls:
        sheets = {sheet: pd.read_excel(xls, sheet_name=sheet) for sheet in xls.sheet_names}
    if COMPONENTS_SHEET not in sheets:
        print(f"❌ '{COMPONENTS_SHEET}' sheet not found in {filepath}")
        sys.exit(1)
    df = sheets[COMPONENTS_SHEET]
    for col in TRIAD_COLUMNS:
        if col not in df.columns:
            print(f"❌ Column '{col}' not found in '{COMPONENTS_SHEET}' sheet.")
            sys.exit(1)
    total = len(df)
    print(f"🔄 Adding triad hashes to {total} rows...")
    start_time = time.time()
    hashes = []
    for idx, row in df.iterrows():
        hashes.append(hash_triad(row))
        if (idx + 1) % 10 == 0 or idx == total - 1:
            elapsed = time.time() - start_time
            done = idx + 1
            left = total - done
            avg_time = elapsed / done if done else 0
            est_left = avg_time * left
            mins, secs = divmod(int(est_left), 60)
            print(f"  Progress: {done}/{total} rows | {left} left | Est. {mins}m {secs}s remaining")
    df[HASH_COLUMN] = hashes
    sheets[COMPONENTS_SHEET] = df
    with pd.ExcelWriter(filepath, engine='openpyxl', mode='w') as writer:
        for sheet, data in sheets.items():
            data.to_excel(writer, sheet_name=sheet, index=False)
    print(f"✅ Triad hashes added to '{COMPONENTS_SHEET}' sheet in {filepath}")

def repair_triad_hashes(filepath):
    """Repair triad hashes in the Components sheet."""
    with pd.ExcelFile(filepath) as xls:
        sheets = {sheet: pd.read_excel(xls, sheet_name=sheet) for sheet in xls.sheet_names}
    if COMPONENTS_SHEET not in sheets:
        print(f"❌ '{COMPONENTS_SHEET}' sheet not found in {filepath}")
        sys.exit(1)
    df = sheets[COMPONENTS_SHEET]
    for col in TRIAD_COLUMNS:
        if col not in df.columns:
            print(f"❌ Column '{col}' not found in '{COMPONENTS_SHEET}' sheet.")
            sys.exit(1)
    total = len(df)
    print(f"🛠️ Repairing triad hashes in {total} rows...")
    start_time = time.time()
    hashes = []
    for idx, row in df.iterrows():
        hashes.append(hash_triad(row))
        if (idx + 1) % 10 == 0 or idx == total - 1:
            elapsed = time.time() - start_time
            done = idx + 1
            left = total - done
            avg_time = elapsed / done if done else 0
            est_left = avg_time * left
            mins, secs = divmod(int(est_left), 60)
            print(f"  Progress: {done}/{total} rows | {left} left | Est. {mins}m {secs}s remaining")
    df[HASH_COLUMN] = hashes
    sheets[COMPONENTS_SHEET] = df
    with pd.ExcelWriter(filepath, engine='openpyxl', mode='w') as writer:
        for sheet, data in sheets.items():
            data.to_excel(writer, sheet_name=sheet, index=False)
    print(f"🔧 Triad hashes repaired in '{COMPONENTS_SHEET}' sheet in {filepath}")

def print_error_guide(errors):
    """Print visual error guide for novice users."""
    print("\n🛑 Triad Integrity Errors Detected!\n")
    print("Each row below has a corrupted triad. Follow the repair steps.")
    print("\nASCII/Emoji Example:")
    print("""
    +---------------------------------------------------+
    | component_name | description          | source     |
    +---------------------------------------------------+
    | Safety Review  | Product inspections  | FDA Reg 21 |  ← ❌ CORRUPTED
    |                | (should be 'Safety protocols')   |
    +---------------------------------------------------+
    """)
    print("Repair Steps:")
    print("1️⃣ Open your Excel file.")
    print("2️⃣ Go to the 'Components' tab.")
    print("3️⃣ Find the row number listed below.")
    print("4️⃣ Compare 'description' and 'source' to your backup file.")
    print("5️⃣ Correct the values to match the original triad.")
    print("6️⃣ Save your file using File > Save As... (e.g., TriadValidator_FIXED.xlsx)")
    print("7️⃣ Re-run this script to confirm all errors are fixed.\n")
    print("Corrupted Rows:")
    for idx, triad, actual, expected in errors:
        print(f"Row {idx+2}: {triad} | Hash: {actual} (should be {expected})")

def print_menu():
    print("\nWhat do you want to do with your IVN Excel file (ivntest.xlsx)?")
    print("1. Add cryptographic triad hashes (first time use)")
    print("2. Validate/check for triad corruption")
    print("3. Repair triad hashes after editing data")
    print("q. Quit")
    print("Type 1, 2, 3, or q and press Enter:")

def main():
    # Always use ivntest.xlsx in the same folder as the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(script_dir, "ivntest.xlsx")

    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        print("Make sure 'ivntest.xlsx' is in the same folder as this script.")
        print("If you need a template, create an Excel file with a 'Components' tab and columns: component_name, description, source")
        input("Press Enter to exit...")
        sys.exit(1)

    backup_file(filepath)

    while True:
        print_menu()
        choice = input().strip().lower()
        if choice == '1':
            add_triad_hashes(filepath)
            break
        elif choice == '2':
            errors = validate_triad_hashes(filepath)
            if errors:
                print_error_guide(errors)
                print("❌ Triad corruption detected. Please repair before proceeding.")
                input("Press Enter to exit...")
                sys.exit(2)
            else:
                print("✅ All triads are valid and uncorrupted.")
            break
        elif choice == '3':
            repair_triad_hashes(filepath)
            break
        elif choice == 'q':
            print("Exiting. No changes made.")
            sys.exit(0)
        else:
            print("❌ Invalid choice. Please type 1, 2, 3, or q.")

if __name__ == "__main__":
    main()

"""
🖼️ Visual Error Example:
+---------------------------------------------------+
| component_name | description          | source     |
+---------------------------------------------------+
| Safety Review  | Product inspections  | FDA Reg 21 |  ← ❌ CORRUPTED
|                | (should be 'Safety protocols')   |
+---------------------------------------------------+

📝 Save As Instructions:
- Click File > Save As...
- Enter a new filename (e.g., TriadValidator_FIXED.xlsx)
- Click Save

💻 Windows/Mac Dual Instructions:
- Windows: Use Ctrl for shortcuts
- Mac: Use Cmd for shortcuts

📋 Troubleshooting Matrix:
| Error Message                | What It Means         | How to Fix                |
|------------------------------|----------------------|---------------------------|
| File not found               | Wrong filename       | Check path and spelling   |
| Triad corruption detected    | Data mismatch        | Repair using backup       |
| Unknown command              | Typo in command      | Use add/validate/repair   |
"""