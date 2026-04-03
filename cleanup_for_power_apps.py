import openpyxl
import re
import sys
import json
import time
import os
from tkinter import Tk
from tkinter.filedialog import askopenfilename
import pandas as pd

class ProgressTracker:
    def __init__(self, log_file='timings.json'):
        self.log_file = log_file
        self.timings = self.load_timings()
        self.operation_start_time = None
        self.total_start_time = None

    def load_timings(self):
        if os.path.exists(self.log_file):
            with open(self.log_file, 'r') as f:
                return json.load(f)
        return {}

    def save_timings(self):
        with open(self.log_file, 'w') as f:
            json.dump(self.timings, f, indent=4)

    def start_operation(self, op_name):
        self.operation_start_time = time.time()
        print(f"\nStarting operation: {op_name}...")

    def end_operation(self, op_name, items_processed):
        elapsed_time = time.time() - self.operation_start_time
        if op_name not in self.timings:
            self.timings[op_name] = {'total_time': 0, 'total_items': 0}
        
        self.timings[op_name]['total_time'] += elapsed_time
        self.timings[op_name]['total_items'] += items_processed
        self.save_timings()
        print(f"Operation '{op_name}' completed in {elapsed_time:.2f} seconds.")

    def estimate_time(self, op_name, items_to_process):
        if op_name in self.timings and self.timings[op_name]['total_items'] > 0:
            avg_time_per_item = self.timings[op_name]['total_time'] / self.timings[op_name]['total_items']
            return avg_time_per_item * items_to_process
        return None # No estimate available

    def report_progress(self, op_name, current_item, total_items):
        elapsed = time.time() - self.operation_start_time
        
        # Estimate remaining time for current operation
        if total_items is not None and total_items > 0:
            est_remaining_op = self.estimate_time(op_name, total_items - current_item)
            if est_remaining_op is not None:
                progress = (current_item / total_items) * 100
                print(f"\r  - Progress: {progress:.2f}% ({current_item}/{total_items}). Elapsed: {elapsed:.2f}s. Remaining: {est_remaining_op:.2f}s.", end="")
            else:
                print(f"\r  - Progress: {current_item}/{total_items}. Elapsed: {elapsed:.2f}s.", end="")
        else:
            # When total_items is not available, just show elapsed time
            print(f"\r  - Processed {current_item} cells. Elapsed: {elapsed:.2f}s.", end="")

def load_column_config(config_file='column_config.json'):
    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            return json.load(f)
    return {}

def save_column_config(config, config_file='column_config.json'):
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=4)

def cleanup_excel_file(file_path, tracker):
    """
    Cleans up an Excel file using pandas for high performance.
    """
    try:
        print("Loading workbook into pandas...")
        # Reading all sheets into a dictionary of DataFrames
        xls = pd.ExcelFile(file_path)
        sheets_dfs = {sheet_name: xls.parse(sheet_name) for sheet_name in xls.sheet_names}
        
        column_config = load_column_config()
        print("Workbook and column config loaded.")

        special_chars_regex = re.compile("[\x00-\x1F\x7F-\x9F\u00A0\u2022]")

        # --- High-Performance Operations using Pandas ---
        
        tracker.total_start_time = time.time()
        
        # The operations will now be applied per-sheet (DataFrame)
        for sheet_name, df in sheets_dfs.items():
            print(f"\n--- Processing Sheet: {sheet_name} ---")
            if df.empty:
                print("Sheet is empty, skipping.")
                continue

            # 1. Delete special characters
            tracker.start_operation(f"Delete special characters on {sheet_name}")
            df.update(df.select_dtypes(include=['object']).apply(lambda col: col.str.replace(special_chars_regex, '', regex=True)))
            tracker.end_operation(f"Delete special characters on {sheet_name}", df.size)

            # 2. Trim cells based on config (with interaction)
            tracker.start_operation(f"Trim cells on {sheet_name}")
            for col in df.select_dtypes(include=['object']).columns:
                col_name_lower = str(col).lower()
                limit = column_config.get(col_name_lower)

                # Find max length in column to see if we need to prompt
                max_len = df[col].str.len().max()

                if limit is None and max_len > 3999:
                    print(f"\n-- INTERACTIVE PROMPT --")
                    print(f"Column '{col}' in sheet '{sheet_name}' has a max length of {int(max_len)} and is not in the configuration.")
                    new_limit = 3999
                    try:
                        new_limit_str = input(f"Please enter the correct maximum length for '{col}' (or press Enter to use 3999): ")
                        if new_limit_str:
                            new_limit = int(new_limit_str)
                    except ValueError:
                        print("Invalid input. Using 3999.")
                    
                    column_config[col_name_lower] = new_limit
                    save_column_config(column_config)
                    limit = new_limit
                    print(f"Configuration updated for '{col}'. Resuming...")

                if limit:
                    df[col] = df[col].str.slice(0, limit)
            tracker.end_operation(f"Trim cells on {sheet_name}", df.size)

            # 3. Replace smart quotes
            tracker.start_operation(f"Replace smart quotes on {sheet_name}")
            df.update(df.select_dtypes(include=['object']).apply(lambda col: col.str.replace('‘', "'").str.replace('’', "'").str.replace('“', '"').str.replace('”', '"')))
            tracker.end_operation(f"Replace smart quotes on {sheet_name}", df.size)

            # 4. Replace multiple spaces
            tracker.start_operation(f"Replace multiple spaces on {sheet_name}")
            df.update(df.select_dtypes(include=['object']).apply(lambda col: col.str.replace(r' +', ' ', regex=True)))
            tracker.end_operation(f"Replace multiple spaces on {sheet_name}", df.size)

        print("\nSaving workbook from pandas...")
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            for sheet_name, df in sheets_dfs.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"Successfully processed and saved {file_path}")
        # Note: Detailed cell-by-cell report is omitted in this high-performance version.

    except Exception as e:
        print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
    root = Tk()
    root.withdraw()

    file_to_process = askopenfilename(title="Select Excel file to clean")
    
    if file_to_process:
        while True:
            progress_tracker = ProgressTracker()
            cleanup_excel_file(file_to_process, progress_tracker)

            # --- Fix and Re-run Workflow ---
            review_choice = input("\nWould you like to open the file for manual review? (y/n): ").lower()
            if review_choice == 'y':
                print(f"Opening '{os.path.basename(file_to_process)}' for your review...")
                try:
                    os.startfile(file_to_process)
                except Exception as e:
                    print(f"Could not automatically open the file: {e}")
                    print(f"Please open it manually: {file_to_process}")
                
                input("--> After you have saved your changes and closed the file, press Enter here to continue...")

                rerun_choice = input("Would you like to re-run the entire cleanup process on the modified file? (y/n): ").lower()
                if rerun_choice == 'y':
                    print("\n" + "="*50)
                    print("RE-RUNNING CLEANUP PROCESS")
                    print("="*50 + "\n")
                    continue  # This will restart the while loop
                else:
                    print("Exiting.")
                    break  # This will exit the while loop
            else:
                print("Process complete. Exiting.")
                break # This will exit the while loop
    else:
        print("No file selected.")
