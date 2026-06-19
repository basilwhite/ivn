import pandas as pd
import requests
import time


# This is Infer_URLs.py - last execution time: 5880.22 seconds


# File paths (update if necessary)
input_file = r"C:\Users\basil.white\Python\scripts\Infer_URLs\ivntest.xlsx"
output_file = r"C:\Users\basil.white\Python\scripts\Infer_URLs\ivntest_checked.xlsx"


# Start time tracking
start_time = time.time()


# Load the Excel file (first sheet automatically)
df = pd.read_excel(input_file, sheet_name=0)
print("✅ Loaded Excel file successfully.")


# Ensure 'Enabling URL Status' and 'Dependent URL Status' columns exist
if "Enabling URL Status" not in df.columns:
    df["Enabling URL Status"] = ""


if "Dependent URL Status" not in df.columns:
    df["Dependent URL Status"] = ""


df["Enabling URL Status"] = df["Enabling URL Status"].astype(str)
df["Dependent URL Status"] = df["Dependent URL Status"].astype(str)


# 1. Fill missing URLs using existing mappings
enabling_url_map = df.dropna(subset=["Enabling Component URL"]).set_index("Enabling Component")["Enabling Component URL"].to_dict()
dependent_url_map = df.dropna(subset=["Dependent Component URL"]).set_index("Dependent Component")["Dependent Component URL"].to_dict()


df["Enabling Component URL"] = df.apply(
    lambda row: enabling_url_map.get(row["Enabling Component"], row["Enabling Component URL"]), axis=1
)
df["Dependent Component URL"] = df.apply(
    lambda row: dependent_url_map.get(row["Dependent Component"], row["Dependent Component URL"]), axis=1
)


# Count inferred URLs
inferred_enabling = df["Enabling Component URL"].notna().sum()
inferred_dependent = df["Dependent Component URL"].notna().sum()


print(f"🔍 Inferred {inferred_enabling} Enabling URLs and {inferred_dependent} Dependent URLs.")


# 2. Function to check URL status with caching
url_status_cache = {}


def check_url_status(url):
    if url in url_status_cache:
        return url_status_cache[url]  # Use cached result
    
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        status = "error" if response.status_code >= 400 else "valid"
    except requests.RequestException:
        status = "error"


    url_status_cache[url] = status  # Cache the result
    return status


# 3. Apply function to check URLs with progress tracking
broken_enabling_urls = 0
broken_dependent_urls = 0
total_urls = df["Enabling Component URL"].count() + df["Dependent Component URL"].count()
checked_urls = 0


print("⏳ Checking URLs (this may take a few minutes)...")


for index, row in df.iterrows():
    if pd.notna(row["Enabling Component URL"]):
        status = check_url_status(row["Enabling Component URL"])
        df.at[index, "Enabling URL Status"] = status
        if status == "error":
            broken_enabling_urls += 1


    if pd.notna(row["Dependent Component URL"]):
        status = check_url_status(row["Dependent Component URL"])
        df.at[index, "Dependent URL Status"] = status
        if status == "error":
            broken_dependent_urls += 1


    checked_urls += 1
    if checked_urls % 100 == 0:  # Print status every 100 checks
        print(f"🔄 Checked {checked_urls}/{total_urls} URLs...")


# 4. Highlight errors in orange in Excel
def apply_color(val):
    return 'background-color: orange' if val == "error" else ''


styled_df = df.style.applymap(apply_color, subset=["Enabling Component URL", "Dependent Component URL"])


# Save the updated file
styled_df.to_excel(output_file, engine="openpyxl", index=False)


# Summary
print("\n✅ URL check complete!")
print(f"❌ Broken Enabling URLs: {broken_enabling_urls}")
print(f"❌ Broken Dependent URLs: {broken_dependent_urls}")
print(f"📂 Processed file saved as: {output_file}")


# Show execution time
elapsed_time = round(time.time() - start_time, 2)
print(f"⏱️ Total execution time: {elapsed_time} seconds")
