"""
PROMPT FOR NAIVE LLM (GENERATE THE FULL SCRIPT BELOW):

UPDATE / TASK:
The Excel layout has changed. You must now enrich TWO parallel component pairs in ivntest.xlsx (located in the SAME DIRECTORY as this script):

PAIR 1 (Enabling):
  Columns:
    - Enabling Component
    - Enabling Component Description
    - Enabling Component URL
  Logic:
    If Enabling Component Description is blank OR exactly equals Enabling Component:
       Fetch and derive textual content from Enabling Component URL and store the cleaned/validated text BACK INTO Enabling Component Description.
    Else:
       Leave existing Enabling Component Description as-is (status = COPIED).

PAIR 2 (Dependent):
  Columns:
    - Dependent Component
    - Dependent Component Description
    - Dependent Component URL
  Logic:
    If Dependent Component Description is blank OR exactly equals Dependent Component:
       Fetch and derive textual content from Dependent Component URL and store the cleaned/validated text BACK INTO Dependent Component Description.
    Else:
       Leave existing Dependent Component Description as-is (status = COPIED).

You must perform BOTH evaluations per row independently.

GENERAL REQUIREMENTS (apply to both pairs):
1. Input:
   - Read ivntest.xlsx from the script directory (first sheet). Preserve original row order and other columns.
2. Content extraction:
   - Detect content type by Content-Type header + extension.
   - HTML: strip <script>, <style>, <noscript>, comments; extract visible text; normalize whitespace; collapse redundant blank lines; remove trivial nav/footer crumbs (e.g., 'home', 'menu').
   - PDF: Extract text from all pages (PyPDF2). If no text -> "[PDF RETRIEVED BUT NO READABLE TEXT FOUND]".
   - Other textual types: accept text/*, json, xml. If binary/unsupported -> mark error.
3. Validation (before storing):
   - Reject if empty.
   - Reject if identical to the URL or only contains the URL.
   - Enforce MIN_CONTENT_CHARS (40) else "[NO MEANINGFUL CONTENT - TOO_SHORT]".
4. Error handling / retries:
   - Retry on timeouts, 5xx, 429 with exponential backoff (1s,2s,4s,...).
   - Distinguish 403, 404, 429, connection errors, redirect loops.
   - After max retries produce status "[TIMEOUT - GAVE UP]" or "[SERVER ERROR ... - GAVE UP]" etc.
5. Caching:
   - Cache by exact URL across BOTH pairs (shared cache).
6. Output columns to ADD (do not remove originals):
   - Enabling Fetch Status
   - Dependent Fetch Status
7. Overwrite only the description columns when derivation is required; otherwise leave original description text untouched (but still assign status COPIED).
8. Truncate any stored description to 32,000 chars (Excel safety).
9. Save output Excel: component_descriptions_from_URLsYYYYMMDDHHMM.xlsx in script directory.
10. Write runtime seconds to ivn_populate_descriptions_runtime.txt.
11. Failure report CSV (failed_url_reportYYYYMMDDHHMM.csv):
    - Rows where either Fetch Status begins with ERROR or where validation produced a NO MEANINGFUL CONTENT marker.
    - Columns: DataFrameIndex, Role (ENABLING/DEPENDENT), URL, Status.
12. Progress:
    - Print per-row progress with dynamic ETA (moving average).
13. Summary:
    - Print separate aggregated counts for Enabling and Dependent statuses.
14. Quality & Structure:
    - Use requests.Session.
    - Functions: fetch_url_content(session,url), sanitize_content(raw), validate_content(url,content).
    - All paths relative to script.
    - Clear comments; only dependencies: pandas, requests, beautifulsoup4, PyPDF2.

COGNITIVE STEPS (EXPLANATION FOR NAIVE LLM):
- Identify new dual-component requirement (Enabling + Dependent) and keep logic independent per row.
- Reuse robust fetch + caching once per URL across both roles.
- Sanitize and validate to avoid storing raw URL or meaningless fragments.
- Maintain separation of concerns: fetch (I/O), sanitize (text cleaning), validate (decision), integrate (row update).
- Provide explicit statuses for auditability (SUCCESS, CACHED, COPIED, SKIPPED-NO-URL, ERROR:<type>).
- Failures aggregated for analytics; runtime logged for performance tracking.

DELIVERABLE:
Produce a single, copy-paste-ready Python script satisfying all above.
"""

import os
import time
from datetime import datetime
from io import BytesIO
import requests
import pandas as pd
from bs4 import BeautifulSoup
import PyPDF2
from requests.exceptions import (
    Timeout,
    ConnectionError as ReqConnectionError,
    TooManyRedirects
)

# ---------------- Configuration ----------------
MIN_CONTENT_CHARS = 40
MAX_EXCEL_CELL = 32000
MAX_RETRIES = 4
INITIAL_BACKOFF = 1.0
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# --------------- Path Setup -------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_XLSX = os.path.join(SCRIPT_DIR, "ivntest.xlsx")
RUNTIME_FILE = os.path.join(SCRIPT_DIR, "ivn_populate_descriptions_runtime.txt")

timestamp = datetime.now().strftime("%Y%m%d%H%M")
OUTPUT_XLSX = os.path.join(SCRIPT_DIR, f"component_descriptions_from_URLs{timestamp}.xlsx")
FAIL_CSV = os.path.join(SCRIPT_DIR, f"failed_url_report{timestamp}.csv")

# --------------- Helpers ----------------------
def load_input_dataframe(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input Excel not found at {path}")
    return pd.read_excel(path, sheet_name=0)

def sanitize_content(raw: str) -> str:
    if not raw:
        return ""
    text = raw.replace("\r", "\n")
    lines = [l.strip() for l in text.split("\n")]
    cleaned = []
    for l in lines:
        if not l:
            cleaned.append("")
            continue
        low = l.lower()
        if low in {"home", "menu"}:
            continue
        if len(l) <= 2 and not l.isalnum():
            continue
        cleaned.append(l)
    final_lines = []
    prev_blank = False
    for l in cleaned:
        is_blank = (l.strip() == "")
        if is_blank and prev_blank:
            continue
        final_lines.append(l)
        prev_blank = is_blank
    return "\n".join(final_lines).strip()

def validate_content(url: str, content: str) -> tuple[bool, str]:
    if not content or not content.strip():
        return False, "EMPTY"
    if content.strip() == url.strip():
        return False, "EQUALS_URL"
    if url in content and len(content.strip()) <= len(url.strip()) + 10:
        return False, "ONLY_URL"
    if len(content.strip()) < MIN_CONTENT_CHARS:
        return False, "TOO_SHORT"
    return True, "OK"

def sanitize_for_excel(text):
    """Remove characters that are not compatible with Excel cells."""
    if not text or not isinstance(text, str):
        return text
    
    # Remove ASCII control characters (0-31) except allowed ones
    allowed = [9, 10, 13]  # Tab, LF, CR
    result = ''.join(c for c in text if ord(c) >= 32 or ord(c) in allowed)
    return result

# --------------- Fetch Logic ------------------
def fetch_url_content(session: requests.Session, url: str, max_retries: int = MAX_RETRIES) -> tuple[str, str]:
    """
    Returns (content_or_message, status_code_string)
    status_code_string categories:
      SUCCESS, ERROR:..., or specific HTTP codes
    """
    if not url or not url.startswith(("http://", "https://")):
        return "[INVALID URL FORMAT]", "ERROR:INVALID_URL"

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    backoff = INITIAL_BACKOFF
    last_status = None
    for attempt in range(1, max_retries + 1):
        try:
            print(f"  Fetch attempt {attempt}/{max_retries} :: {url}")
            resp = session.get(url, headers=headers, timeout=30, allow_redirects=True)
            last_status = resp.status_code

            if resp.status_code == 200:
                ctype = resp.headers.get("Content-Type", "").lower()

                # PDF
                if "pdf" in ctype or url.lower().endswith(".pdf"):
                    try:
                        reader = PyPDF2.PdfReader(BytesIO(resp.content))
                        all_text = []
                        for page in reader.pages:
                            page_text = page.extract_text()
                            if page_text:
                                all_text.append(page_text)
                        joined = "\n".join(all_text).strip()
                        if not joined:
                            return "[PDF RETRIEVED BUT NO READABLE TEXT FOUND]", "SUCCESS"
                        return joined, "SUCCESS"
                    except Exception as e:
                        return f"[PDF PARSING ERROR: {e}]", "ERROR:PDF_PARSE"

                # HTML
                if ("html" in ctype) or any(url.lower().endswith(ext) for ext in (".htm", ".html", "/")):
                    soup = BeautifulSoup(resp.text, "html.parser")
                    for tag in soup(["script", "style", "noscript"]):
                        tag.decompose()
                    for comment in soup.find_all(string=lambda t: isinstance(t, str) and "<!--" in t):
                        comment.extract()
                    text = soup.get_text(separator="\n")
                    return text, "SUCCESS"

                # Other textual
                if any(t in ctype for t in ["text/", "json", "xml"]):
                    return resp.text, "SUCCESS"

                # PDF signature in raw bytes without header
                if resp.content[:4] == b"%PDF":
                    return "[PDF HEADER WITHOUT CONTENT-TYPE]", "ERROR:UNDECLARED_PDF"

                return "[UNSUPPORTED CONTENT TYPE OR BINARY]", "ERROR:UNSUPPORTED_TYPE"

            # 4xx
            if 400 <= resp.status_code < 500:
                if resp.status_code == 429:
                    if attempt < max_retries:
                        print("    429 rate limited. Backing off...")
                        time.sleep(backoff)
                        backoff *= 2
                        continue
                    return "[RATE LIMITED - GAVE UP]", "ERROR:429"
                if resp.status_code == 403:
                    return "[ACCESS DENIED 403 - POSSIBLE SCRAPER BLOCK]", "ERROR:403"
                if resp.status_code == 404:
                    return "[NOT FOUND 404]", "ERROR:404"
                return f"[CLIENT ERROR {resp.status_code}]", f"ERROR:{resp.status_code}"

            # 5xx
            if 500 <= resp.status_code < 600:
                if attempt < max_retries:
                    print(f"    Server {resp.status_code}. Retrying after {backoff:.1f}s...")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                return f"[SERVER ERROR {resp.status_code} - GAVE UP]", f"ERROR:{resp.status_code}"

        except Timeout:
            if attempt < max_retries:
                print(f"    Timeout. Retrying after {backoff:.1f}s...")
                time.sleep(backoff)
                backoff *= 2
                continue
            return "[TIMEOUT - GAVE UP]", "ERROR:TIMEOUT"
        except ReqConnectionError:
            return "[CONNECTION ERROR - HOST UNREACHABLE]", "ERROR:CONNECTION"
        except TooManyRedirects:
            return "[TOO MANY REDIRECTS]", "ERROR:REDIRECT_LOOP"
        except Exception as e:
            return f"[UNEXPECTED ERROR: {e}]", "ERROR:EXCEPTION"

    return "[FAILED AFTER RETRIES]", f"ERROR:{last_status or 'UNKNOWN'}"

# --------------- Main Processing --------------
def main():
    start = time.time()
    try:
        df = load_input_dataframe(INPUT_XLSX)
    except Exception as e:
        print(f"FATAL: Cannot load input Excel: {e}")
        return

    # Ensure columns exist gracefully
    required_cols = {
        "Enabling Component", "Enabling Component Description", "Enabling Component URL",
        "Dependent Component", "Dependent Component Description", "Dependent Component URL"
    }
    missing = required_cols - set(df.columns)
    if missing:
        print(f"FATAL: Missing required columns: {missing}")
        return

    # Prepare output columns
    if "Enabling Fetch Status" not in df.columns:
        df["Enabling Fetch Status"] = ""
    if "Dependent Fetch Status" not in df.columns:
        df["Dependent Fetch Status"] = ""

    # Load prior runtime if present
    prev_runtime = None
    if os.path.exists(RUNTIME_FILE):
        try:
            with open(RUNTIME_FILE, "r") as f:
                prev_runtime = float(f.read().strip())
        except Exception:
            prev_runtime = None

    session = requests.Session()
    url_cache: dict[str, tuple[str, str]] = {}

    total_rows = len(df)
    moving_avg_window = 15
    recent_durations = []
    failures = []

    print(f"Processing {total_rows} rows...")
    loop_start = time.time()
    overall_start_time = time.time()

    for i, (idx, row) in enumerate(df.iterrows(), start=1):
        row_start = time.time()

        # Enabling Component
        enabling_comp = str(row.get("Enabling Component", "") if pd.notna(row.get("Enabling Component")) else "").strip()
        enabling_desc = str(row.get("Enabling Component Description", "") if pd.notna(row.get("Enabling Component Description")) else "").strip()
        enabling_url = str(row.get("Enabling Component URL", "") if pd.notna(row.get("Enabling Component URL")) else "").strip()

        # Dependent Component
        dependent_comp = str(row.get("Dependent Component", "") if pd.notna(row.get("Dependent Component")) else "").strip()
        dependent_desc = str(row.get("Dependent Component Description", "") if pd.notna(row.get("Dependent Component Description")) else "").strip()
        dependent_url = str(row.get("Dependent Component URL", "") if pd.notna(row.get("Dependent Component URL")) else "").strip()

        # Default statuses
        enabling_derived_desc = ""
        enabling_status = ""
        dependent_derived_desc = ""
        dependent_status = ""

        # Enabling Component Logic
        if not enabling_comp:
            enabling_derived_desc = None
            enabling_status = ""
        elif enabling_desc and enabling_desc != enabling_comp:
            enabling_derived_desc = enabling_desc
            enabling_status = "COPIED"
        elif not enabling_url:
            enabling_derived_desc = "[NO URL PROVIDED]"
            enabling_status = "SKIPPED-NO-URL"
        else:
            if enabling_url in url_cache:
                raw_content, fetch_status = url_cache[enabling_url]
                enabling_status = "CACHED" if fetch_status == "SUCCESS" else fetch_status
            else:
                raw_content, fetch_status = fetch_url_content(session, enabling_url)
                url_cache[enabling_url] = (raw_content, fetch_status)
                enabling_status = fetch_status

            if enabling_status in ("SUCCESS", "CACHED"):
                cleaned = sanitize_content(raw_content)
                ok, reason = validate_content(enabling_url, cleaned)
                if ok:
                    enabling_derived_desc = cleaned
                    if enabling_status == "SUCCESS":
                        enabling_status = "SUCCESS"
                else:
                    enabling_derived_desc = f"[NO MEANINGFUL CONTENT - {reason}]"
                    enabling_status = f"ERROR:{reason}"
                    failures.append((idx, "ENABLING", enabling_url, enabling_status))
            else:
                enabling_derived_desc = raw_content
                failures.append((idx, "ENABLING", enabling_url, enabling_status))

        enabling_derived_desc = (enabling_derived_desc or "")[:MAX_EXCEL_CELL] if enabling_derived_desc is not None else None
        df.at[idx, "Enabling Component Description"] = enabling_derived_desc
        df.at[idx, "Enabling Fetch Status"] = enabling_status

        # Dependent Component Logic
        if not dependent_comp:
            dependent_derived_desc = None
            dependent_status = ""
        elif dependent_desc and dependent_desc != dependent_comp:
            dependent_derived_desc = dependent_desc
            dependent_status = "COPIED"
        else:
            if not dependent_url:
                dependent_derived_desc = "[NO URL PROVIDED]"
                dependent_status = "SKIPPED-NO-URL"
            else:
                if dependent_url in url_cache:
                    raw_content, fetch_status = url_cache[dependent_url]
                    dependent_status = "CACHED" if fetch_status == "SUCCESS" else fetch_status
                else:
                    raw_content, fetch_status = fetch_url_content(session, dependent_url)
                    url_cache[dependent_url] = (raw_content, fetch_status)
                    dependent_status = fetch_status

                if dependent_status in ("SUCCESS", "CACHED"):
                    cleaned = sanitize_content(raw_content)
                    ok, reason = validate_content(dependent_url, cleaned)
                    if ok:
                        dependent_derived_desc = cleaned
                        if dependent_status == "SUCCESS":
                            dependent_status = "SUCCESS"
                    else:
                        dependent_derived_desc = f"[NO MEANINGFUL CONTENT - {reason}]"
                        dependent_status = f"ERROR:{reason}"
                        failures.append((idx, "DEPENDENT", dependent_url, dependent_status))
                else:
                    dependent_derived_desc = raw_content
                    failures.append((idx, "DEPENDENT", dependent_url, dependent_status))

        dependent_derived_desc = (dependent_derived_desc or "")[:MAX_EXCEL_CELL] if dependent_derived_desc is not None else None
        df.at[idx, "Dependent Component Description"] = dependent_derived_desc
        df.at[idx, "Dependent Fetch Status"] = dependent_status

        # Progress & ETA
        row_duration = time.time() - row_start
        recent_durations.append(row_duration)
        if len(recent_durations) > moving_avg_window:
            recent_durations.pop(0)
        avg_row_time = sum(recent_durations) / len(recent_durations)
        remaining = total_rows - i
        eta_seconds = remaining * avg_row_time
        eta_m, eta_s = divmod(int(eta_seconds), 60)
        
        # Calculate overall progress
        elapsed_time = time.time() - overall_start_time
        completion_pct = (i / total_rows) * 100
        
        # Format overall time remaining
        if i > 5:  # Wait for a few iterations to get a stable estimate
            estimated_total_time = (elapsed_time / i) * total_rows
            remaining_total = max(0, estimated_total_time - elapsed_time)
            rem_hrs, remainder = divmod(int(remaining_total), 3600)
            rem_mins, rem_secs = divmod(remainder, 60)
            time_str = f"{rem_hrs}h {rem_mins}m {rem_secs}s" if rem_hrs > 0 else f"{rem_mins}m {rem_secs}s"
            print(f"[{i}/{total_rows}] ({completion_pct:.1f}%) Enabling Status={enabling_status}, Dependent Status={dependent_status}")
            print(f"   Row ETA: {eta_m}m {eta_s}s | Overall: {time_str} remaining | Elapsed: {int(elapsed_time/60)}m {int(elapsed_time%60)}s")
        else:
            print(f"[{i}/{total_rows}] ({completion_pct:.1f}%) Enabling Status={enabling_status}, Dependent Status={dependent_status} ETA ~ {eta_m}m {eta_s}s")

    # Consolidate duplicate records based on specified columns, keeping the row with the most detailed info in description/office fields
    group_cols = [
        "Enabling Source", "Enabling Component", "Dependent Component", "Dependent Source",
        "Enabling Component URL", "Dependent Component URL", "Enabling Source Agency", "Dependent Source Agency"
    ]
    detail_cols = [
        "Enabling Component Description", "Dependent Component Description",
        "Enabling Component Responsible Office", "Dependent Component Responsible Office"
    ]

    def pick_most_detailed(group):
        # For each detail column, pick the row with the longest non-null value
        row = group.iloc[0].copy()
        for col in detail_cols:
            max_val = None
            max_len = -1
            for val in group[col]:
                if pd.notna(val) and str(val).strip():
                    l = len(str(val))
                    if l > max_len:
                        max_len = l
                        max_val = val
            row[col] = max_val
        return row

    # Save duplicated groups to a separate Excel file before consolidation
    duplicated_mask = df.duplicated(group_cols, keep=False)
    duplicated_df = df[duplicated_mask].copy()
    if not duplicated_df.empty:
        dup_path = os.path.join(SCRIPT_DIR, "Duplicated Dependent Components.xlsx")
        duplicated_df.to_excel(dup_path, index=False)
        print(f"Duplicated Dependent Components saved: {dup_path}")
    else:
        print("No duplicated Dependent Components found.")

    if all(col in df.columns for col in group_cols + detail_cols):
        df = df.groupby(group_cols, as_index=False).apply(pick_most_detailed).reset_index(drop=True)

    # Apply Excel sanitization to the description fields
    print("Sanitizing content for Excel compatibility...")
    for idx in df.index:
        df.at[idx, "Enabling Component Description"] = sanitize_for_excel(df.at[idx, "Enabling Component Description"])
        df.at[idx, "Dependent Component Description"] = sanitize_for_excel(df.at[idx, "Dependent Component Description"])
    
    # Save main Excel with error handling
    print(f"Saving enriched workbook: {OUTPUT_XLSX}")
    try:
        df.to_excel(OUTPUT_XLSX, index=False)
    except Exception as e:
        print(f"Error saving Excel file: {e}")
        csv_fallback = OUTPUT_XLSX.replace('.xlsx', '.csv')
        print(f"Attempting to save as CSV instead: {csv_fallback}")
        df.to_csv(csv_fallback, index=False)
        print(f"CSV file saved successfully. Excel failed due to illegal characters.")

    # Summary
    enabling_summary_counts = df["Enabling Fetch Status"].value_counts().to_dict()
    dependent_summary_counts = df["Dependent Fetch Status"].value_counts().to_dict()
    print("\n--- SUMMARY ---")
    print("Enabling Component:")
    for k, v in enabling_summary_counts.items():
        print(f"{k}: {v}")
    print("Dependent Component:")
    for k, v in dependent_summary_counts.items():
        print(f"{k}: {v}")
    print("--------------")

    # Failure CSV
    if failures:
        fail_records = []
        for ridx, role, furl, fstat in failures:
            fail_records.append({
                "DataFrameIndex": ridx,
                "Role": role,
                "URL": furl,
                "Status": fstat
            })
        pd.DataFrame(fail_records).to_csv(FAIL_CSV, index=False)
        print(f"Failure report saved: {FAIL_CSV}")
    else:
        print("No failures to report.")

    # Runtime logging
    total_runtime = time.time() - start
    with open(RUNTIME_FILE, "w") as f:
        f.write(str(total_runtime))
    print(f"Runtime: {total_runtime:.2f}s (previous estimate: {prev_runtime if prev_runtime else 'N/A'})")

if __name__ == "__main__":
    main()