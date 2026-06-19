import pandas as pd
import requests
import io
from PyPDF2 import PdfReader
import sys
import os
import time
import random
from tqdm import tqdm
from datetime import datetime

SEARCH_WORDS = ["Service", "Office", "Center", "Agency", "Institute"]

print("Starting PDF context extraction script...")

try:
    print("Reading ivntest.xlsx...")
    df = pd.read_excel("ivntest.xlsx", engine="openpyxl")
except Exception as e:
    print(f"Failed to read Excel file: {e}")
    sys.exit(1)

if "Enabling Component URL" not in df.columns or "Dependent Component URL" not in df.columns:
    print("Required columns are missing in the Excel file.")
    sys.exit(1)

# Cache to avoid reprocessing the same URL
url_cache = {}

# List of user agents for browser mimicry
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

def extract_pdf_context(url, search_words):
    if url in url_cache:
        print(f"  Using cached result for URL: {url}")
        return url_cache[url]

    print(f"  Downloading PDF: {url}")
    try:
        headers = get_headers()
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        pdf_file = io.BytesIO(response.content)
        reader = PdfReader(pdf_file)
        text = "\n".join([page.extract_text() or "" for page in reader.pages])
        results = []
        for word in search_words:
            start = 0
            while True:
                idx = text.find(word, start)
                if idx == -1:
                    break
                before = max(0, idx - 200)
                after = idx + len(word) + 200
                snippet = text[before:after].replace('\n', ' ').replace('\r', ' ')
                while '  ' in snippet:
                    snippet = snippet.replace('  ', ' ')
                results.append(snippet)
                start = idx + len(word)
        print(f"    Found {len(results)} matches for search words.")
        context = "\n---\n".join(results)
        url_cache[url] = context
        return context
    except requests.exceptions.HTTPError as e:
        error_message = f"HTTP Error: {e.response.status_code} for url: {url}"
        print(f"    Error processing PDF at {url[:60]}...: {error_message}")
        url_cache[url] = error_message
        return error_message
    except Exception as e:
        error_message = f"Error: {e}"
        print(f"    Error processing PDF at {url[:60]}...: {e}")
        url_cache[url] = error_message
        return error_message

def process_with_status_bar(column_name, target_column):
    total = len(df)
    start_time = time.time()
    results = []
    for i, url in enumerate(df[column_name]):
        if pd.notna(url) and str(url).strip():
            result = extract_pdf_context(url, SEARCH_WORDS)
        else:
            result = ""
        results.append(result)
        elapsed = time.time() - start_time
        percent = (i + 1) / total * 100
        if i + 1 < total and percent > 0:
            eta = elapsed / (i + 1) * (total - (i + 1))
        else:
            eta = 0
        bar = f"[{int(percent)//2*'='}{(50-int(percent)//2)*' '}]"
        print(f"\r{column_name}: {i+1}/{total} {bar} {percent:.1f}% complete, ETA: {int(eta)}s", end="")
    print()
    df[target_column] = results

try:
    print("Processing Enabling Component URLs...")
    process_with_status_bar("Enabling Component URL", "Enabling Component Responsible Office")

    print("Processing Dependent Component URLs...")
    process_with_status_bar("Dependent Component URL", "Dependent Component Responsible Office")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.abspath(f"ivntest_output_{timestamp}.tsv")
    print(f"Saving results to {output_path} ...")
    df.to_csv(output_path, index=False, sep='\t', encoding='utf-8-sig')
    print(f"Script completed successfully. Output saved as {output_path}.")
except Exception as e:
    print(f"Script failed: {e}")
    sys.exit(1)
