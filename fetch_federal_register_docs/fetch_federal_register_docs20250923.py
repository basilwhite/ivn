# fetch_federal_register_docs.py
# Last updated: 2025-07-02 🕓

import requests
import pandas as pd
import time
import os
from datetime import datetime

BASE_URL = "https://www.federalregister.gov/api/v1/documents.json"
PARAMS = {
    "conditions[president][]": "donald-trump",
    "conditions[presidential_document_type][]": [
        "determination", "executive_order", "memorandum", "notice",
        "proclamation", "presidential_order", "other"
    ],
    "conditions[publication_date][year]": "2025",
    "conditions[term]": '"homeland security" & (resilience | enterprise | stakeholder | interoperability | sustainment | governance | "mission space" | outcome | capability | capacity | effectiveness | efficiency | benchmark | "threat vector" | mitigation | consequence | hazard | continuity | "fusion center" | synchronize | operationalize | deconflict | enablement | execution | alignment | scalability | modernization | transformation | innovation | integration)',
    "conditions[type][]": "PRESDOCU",
    "per_page": 100,
    "page": 1,
    "order": "newest"
}

def fetch_document_text(url):
    """Fetch body text from a Federal Register document."""
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")
        main_content = soup.find("div", class_="article-body") or soup.find("div", id="article")
        if main_content:
            return " ".join(main_content.stripped_strings)
        return ""
    except Exception as e:
        print(f"❌ Failed to fetch description from {url}: {e}")
        return ""

def fetch_documents():
    all_docs = []
    page = 1

    while True:
        PARAMS["page"] = page
        print(f"📄 Fetching page {page} with params: {PARAMS}")
        try:
            response = requests.get(BASE_URL, params=PARAMS, timeout=10)
            print(f"🔎 HTTP {response.status_code} for {response.url}")
            if response.status_code != 200:
                print("⚠️  Response content (first 300 chars):")
                print(response.text[:300])
                break
            data = response.json()
        except Exception as e:
            print(f"🔥 Error fetching page {page}: {e}")
            break

        docs = data.get("results", [])
        print(f"📦 Documents on this page: {len(docs)}")
        if not docs:
            print("📭 No more results.")
            break

        for i, doc in enumerate(docs, start=1):
            url = doc.get("html_url")
            print(f"   ↳ [{i}] {doc.get('title')}")
            description = fetch_document_text(url) if url else ""
            all_docs.append({
                "Title": doc.get("title"),
                "URL": url,
                "Publication Date": doc.get("publication_date"),
                "Type": doc.get("document_type"),
                "President": doc.get("president"),
                "Agencies": ", ".join(a.get("name", "") for a in doc.get("agencies", [])),
                "Description": description
            })

        if page >= data.get("total_pages", 1):
            break

        page += 1
        time.sleep(1)  # Be polite to the API

    return all_docs

def main():
    print("⚙️  Script started.")
    docs = fetch_documents()
    print(f"✅ Total documents retrieved: {len(docs)}")
    if not docs:
        print("❗No data saved. No documents matched the query.")
        return

    df = pd.DataFrame(docs)

    # Create timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"federal_register_docs_{timestamp}.tsv"

    # Save TSV in same folder as script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, filename)
    df.to_csv(output_path, index=False, encoding="utf-8", sep='\t')
    print(f"📁 Saved {len(df)} records to {output_path}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"🔥 Script failed with error: {e}")


