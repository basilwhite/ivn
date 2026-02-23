# omb_memo_scraper.py
# Last updated: 2025-05-08 20:00 UTC

import requests
from bs4 import BeautifulSoup
import csv
import re

URL = "https://www.whitehouse.gov/omb/information-resources/guidance/memoranda/"
OUTPUT_FILE = "OMB_Memos_Dependent_Components.csv"

def get_memos():
    response = requests.get(URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all("a", href=True)

    memos = []

    for link in links:
        title = link.text.strip()
        href = link["href"]

        # Skip non-PDF or non-memo links
        if not href.startswith("https://www.whitehouse.gov") or not title:
            continue

        # Try to extract memo number
        match = re.search(r"(M-\d{2}-\d{2})", title)
        if match:
            source = f"OMB {match.group(1)}"
        else:
            year_match = re.search(r"(20\d{2})", title)
            year = year_match.group(1) if year_match else "unknown"
            source = f"OMB (undesignated, {year})"

        component = title
        description = f"{title} – guidance for Federal agencies." # Basic default description

        memos.append({
            "Dependent Source": source,
            "Dependent Component": component,
            "Dependent Component Description": description,
            "Dependent Component URL": href
        })

    return sorted(memos, key=lambda x: x["Dependent Source"])

def write_csv(memos):
    headers = [
        "Dependent Source",
        "Dependent Component",
        "Dependent Component Description",
        "Dependent Component URL"
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(memos)

if __name__ == "__main__":
    memos = get_memos()
    write_csv(memos)
    print(f"Saved {len(memos)} memos to {OUTPUT_FILE}")
