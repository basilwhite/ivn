import datetime as dt
import json
import sys
from typing import Any, Dict, List

import requests

API_URL = "https://www.federalregister.gov/api/v1/documents.json"
DAYS_BACK = 62


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def in_range(date_str: str, start_date: dt.date, end_date: dt.date) -> bool:
    try:
        d = dt.date.fromisoformat(date_str[:10])
    except Exception:
        return False
    return start_date <= d <= end_date


def normalize_record(item: Dict[str, Any]) -> Dict[str, str]:
    citation = item.get("citation") or item.get("document_number") or ""
    title = item.get("title") or ""
    document_type = item.get("type") or item.get("document_type") or ""
    publication_date = (item.get("publication_date") or "")[:10]
    url = item.get("html_url") or item.get("pdf_url") or ""
    return {
        "citation": citation,
        "title": title,
        "document_type": document_type,
        "publication_date": publication_date,
        "url": url,
    }


def keep_item(item: Dict[str, Any]) -> bool:
    title = (item.get("title") or "").lower()
    doc_type = (item.get("type") or "").upper()

    if doc_type == "PRESDOCU":
        if "executive order" in title or item.get("executive_order_number"):
            return True

    if doc_type == "NOTICE":
        if "omb" in title or "memorandum" in title:
            return True

    return False


def fetch_documents() -> List[Dict[str, str]]:
    today = dt.date.today()
    start_date = today - dt.timedelta(days=DAYS_BACK)

    page = 1
    results: List[Dict[str, str]] = []

    while True:
        params = {
            "conditions[type][]": ["PRESDOCU", "NOTICE"],
            "conditions[publication_date][gte]": start_date.isoformat(),
            "conditions[publication_date][lte]": today.isoformat(),
            "order": "newest",
            "per_page": 100,
            "page": page,
        }

        try:
            resp = requests.get(API_URL, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            eprint(f"Failed to fetch Federal Register data: {exc}")
            return []

        payload = resp.json()
        docs = payload.get("results", [])
        if not docs:
            break

        for item in docs:
            publication_date = (item.get("publication_date") or "")[:10]
            if not in_range(publication_date, start_date, today):
                continue
            if not keep_item(item):
                continue
            results.append(normalize_record(item))

        page += 1

    return results


def main() -> None:
    records = fetch_documents()
    print(json.dumps(records, indent=2))


if __name__ == "__main__":
    main()
