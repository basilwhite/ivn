import datetime as dt
import json
import sys
from typing import Any, Dict, List, Optional

import requests

API_KEY = "YOUR_CONGRESS_GOV_API_KEY"
API_URL = "https://api.congress.gov/v3/law/119"
DAYS_BACK = 62


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def parse_date(value: Any) -> Optional[dt.date]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text[:10]
    try:
        return dt.date.fromisoformat(text)
    except ValueError:
        return None


def normalize_record(item: Dict[str, Any]) -> Dict[str, str]:
    congress = str(item.get("congress") or "119")
    number = str(item.get("number") or item.get("lawNumber") or "")
    citation = f"Pub. L. {congress}-{number}" if number else ""

    title = item.get("title") or item.get("shortTitle") or ""

    action = item.get("latestAction") or {}
    publication_date = (
        item.get("lawDate")
        or item.get("enactedDate")
        or action.get("actionDate")
        or item.get("updateDate")
        or ""
    )

    url = item.get("url") or ""

    return {
        "citation": citation,
        "title": str(title),
        "document_type": "Public Law",
        "publication_date": str(publication_date)[:10],
        "url": str(url),
    }


def fetch_public_laws() -> List[Dict[str, str]]:
    if not API_KEY or API_KEY == "YOUR_CONGRESS_GOV_API_KEY":
        eprint("Congress.gov API key is required. Set API_KEY in this script.")
        return []

    today = dt.date.today()
    start_date = today - dt.timedelta(days=DAYS_BACK)

    all_items: List[Dict[str, str]] = []
    offset = 0
    limit = 250

    while True:
        params = {
            "api_key": API_KEY,
            "format": "json",
            "limit": limit,
            "offset": offset,
        }

        try:
            resp = requests.get(API_URL, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            eprint(f"Failed to fetch Congress.gov laws: {exc}")
            return []

        payload = resp.json()
        laws = payload.get("laws", [])
        if not laws:
            break

        for law in laws:
            record = normalize_record(law)
            pub_date = parse_date(record["publication_date"])
            if not pub_date:
                continue
            if not (start_date <= pub_date <= today):
                continue
            all_items.append(record)

        pagination = payload.get("pagination") or {}
        next_url = pagination.get("next")
        if not next_url:
            break
        offset += limit

    all_items.sort(key=lambda x: x["publication_date"], reverse=True)
    return all_items


def main() -> None:
    records = fetch_public_laws()
    print(json.dumps(records, indent=2))


if __name__ == "__main__":
    main()
