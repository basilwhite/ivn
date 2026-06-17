import datetime as dt
import json
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

PRIMARY_API_URL = "https://www.ecfr.gov/api/admin/v1/changes.json"
FALLBACK_API_URL = "https://www.ecfr.gov/api/versioner/v1/versions/title-48.json"
DAYS_BACK = 62


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def parse_date(value: Any) -> Optional[dt.date]:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    # Accept ISO dates and ISO date-times.
    text = text[:10]
    try:
        return dt.date.fromisoformat(text)
    except ValueError:
        return None


def normalize_record(item: Dict[str, Any]) -> Optional[Dict[str, str]]:
    publication_date = (
        parse_date(item.get("date"))
        or parse_date(item.get("amendment_date"))
        or parse_date(item.get("issue_date"))
    )
    if publication_date is None:
        return None

    citation = (
        item.get("identifier")
        or item.get("citation")
        or item.get("part")
        or ""
    )

    title = (
        item.get("name")
        or item.get("title")
        or item.get("identifier")
        or "eCFR change"
    )

    doc_type = str(item.get("type") or "section")
    title_no = str(item.get("title") or "48")
    part = str(item.get("part") or "").strip()

    if part:
        url = f"https://www.ecfr.gov/current/title-{title_no}/part-{part}"
    else:
        url = f"https://www.ecfr.gov/current/title-{title_no}"

    return {
        "citation": str(citation),
        "title": str(title),
        "document_type": f"eCFR {doc_type.title()}",
        "publication_date": publication_date.isoformat(),
        "url": url,
    }


def fetch_changes() -> List[Dict[str, str]]:
    today = dt.date.today()
    start_date = today - dt.timedelta(days=DAYS_BACK)

    output: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str, str, str]] = set()

    # Try the requested endpoint first and gracefully continue to the working endpoint.
    try:
        test_resp = requests.get(PRIMARY_API_URL, params={"date": today.isoformat()}, timeout=20)
        test_resp.raise_for_status()
    except requests.RequestException as exc:
        eprint(f"Primary eCFR changes endpoint unavailable, using versioner endpoint: {exc}")

    params = {
        "date[gte]": start_date.isoformat(),
        "date[lte]": today.isoformat(),
    }
    try:
        resp = requests.get(FALLBACK_API_URL, params=params, timeout=45)
        resp.raise_for_status()
    except requests.RequestException as exc:
        eprint(f"Failed to fetch eCFR version data: {exc}")
        return []

    payload = resp.json()
    records = payload.get("content_versions") or []
    for raw_item in records:
        normalized = normalize_record(raw_item)
        if not normalized:
            continue

        try:
            publication_date = dt.date.fromisoformat(normalized["publication_date"])
        except ValueError:
            continue

        if not (start_date <= publication_date <= today):
            continue

        dedupe_key = (
            normalized["citation"],
            normalized["title"],
            normalized["publication_date"],
            normalized["url"],
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        output.append(normalized)

    output.sort(key=lambda x: x["publication_date"], reverse=True)
    return output


def main() -> None:
    records = fetch_changes()
    print(json.dumps(records, indent=2))


if __name__ == "__main__":
    main()
