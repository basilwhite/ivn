import datetime as dt
import json
import re
import sys
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

SOURCE_URLS = [
    "https://www.usda.gov/about-usda/policies-and-links/departmental-directives",
    "https://www.usda.gov/about-usda/policies-and-links/departmental-directives/directives-category",
    "https://www.usda.gov/about-usda/policies-and-links/departmental-directives/directive-status",
]
DAYS_BACK = 62

DATE_PATTERNS = [
    re.compile(r"\b(\d{4}-\d{2}-\d{2})\b"),
    re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b"),
    re.compile(
        r"\b("
        r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|"
        r"Nov(?:ember)?|Dec(?:ember)?"
        r")\s+\d{1,2},\s+\d{4}\b",
        re.IGNORECASE,
    ),
]


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def parse_date(value: str) -> Optional[dt.date]:
    value = (value or "").strip()
    if not value:
        return None

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    return None


def find_date(text: str) -> Optional[dt.date]:
    for pattern in DATE_PATTERNS:
        m = pattern.search(text or "")
        if not m:
            continue
        parsed = parse_date(m.group(0))
        if parsed:
            return parsed
    return None


def fetch_html(url: str, attempts: int = 1, timeout: int = 20) -> str:
    last_error: Optional[Exception] = None
    for _ in range(attempts):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            last_error = exc

    eprint(f"Failed to fetch USDA directives page {url}: {last_error}")
    return ""


def is_directive_like(text: str) -> bool:
    lowered = (text or "").lower()
    keywords = [
        "directive",
        "departmental regulation",
        "dr ",
        "secretary memorandum",
        "notice",
        "manual",
        "guidebook",
    ]
    return any(k in lowered for k in keywords)


def fetch_directives() -> List[Dict[str, str]]:
    today = dt.date.today()
    start_date = today - dt.timedelta(days=DAYS_BACK)

    output: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str]] = set()

    for source_url in SOURCE_URLS:
        html = fetch_html(source_url)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")

        for link in soup.select("a[href]"):
            href = link.get("href", "")
            title = " ".join(link.stripped_strings)
            if not href or not title:
                continue

            if len(title) < 5:
                continue

            container = link.find_parent(["article", "li", "tr", "div", "section"]) or link
            context_text = " ".join(container.stripped_strings)

            if not is_directive_like(f"{title} {context_text}"):
                continue

            publication_date = None
            time_tag = container.find("time") if hasattr(container, "find") else None
            if time_tag and time_tag.get("datetime"):
                publication_date = parse_date(time_tag.get("datetime", "")[:10])
            if publication_date is None:
                publication_date = find_date(context_text)

            if publication_date is None:
                continue
            if not (start_date <= publication_date <= today):
                continue

            full_url = urljoin(source_url, href)
            citation_match = re.search(
                r"\b(?:DR|Directive|Departmental\s+Regulation|Secretary\'?s\s+Memorandum)\s*[- ]?([A-Za-z0-9.-]+)?\b",
                title,
                flags=re.IGNORECASE,
            )
            citation = citation_match.group(0).strip() if citation_match else ""

            key = (title, full_url)
            if key in seen:
                continue
            seen.add(key)

            output.append(
                {
                    "citation": citation,
                    "title": title,
                    "document_type": "USDA Directive",
                    "publication_date": publication_date.isoformat(),
                    "url": full_url,
                }
            )

    output.sort(key=lambda x: x["publication_date"], reverse=True)
    return output


def main() -> None:
    records = fetch_directives()
    print(json.dumps(records, indent=2))


if __name__ == "__main__":
    main()
