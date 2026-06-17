import datetime as dt
import json
import re
import sys
import requests

SITEMAP_INDEX_URL = "https://www.acquisition.gov/sitemap.xml"
DAYS_BACK = 62


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def fetch_text(url: str) -> str:
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as exc:
        eprint(f"Failed to fetch {url}: {exc}")
        return ""


def parse_sitemap_index(xml_text: str) -> List[str]:
    return re.findall(r"<loc>(https://www\.acquisition\.gov/sitemap\.xml\?page=\d+)</loc>", xml_text)


def parse_sitemap_urls(xml_text: str) -> List[tuple[str, str]]:
    entries = re.findall(r"<url><loc>(.*?)</loc><lastmod>(.*?)</lastmod>", xml_text)
    return [(loc, lastmod) for loc, lastmod in entries if loc and lastmod]


def is_far_case_url(url: str) -> bool:
    lowered = url.lower()
    return (
        "far-case" in lowered
        or "gsar-case" in lowered
        or "fars-case" in lowered
        or ("/archives/change-" in lowered and "-case-" in lowered)
    )


def build_citation(url: str) -> str:
    lowered = url.lower()
    m = re.search(r"far-case-(\d{4}-\d{3})", lowered)
    if m:
        return f"FAR Case {m.group(1)}"

    m = re.search(r"gsar-case-([\w-]+)", lowered)
    if m:
        return f"GSAR Case {m.group(1).upper()}"

    m = re.search(r"/archives/change-(\d+)-", lowered)
    if m:
        return f"Change {m.group(1)}"

    return ""


def build_title(url: str) -> str:
    slug = url.rstrip("/").split("/")[-1]
    if not slug:
        return "FAR case activity"
    return slug.replace("-", " ").strip().title()


def normalize_item(url: str, lastmod: str) -> Dict[str, str]:
    publication_date = (lastmod or "")[:10]
    citation = build_citation(url)
    title = build_title(url)

    return {
        "citation": citation,
        "title": title,
        "document_type": "FAR Case Activity",
        "publication_date": publication_date,
        "url": url,
    }


def fetch_far_changes() -> List[Dict[str, str]]:
    today = dt.date.today()
    start_date = today - dt.timedelta(days=DAYS_BACK)

    index_xml = fetch_text(SITEMAP_INDEX_URL)
    if not index_xml:
        return []

    sitemap_pages = parse_sitemap_index(index_xml)
    if not sitemap_pages:
        eprint("Failed to locate acquisition.gov sitemap pages for FAR case activity.")
        return []

    output: List[Dict[str, str]] = []
    seen: set[Tuple[str, str]] = set()

    for page_url in sitemap_pages:
        sitemap_xml = fetch_text(page_url)
        if not sitemap_xml:
            return []

        for url, lastmod in parse_sitemap_urls(sitemap_xml):
            if not is_far_case_url(url):
                continue

            publication_date = None
            try:
                publication_date = dt.date.fromisoformat((lastmod or "")[:10])
            except ValueError:
                continue

            if not (start_date <= publication_date <= today):
                continue

            item = normalize_item(url, lastmod)
            key = (item["title"], item["url"])
            if key in seen:
                continue
            seen.add(key)
            output.append(item)

    output.sort(key=lambda x: x["publication_date"], reverse=True)
    return output


def main() -> None:
    records = fetch_far_changes()
    print(json.dumps(records, indent=2))


if __name__ == "__main__":
    main()
