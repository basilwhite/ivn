"""
ivn_governance_toolkit.py

Automation toolkit for the IVN governance workflow.

Goals covered:
1. Audit IVN source freshness.
2. Discover candidate updated .gov governance sources.
3. Break a new source document into components.
4. Learn traits from Alignments tab.
5. Learn traits from Nonaligned-Edge-Cases tab.
6. Propose alignments for new components against IVN Components.
7. Generate a leadership-ready executive report.
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus, urlparse

import pandas as pd
import requests


STOP_WORDS = {
    "the", "and", "for", "with", "from", "that", "this", "into", "under", "over", "about",
    "agency", "federal", "government", "shall", "must", "will", "their", "there", "where",
    "component", "components", "requirements", "requirement", "policy", "program", "office",
}


@dataclass
class TraitModel:
    positive_terms: list[str]
    negative_terms: list[str]
    threshold: float


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H%M")


def ask_path(prompt: str, must_exist: bool = True) -> Path:
    while True:
        raw = input(prompt).strip().strip('"')
        p = Path(raw)
        if must_exist and not p.exists():
            print(f"Path not found: {p}")
            continue
        return p


def read_excel_case_insensitive(xls: pd.ExcelFile, preferred_names: list[str]) -> pd.DataFrame:
    lower_map = {sheet.lower(): sheet for sheet in xls.sheet_names}
    for name in preferred_names:
        sheet = lower_map.get(name.lower())
        if sheet:
            return pd.read_excel(xls, sheet_name=sheet)
    raise ValueError(f"Missing expected sheet names: {preferred_names}; found: {xls.sheet_names}")


def find_column(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    normalized = {re.sub(r"\s+", "", c.lower()): c for c in df.columns}
    for candidate in candidates:
        key = re.sub(r"\s+", "", candidate.lower())
        if key in normalized:
            return normalized[key]

    # Fuzzy contains fallback
    for candidate in candidates:
        c_norm = re.sub(r"\s+", "", candidate.lower())
        for col in df.columns:
            col_norm = re.sub(r"\s+", "", str(col).lower())
            if c_norm in col_norm or col_norm in c_norm:
                return col
    return None


def tokenize(text: str) -> list[str]:
    if not isinstance(text, str):
        return []
    terms = re.findall(r"[a-z]{3,}", text.lower())
    return [t for t in terms if t not in STOP_WORDS]


def normalize_url(url: str) -> str:
    if not isinstance(url, str):
        return ""
    return url.strip()


def url_is_gov(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    return host.endswith(".gov") or ".gov." in host


def infer_text_columns(df: pd.DataFrame) -> tuple[str, str] | None:
    desc_cols = [c for c in df.columns if "description" in str(c).lower()]
    comp_cols = [c for c in df.columns if "component" in str(c).lower()]

    if len(desc_cols) >= 2:
        return desc_cols[0], desc_cols[1]
    if len(comp_cols) >= 2:
        return comp_cols[0], comp_cols[1]

    text_like = [
        c for c in df.columns
        if pd.api.types.is_string_dtype(df[c])
        and df[c].astype(str).str.len().mean() > 10
    ]
    if len(text_like) >= 2:
        return text_like[0], text_like[1]
    return None


def check_ivn_sources_for_updates(ivn_path: Path) -> Path:
    print("\nChecking IVN sources for updates...")
    xls = pd.ExcelFile(ivn_path)
    sources = read_excel_case_insensitive(xls, ["Sources"])

    source_col = find_column(sources, ["Source", "Title", "Document", "Name"])
    url_col = find_column(sources, ["Source URL", "URL", "Link", "Document URL"])
    if not source_col or not url_col:
        raise ValueError("Sources sheet must include a source/title column and a URL column.")

    rows: list[dict] = []
    session = requests.Session()
    session.headers.update({"User-Agent": "IVN Governance Toolkit/1.0"})

    for _, row in sources.iterrows():
        source_name = str(row.get(source_col, "")).strip()
        url = normalize_url(str(row.get(url_col, "")))
        status = "unknown"
        final_url = ""
        last_modified = ""
        etag = ""
        error = ""

        if not url:
            status = "missing_url"
        else:
            try:
                resp = session.head(url, timeout=12, allow_redirects=True)
                if resp.status_code >= 400 or resp.status_code == 405:
                    # Some sources block HEAD; retry with lightweight GET.
                    resp = session.get(url, timeout=20, allow_redirects=True, stream=True)
                final_url = resp.url
                status = f"http_{resp.status_code}"
                last_modified = resp.headers.get("Last-Modified", "")
                etag = resp.headers.get("ETag", "")
            except Exception as ex:
                status = "error"
                error = str(ex)

        outdated_signal = (
            status in {"error", "http_404", "http_410", "http_500", "http_503"}
            or (final_url and final_url != url)
        )

        rows.append({
            "Source": source_name,
            "URL": url,
            "Status": status,
            "Final URL": final_url,
            "Last-Modified": last_modified,
            "ETag": etag,
            "Potentially Outdated": outdated_signal,
            "Error": error,
        })

    out = Path.cwd() / f"ivn_source_freshness_audit_{now_stamp()}.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"Source freshness audit saved: {out}")
    return out


def search_duckduckgo(query: str, max_results: int = 10) -> list[tuple[str, str]]:
    # DuckDuckGo HTML endpoint returns anchor tags we can parse with regex.
    search_url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(search_url, headers=headers, timeout=25)
    resp.raise_for_status()
    html = resp.text

    matches = re.findall(r'<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', html, re.IGNORECASE)
    cleaned: list[tuple[str, str]] = []
    for href, title_html in matches:
        title = re.sub(r"<.*?>", "", title_html)
        title = re.sub(r"\s+", " ", title).strip()
        cleaned.append((href, title))
        if len(cleaned) >= max_results:
            break
    return cleaned


def discover_updated_gov_sources(ivn_path: Path, max_queries: int = 12) -> Path:
    print("\nSearching for candidate updated .gov governance sources...")

    xls = pd.ExcelFile(ivn_path)
    sources = read_excel_case_insensitive(xls, ["Sources"])

    source_col = find_column(sources, ["Source", "Title", "Document", "Name"])
    url_col = find_column(sources, ["Source URL", "URL", "Link", "Document URL"])
    if not source_col:
        raise ValueError("Could not find source/title column in Sources sheet.")

    known_urls = set()
    if url_col:
        known_urls = {normalize_url(str(u)) for u in sources[url_col].dropna().astype(str).tolist()}

    keywords: list[str] = []
    for value in sources[source_col].dropna().astype(str).head(max_queries * 2):
        terms = [t for t in tokenize(value) if len(t) >= 5]
        if terms:
            keywords.append(" ".join(terms[:4]))
        if len(keywords) >= max_queries:
            break

    results: list[dict] = []
    seen = set()
    for kw in keywords:
        query = f"site:.gov \"{kw}\" federal policy"
        try:
            hits = search_duckduckgo(query)
        except Exception as ex:
            results.append({
                "Query": query,
                "Candidate Title": "",
                "Candidate URL": "",
                "Reason": f"search_error: {ex}",
                "Is .gov": False,
                "Already In Sources": False,
            })
            continue

        for url, title in hits:
            url = normalize_url(url)
            gov = url_is_gov(url)
            already_known = url in known_urls
            key = (url, query)
            if key in seen:
                continue
            seen.add(key)
            if gov and not already_known:
                reason = "new_gov_candidate_similar_to_existing_sources"
            elif gov and already_known:
                reason = "already_in_sources"
            else:
                reason = "non_gov_filtered"

            results.append({
                "Query": query,
                "Candidate Title": title,
                "Candidate URL": url,
                "Reason": reason,
                "Is .gov": gov,
                "Already In Sources": already_known,
            })

    out = Path.cwd() / f"ivn_gov_source_candidates_{now_stamp()}.csv"
    pd.DataFrame(results).to_csv(out, index=False)
    print(f".gov source candidate table saved: {out}")
    return out


def ingest_and_analyze_new_document() -> None:
    print("\nIngest and analyze a new governance document...")
    try:
        from ivn_extract_components_from_pdf import run_pipeline
    except Exception as ex:
        print(f"Unable to import run_pipeline from ivn_extract_components_from_pdf.py: {ex}")
        return

    pdf_path = ask_path("Enter path to the new source PDF: ")
    component_url = input("Enter authoritative source URL for this PDF: ").strip()
    source_title = input("Optional source title override (press Enter to auto-detect): ").strip() or None

    _, _, output_file = run_pipeline(pdf_path, component_url, source_title)
    print(f"Component extraction complete: {output_file}")


def learn_alignment_traits(ivn_path: Path) -> tuple[TraitModel, Path]:
    print("\nLearning alignment and nonalignment traits from IVN dataset...")
    xls = pd.ExcelFile(ivn_path)

    aligns = read_excel_case_insensitive(xls, ["Alignments"])
    nonaligns = read_excel_case_insensitive(xls, ["Nonaligned-Edge-Cases", "Nonaligned Edge Cases", "Nonaligned"])

    cols_a = infer_text_columns(aligns)
    cols_n = infer_text_columns(nonaligns)
    if not cols_a or not cols_n:
        raise ValueError("Could not infer paired text columns in Alignments and Nonaligned sheets.")

    pos_counter: Counter[str] = Counter()
    neg_counter: Counter[str] = Counter()

    for _, row in aligns.iterrows():
        text = f"{row.get(cols_a[0], '')} {row.get(cols_a[1], '')}"
        pos_counter.update(tokenize(text))

    for _, row in nonaligns.iterrows():
        text = f"{row.get(cols_n[0], '')} {row.get(cols_n[1], '')}"
        neg_counter.update(tokenize(text))

    # Terms with the strongest differential signal.
    differential = Counter()
    all_terms = set(pos_counter.keys()) | set(neg_counter.keys())
    for term in all_terms:
        differential[term] = pos_counter.get(term, 0) - neg_counter.get(term, 0)

    positive_terms = [t for t, score in differential.most_common(80) if score > 0][:40]
    negative_terms = [t for t, score in differential.most_common() if score < 0][-40:]
    negative_terms = list(reversed([t for t in negative_terms]))

    model = TraitModel(positive_terms=positive_terms, negative_terms=negative_terms, threshold=0.35)

    out = Path.cwd() / f"ivn_alignment_trait_model_{now_stamp()}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump({
            "positive_terms": model.positive_terms,
            "negative_terms": model.negative_terms,
            "threshold": model.threshold,
            "learned_at": datetime.now().isoformat(timespec="seconds"),
            "sheet_info": {
                "alignments_columns": cols_a,
                "nonaligned_columns": cols_n,
            },
        }, f, indent=2)

    print(f"Trait model saved: {out}")
    return model, out


def load_trait_model(path: Path) -> TraitModel:
    data = json.loads(path.read_text(encoding="utf-8"))
    return TraitModel(
        positive_terms=list(data.get("positive_terms", [])),
        negative_terms=list(data.get("negative_terms", [])),
        threshold=float(data.get("threshold", 0.35)),
    )


def jaccard_similarity(a: str, b: str) -> float:
    ta = set(tokenize(a))
    tb = set(tokenize(b))
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return inter / union if union else 0.0


def trait_signal_score(text_a: str, text_b: str, model: TraitModel) -> float:
    terms = set(tokenize(text_a)) & set(tokenize(text_b))
    if not terms:
        return 0.0
    pos_hits = sum(1 for t in terms if t in model.positive_terms)
    neg_hits = sum(1 for t in terms if t in model.negative_terms)
    raw = pos_hits - neg_hits
    # Map roughly into [0,1].
    return max(0.0, min(1.0, (raw + 5.0) / 10.0))


def pick_component_description_column(df: pd.DataFrame) -> str:
    col = find_column(df, ["Component Description", "Description", "Text"])
    if col:
        return col

    candidates = [c for c in df.columns if pd.api.types.is_string_dtype(df[c])]
    if not candidates:
        raise ValueError("No text-like column available for semantic matching.")
    # Prefer longest average text length.
    best = max(candidates, key=lambda c: df[c].astype(str).str.len().mean())
    return best


def find_new_alignments(new_components_path: Path, ivn_path: Path, model: TraitModel, top_k: int = 3) -> Path:
    print("\nFinding alignments for new components...")

    new_xls = pd.ExcelFile(new_components_path)
    new_sheet = new_xls.sheet_names[0]
    new_df = pd.read_excel(new_xls, sheet_name=new_sheet)

    ivn_xls = pd.ExcelFile(ivn_path)
    components_df = read_excel_case_insensitive(ivn_xls, ["Components"])

    new_comp_col = find_column(new_df, ["Component", "Component Name", "Title"]) or new_df.columns[0]
    new_desc_col = pick_component_description_column(new_df)
    new_url_col = find_column(new_df, ["Component URL", "URL", "Link"]) or ""

    ivn_comp_col = find_column(components_df, ["Component", "Component Name", "Title"]) or components_df.columns[0]
    ivn_desc_col = pick_component_description_column(components_df)
    ivn_url_col = find_column(components_df, ["Component URL", "URL", "Link"]) or ""

    rows: list[dict] = []
    for _, new_row in new_df.iterrows():
        new_component = str(new_row.get(new_comp_col, ""))
        new_desc = str(new_row.get(new_desc_col, ""))
        if not new_desc.strip():
            continue

        scored = []
        for _, ivn_row in components_df.iterrows():
            ivn_component = str(ivn_row.get(ivn_comp_col, ""))
            ivn_desc = str(ivn_row.get(ivn_desc_col, ""))
            if not ivn_desc.strip():
                continue

            sem = jaccard_similarity(new_desc, ivn_desc)
            trait = trait_signal_score(new_desc, ivn_desc, model)
            final = 0.7 * sem + 0.3 * trait
            scored.append((final, sem, trait, ivn_component, ivn_desc, str(ivn_row.get(ivn_url_col, ""))))

        scored.sort(key=lambda x: x[0], reverse=True)
        for final, sem, trait, ivn_component, ivn_desc, ivn_url in scored[:top_k]:
            if final < model.threshold:
                continue
            rows.append({
                "New Component": new_component,
                "New Component Description": new_desc,
                "New Component URL": str(new_row.get(new_url_col, "")) if new_url_col else "",
                "Aligned IVN Component": ivn_component,
                "Aligned IVN Description": ivn_desc,
                "Aligned IVN URL": ivn_url,
                "Semantic Score": round(sem, 4),
                "Trait Score": round(trait, 4),
                "Final Alignment Score": round(final, 4),
            })

    out = Path.cwd() / f"ivn_new_alignments_{now_stamp()}.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"Alignment table saved: {out}")
    return out


def generate_executive_report(
    ivn_path: Path,
    source_audit_path: Path,
    source_candidates_path: Path,
    new_components_path: Path,
    alignments_path: Path,
) -> Path:
    print("\nGenerating executive report...")

    audit_df = pd.read_csv(source_audit_path)
    cand_df = pd.read_csv(source_candidates_path)
    align_df = pd.read_csv(alignments_path)

    new_xls = pd.ExcelFile(new_components_path)
    new_df = pd.read_excel(new_xls, sheet_name=new_xls.sheet_names[0])

    potentially_outdated = int(audit_df.get("Potentially Outdated", pd.Series(dtype=bool)).fillna(False).sum())
    total_sources = len(audit_df)
    new_gov_candidates = len(cand_df[(cand_df.get("Is .gov", False) == True) & (cand_df.get("Already In Sources", False) == False)])
    new_components_count = len(new_df)
    alignments_count = len(align_df)

    high_conf = 0
    if "Final Alignment Score" in align_df.columns:
        high_conf = int((align_df["Final Alignment Score"] >= 0.55).sum())

    report = []
    report.append("# IVN Governance Executive Report")
    report.append("")
    report.append(f"Report timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"IVN dataset path: {ivn_path}")
    report.append("")
    report.append("## Executive Summary")
    report.append(f"- Sources reviewed: {total_sources}")
    report.append(f"- Potentially outdated sources: {potentially_outdated}")
    report.append(f"- Candidate new .gov sources found: {new_gov_candidates}")
    report.append(f"- New source components identified: {new_components_count}")
    report.append(f"- Proposed component alignments: {alignments_count}")
    report.append(f"- High-confidence alignments (score >= 0.55): {high_conf}")
    report.append("")

    report.append("## Leadership Actions")
    report.append("1. Prioritize remediation for sources flagged as potentially outdated and assign source owners.")
    report.append("2. Triage new .gov candidates for authority, recency, and mission relevance before intake.")
    report.append("3. Validate high-confidence alignments first and publish a reviewed crosswalk for program leads.")
    report.append("4. For lower-confidence alignments, route to SMEs and capture adjudication outcomes for model tuning.")
    report.append("5. Communicate compliance narrative by mapping each IVN component to authoritative language in new sources.")
    report.append("")

    report.append("## Delivery Guidance")
    report.append("- Make source-to-component traceability visible in governance dashboards.")
    report.append("- Update implementation playbooks to explicitly reference newly aligned obligations.")
    report.append("- Include measurable milestones and owners for each newly aligned component.")
    report.append("- Review this report monthly and compare trend lines for source freshness and alignment confidence.")

    out = Path.cwd() / f"ivn_executive_report_{now_stamp()}.md"
    out.write_text("\n".join(report), encoding="utf-8")
    print(f"Executive report saved: {out}")
    return out


def run_end_to_end() -> None:
    print("\nRun end-to-end IVN governance automation...")
    ivn_path = ask_path("Enter full path to USDA-IVN-dataset.xlsx: ")

    source_audit = check_ivn_sources_for_updates(ivn_path)
    source_candidates = discover_updated_gov_sources(ivn_path)

    print("\nStep 3: Extract components from a new source document.")
    ingest_and_analyze_new_document()

    trait_model, trait_model_path = learn_alignment_traits(ivn_path)

    new_components_path = ask_path("Enter path to extracted new-components Excel file: ")
    alignments_path = find_new_alignments(new_components_path, ivn_path, trait_model)

    report_path = generate_executive_report(
        ivn_path=ivn_path,
        source_audit_path=source_audit,
        source_candidates_path=source_candidates,
        new_components_path=new_components_path,
        alignments_path=alignments_path,
    )

    print("\nPipeline complete.")
    print(f"Source audit: {source_audit}")
    print(f".gov candidates: {source_candidates}")
    print(f"Trait model: {trait_model_path}")
    print(f"Alignments: {alignments_path}")
    print(f"Executive report: {report_path}")


def main_menu() -> str:
    print("\nIVN Governance Toolkit - Main Menu")
    print("[1] Check IVN sources for updates")
    print("[2] Discover candidate updated .gov sources")
    print("[3] Ingest and analyze a new governance document")
    print("[4] Learn alignment/nonalignment traits from IVN dataset")
    print("[5] Find new alignments for a document")
    print("[6] Generate executive report")
    print("[7] Run end-to-end automation")
    print("[0] Exit")
    return input("Select an option: ").strip()


def main() -> None:
    trait_model_cache: TraitModel | None = None

    while True:
        choice = main_menu()
        try:
            if choice == "1":
                ivn_path = ask_path("Enter full path to USDA-IVN-dataset.xlsx: ")
                check_ivn_sources_for_updates(ivn_path)
            elif choice == "2":
                ivn_path = ask_path("Enter full path to USDA-IVN-dataset.xlsx: ")
                discover_updated_gov_sources(ivn_path)
            elif choice == "3":
                ingest_and_analyze_new_document()
            elif choice == "4":
                ivn_path = ask_path("Enter full path to USDA-IVN-dataset.xlsx: ")
                trait_model_cache, _ = learn_alignment_traits(ivn_path)
            elif choice == "5":
                ivn_path = ask_path("Enter full path to USDA-IVN-dataset.xlsx: ")
                new_components_path = ask_path("Enter path to extracted new-components Excel file: ")

                reuse = "n"
                if trait_model_cache is not None:
                    reuse = input("Reuse in-memory trait model from this session? [Y/n]: ").strip().lower() or "y"

                if trait_model_cache is not None and reuse in {"y", "yes"}:
                    model = trait_model_cache
                else:
                    model_mode = input("Load existing trait model JSON file? [y/N]: ").strip().lower() or "n"
                    if model_mode in {"y", "yes"}:
                        model_path = ask_path("Enter trait model JSON path: ")
                        model = load_trait_model(model_path)
                    else:
                        model, _ = learn_alignment_traits(ivn_path)
                        trait_model_cache = model

                find_new_alignments(new_components_path, ivn_path, model)
            elif choice == "6":
                ivn_path = ask_path("Enter full path to USDA-IVN-dataset.xlsx: ")
                source_audit_path = ask_path("Enter path to source audit CSV: ")
                source_candidates_path = ask_path("Enter path to source candidate CSV: ")
                new_components_path = ask_path("Enter path to extracted new-components Excel file: ")
                alignments_path = ask_path("Enter path to alignments CSV: ")
                generate_executive_report(
                    ivn_path,
                    source_audit_path,
                    source_candidates_path,
                    new_components_path,
                    alignments_path,
                )
            elif choice == "7":
                run_end_to_end()
            elif choice == "0":
                print("Exiting.")
                sys.exit(0)
            else:
                print("Invalid choice. Please select a valid option.")
        except Exception as ex:
            print(f"Operation failed: {ex}")


if __name__ == "__main__":
    main()
