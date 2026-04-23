#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from pyxlsb import open_workbook

ALIGNMENT_HEADERS = [
    "Enabling Component ID",
    "Enabling Component Name",
    "Probable Dependent Component",
    "Dependent Source",
    "Relationship Basis",
    "Suggested IVN Sheet",
    "Suggested Confidence",
]

DEFAULT_URL = "https://www.whitehouse.gov/presidential-actions/2025/11/launching-the-genesis-mission/"
DEFAULT_XLSB = "IVN-dataset.xlsb"
DEFAULT_OUT_DIR = "."


@dataclass
class GovernanceClause:
    clause_id: str
    text: str
    citations: list[str]


@dataclass
class IvnComponent:
    component_id: str
    component_name: str
    component_description: str
    source_id: str


@dataclass
class MatchResult:
    clause: GovernanceClause
    component: IvnComponent
    score: float
    basis: str


def utc_filename_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%MUTC")


def fetch_page_text(url: str) -> tuple[str, str]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; IVN-AlignmentBot/1.0)",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        html_bytes = response.read()
    html = html_bytes.decode("utf-8", errors="replace")

    title_match = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    title = " ".join((title_match.group(1) if title_match else "Untitled").split())

    # Remove script/style blocks and then strip tags.
    cleaned = re.sub(r"<script\b[^>]*>.*?</script>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<style\b[^>]*>.*?</style>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<noscript\b[^>]*>.*?</noscript>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = cleaned.replace("\xa0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return title, cleaned


def split_sentences(text: str) -> list[str]:
    rough = re.split(r"(?<=[.!?])\s+", text)
    sentences = []
    for sentence in rough:
        normalized = " ".join(sentence.split())
        if len(normalized) >= 40:
            sentences.append(normalized)
    return sentences


def extract_citations(text: str) -> list[str]:
    patterns = [
        r"\b\d+\s*U\.?\s*S\.?\s*C\.?\s*(?:section|sec\.?|§)?\s*[\w\-\.()]*",
        r"\bExecutive\s+Order\s+\d{4,6}\b",
        r"\bE\.?\s*O\.?\s*\d{4,6}\b",
        r"\bOMB\b[^.;,\n]{0,80}",
        r"\bCircular\s+[A-Z]-\d+(?:-\d+)?\b",
        r"\bM-\d{2}-\d{2}\b",
        r"\bFederal\s+Register\b[^.;,\n]{0,80}",
        r"\bPublic\s+Law\s+\d+-\d+\b",
    ]
    found: list[str] = []
    seen: set[str] = set()
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            citation = " ".join(match.group(0).split())
            key = citation.lower()
            if citation and key not in seen:
                seen.add(key)
                found.append(citation)
    return found


def extract_requirements_and_clauses(text: str, prefix: str) -> tuple[list[str], list[GovernanceClause], list[str]]:
    modal_pat = re.compile(r"\b(must|shall|required|deadline|effective|within\s+\d+\s+days|is\s+hereby|are\s+hereby)\b", re.IGNORECASE)
    sentences = split_sentences(text)

    requirement_lines: list[str] = []
    citation_lines: list[str] = []
    clauses: list[GovernanceClause] = []

    for sentence in sentences:
        citations = extract_citations(sentence)
        if citations:
            citation_lines.append(sentence)
        if modal_pat.search(sentence):
            requirement_lines.append(sentence)
            clause_id = f"{prefix}-{len(clauses) + 1:03d}"
            clauses.append(GovernanceClause(clause_id=clause_id, text=sentence, citations=citations))

    if not clauses:
        # Fallback: use top citation-bearing sentences if strict modal extraction finds none.
        fallback = [s for s in sentences if extract_citations(s)]
        for sentence in fallback[:25]:
            clause_id = f"{prefix}-{len(clauses) + 1:03d}"
            clauses.append(GovernanceClause(clause_id=clause_id, text=sentence, citations=extract_citations(sentence)))

    return requirement_lines, clauses, citation_lines


def normalize_tokens(text: str) -> list[str]:
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = [token for token in text.split() if len(token) > 2]
    stopwords = {
        "the",
        "and",
        "for",
        "with",
        "from",
        "that",
        "this",
        "into",
        "such",
        "shall",
        "must",
        "within",
        "will",
        "are",
        "is",
        "under",
        "subject",
        "section",
        "order",
        "mission",
        "launching",
    }
    return [token for token in tokens if token not in stopwords]


def jaccard_score(a_tokens: set[str], b_tokens: set[str]) -> float:
    if not a_tokens or not b_tokens:
        return 0.0
    intersection = len(a_tokens.intersection(b_tokens))
    union = len(a_tokens.union(b_tokens))
    return intersection / union if union else 0.0


def load_components_from_xlsb(xlsb_path: Path) -> list[IvnComponent]:
    components: list[IvnComponent] = []
    with open_workbook(str(xlsb_path)) as workbook:
        with workbook.get_sheet("Components") as sheet:
            row_iter = sheet.rows()
            header_row = next(row_iter, None)
            if header_row is None:
                return components

            headers = ["" if cell.v is None else str(cell.v).strip() for cell in header_row]
            index = {name: i for i, name in enumerate(headers)}

            required_columns = ["component_name", "component_description", "source_id", "component_id"]
            missing = [col for col in required_columns if col not in index]
            if missing:
                raise ValueError(f"Missing expected columns in Components sheet: {missing}")

            for row in row_iter:
                values = ["" if cell.v is None else str(cell.v).strip() for cell in row]

                def get(col: str) -> str:
                    i = index[col]
                    return values[i] if i < len(values) else ""

                component_name = get("component_name")
                component_description = get("component_description")
                source_id = get("source_id")
                component_id = get("component_id")

                if not component_name and not component_description:
                    continue

                components.append(
                    IvnComponent(
                        component_id=component_id or f"COMP-{len(components)+1}",
                        component_name=component_name or "Unnamed component",
                        component_description=component_description,
                        source_id=source_id or "Unknown source",
                    )
                )

    return components


def pick_best_matches(clauses: list[GovernanceClause], components: list[IvnComponent], threshold: float) -> list[MatchResult]:
    results: list[MatchResult] = []

    component_tokens: list[tuple[IvnComponent, set[str]]] = []
    for component in components:
        text = f"{component.component_name} {component.component_description} {component.source_id}"
        tokens = set(normalize_tokens(text))
        if tokens:
            component_tokens.append((component, tokens))

    for clause in clauses:
        clause_tokens = set(normalize_tokens(clause.text))
        if not clause_tokens:
            continue

        best_component: IvnComponent | None = None
        best_score = 0.0

        for component, c_tokens in component_tokens:
            score = jaccard_score(clause_tokens, c_tokens)
            if score > best_score:
                best_score = score
                best_component = component

        if best_component and best_score >= threshold:
            basis = "Keyword overlap between governance requirement and IVN component description"
            if clause.citations:
                basis += f"; citations detected: {', '.join(clause.citations[:3])}"
            results.append(MatchResult(clause=clause, component=best_component, score=best_score, basis=basis))

    return results


def write_alignment_csv(matches: list[MatchResult], out_path: Path, source_name: str) -> None:
    with out_path.open("w", encoding="ascii", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ALIGNMENT_HEADERS)
        writer.writeheader()

        for match in matches:
            enabling_name = match.clause.text
            if len(enabling_name) > 140:
                enabling_name = enabling_name[:137] + "..."

            writer.writerow(
                {
                    "Enabling Component ID": match.clause.clause_id,
                    "Enabling Component Name": ascii_clean(enabling_name),
                    "Probable Dependent Component": ascii_clean(match.component.component_name),
                    "Dependent Source": ascii_clean(match.component.source_id),
                    "Relationship Basis": ascii_clean(match.basis),
                    "Suggested IVN Sheet": "Inferred_Alignments",
                    "Suggested Confidence": f"{match.score:.2f}",
                }
            )


def ascii_clean(text: str) -> str:
    replacements = {
        "\u2013": "-",
        "\u2014": "-",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2026": "...",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return text.encode("ascii", errors="replace").decode("ascii")


def write_lines(path: Path, lines: Iterable[str]) -> None:
    path.write_text("\n".join(lines), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch a governance URL, extract requirement/citation clauses, match against IVN components, and write alignments CSV.")
    parser.add_argument("--url", default=DEFAULT_URL, help="Governance document URL")
    parser.add_argument("--xlsb", default=DEFAULT_XLSB, help="Path to IVN XLSB file")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Output directory")
    parser.add_argument("--min-score", type=float, default=0.06, help="Minimum Jaccard score threshold for emitting a match")
    parser.add_argument("--max-rows", type=int, default=50, help="Maximum number of alignment rows")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    xlsb_path = Path(args.xlsb).resolve()

    title, full_text = fetch_page_text(args.url)

    slug = re.sub(r"[^A-Za-z0-9]+", "-", args.url.rstrip("/").split("/")[-1]).strip("-")
    prefix = (slug or "URL").upper()[:18]

    requirement_lines, clauses, citation_lines = extract_requirements_and_clauses(full_text, prefix=prefix)

    extracted_document_path = out_dir / "extracted_document.txt"
    requirement_lines_path = out_dir / "requirement_lines.txt"
    citation_lines_path = out_dir / "citation_lines.txt"
    write_lines(extracted_document_path, [title, "", full_text])
    write_lines(requirement_lines_path, requirement_lines)
    write_lines(citation_lines_path, citation_lines)

    components = load_components_from_xlsb(xlsb_path)
    matches = pick_best_matches(clauses, components, threshold=args.min_score)
    matches = sorted(matches, key=lambda m: m.score, reverse=True)[: max(0, args.max_rows)]

    csv_path = out_dir / f"new_alignments_{utc_filename_stamp()}.csv"
    write_alignment_csv(matches, csv_path, source_name=title)

    payload = {
        "url": args.url,
        "document_title": title,
        "document_text_length": len(full_text),
        "requirements_detected": len(requirement_lines),
        "citations_detected": len(citation_lines),
        "clauses_modeled": len(clauses),
        "components_loaded": len(components),
        "alignment_rows_written": len(matches),
        "new_alignments_csv_path": str(csv_path),
        "requirement_lines_path": str(requirement_lines_path),
        "citation_lines_path": str(citation_lines_path),
        "extracted_document_path": str(extracted_document_path),
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
