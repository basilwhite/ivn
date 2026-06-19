"""Semantic crosswalk generator for Executive Order components against IVN components.

This module creates:
1) a raw top-k semantic alignment table,
2) a validated/filtered alignment table for leadership use, and
3) a concise markdown report with specific alignments and recommendations.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


DECISION_COL = "Decision - Accept-Reject-Needs Review"
LEGACY_DECISION_COL = "Decision"

SCOPE_ALL_COMPONENTS = "all_components"
SCOPE_ENABLING_COMPONENTS = "enabling_components"
SCOPE_DEPENDENT_COMPONENTS = "dependent_components"

SCOPE_LABELS = {
    SCOPE_ENABLING_COMPONENTS: "Relationship-role view: Enabling components from Alignments",
    SCOPE_DEPENDENT_COMPONENTS: "Relationship-role view: Dependent components from Alignments",
    SCOPE_ALL_COMPONENTS: "All components from Components",
}


def _stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H%M")


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".csv", ".tsv"}:
        sep = "\t" if path.suffix.lower() == ".tsv" else ","
        return pd.read_csv(path, sep=sep)
    return pd.read_excel(path)


def _find_column(df: pd.DataFrame, options: list[str]) -> str:
    def norm(value: str) -> str:
        return re.sub(r"[^a-z0-9]", "", str(value).lower())

    normalized = {norm(str(c)): str(c) for c in df.columns}
    for option in options:
        key = norm(option)
        if key in normalized:
            return normalized[key]
    raise ValueError(f"Required column not found. Options={options}; columns={list(df.columns)}")


def _load_ivn_candidates(ivn_filepath: Path, comparison_scope: str) -> pd.DataFrame:
    # Important modeling principle: "enabling" and "dependent" are relationship-scoped roles
    # from a specific pair in Alignments, not intrinsic properties of a component.
    ivn_xls = pd.ExcelFile(ivn_filepath)
    sheet_map = {s.lower(): s for s in ivn_xls.sheet_names}

    if comparison_scope == SCOPE_ALL_COMPONENTS:
        if "components" not in sheet_map:
            raise ValueError(f"IVN file missing Components sheet. Sheets={ivn_xls.sheet_names}")
        ivn_df = pd.read_excel(ivn_xls, sheet_name=sheet_map["components"])
        ivn_component_col = _find_column(ivn_df, ["Component", "Component Name", "component_name"])
        ivn_desc_col = _find_column(ivn_df, ["Component Description", "Description", "Text", "component_description"])
        ivn_url_col = _find_column(ivn_df, ["Component URL", "URL", "Link", "component_url"])
    else:
        if "alignments" not in sheet_map:
            raise ValueError(f"IVN file missing Alignments sheet. Sheets={ivn_xls.sheet_names}")
        ivn_df = pd.read_excel(ivn_xls, sheet_name=sheet_map["alignments"])
        if comparison_scope == SCOPE_ENABLING_COMPONENTS:
            ivn_component_col = _find_column(ivn_df, ["Enabling Component"])
            ivn_desc_col = _find_column(ivn_df, ["Enabling Component Description"])
            ivn_url_col = _find_column(ivn_df, ["Enabling Component URL"])
        elif comparison_scope == SCOPE_DEPENDENT_COMPONENTS:
            ivn_component_col = _find_column(ivn_df, ["Dependent Component"])
            ivn_desc_col = _find_column(ivn_df, ["Dependent Component Description"])
            ivn_url_col = _find_column(ivn_df, ["Dependent Component URL"])
        else:
            raise ValueError(
                f"Unsupported comparison scope: {comparison_scope}. "
                f"Choose one of: {', '.join(SCOPE_LABELS.keys())}."
            )

    ivn_norm = ivn_df[[ivn_component_col, ivn_desc_col, ivn_url_col]].copy()
    ivn_norm.columns = ["Aligned IVN Component", "Aligned IVN Description", "Aligned IVN URL"]
    ivn_norm = ivn_norm.dropna(subset=["Aligned IVN Description"]).reset_index(drop=True)

    # Alignments table can repeat components across many edges; keep one record per component.
    ivn_norm["_desc_len"] = ivn_norm["Aligned IVN Description"].astype(str).str.len()
    ivn_norm = ivn_norm.sort_values("_desc_len", ascending=False).drop_duplicates(
        subset=["Aligned IVN Component"],
        keep="first",
    )
    return ivn_norm.drop(columns=["_desc_len"]).reset_index(drop=True)


def load_data(
    eo_filepath: Path,
    ivn_filepath: Path,
    comparison_scope: str = SCOPE_ALL_COMPONENTS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load EO components and IVN Components tab into normalized dataframes."""
    eo_df = _read_table(eo_filepath)
    eo_component_col = _find_column(eo_df, ["Component", "Component Name"])
    eo_desc_col = _find_column(eo_df, ["Component Description", "Description", "Text"])
    eo_url_col = _find_column(eo_df, ["Component URL", "URL", "Link"])

    eo_norm = eo_df[[eo_component_col, eo_desc_col, eo_url_col]].copy()
    eo_norm.columns = ["New Component", "New Component Description", "New Component URL"]

    ivn_norm = _load_ivn_candidates(ivn_filepath, comparison_scope)

    eo_norm = eo_norm.dropna(subset=["New Component Description"]).reset_index(drop=True)
    return eo_norm, ivn_norm


def perform_semantic_search(
    eo_df: pd.DataFrame,
    ivn_df: pd.DataFrame,
    top_k: int = 5,
    min_score: float = 0.08,
) -> pd.DataFrame:
    """Perform TF-IDF semantic similarity and return top-k candidates per EO component."""
    vectorizer = TfidfVectorizer(stop_words="english")

    eo_texts = eo_df["New Component Description"].astype(str).tolist()
    ivn_texts = ivn_df["Aligned IVN Description"].astype(str).tolist()

    all_texts = eo_texts + ivn_texts
    vectorizer.fit(all_texts)

    eo_embeddings = vectorizer.transform(eo_texts)
    ivn_embeddings = vectorizer.transform(ivn_texts)
    cosine_scores = cosine_similarity(eo_embeddings, ivn_embeddings)

    alignments: list[dict] = []
    for i in range(len(eo_df)):
        ranked = cosine_scores[i].argsort()[::-1][:top_k]
        for j in ranked:
            score = float(cosine_scores[i][j])
            if score < min_score:
                continue
            alignments.append({
                "New Component": eo_df.at[i, "New Component"],
                "New Component Description": eo_df.at[i, "New Component Description"],
                "New Component URL": eo_df.at[i, "New Component URL"],
                "Aligned IVN Component": ivn_df.at[j, "Aligned IVN Component"],
                "Aligned IVN Description": ivn_df.at[j, "Aligned IVN Description"],
                "Aligned IVN URL": ivn_df.at[j, "Aligned IVN URL"],
                "Semantic Score": round(score, 4),
            })
    return pd.DataFrame(alignments)


def curate_alignments(raw_df: pd.DataFrame, min_score: float = 0.14, top_per_component: int = 2) -> pd.DataFrame:
    """Remove noisy rows and return leadership-ready candidates."""
    if raw_df.empty:
        return raw_df.copy()

    df = raw_df.copy()
    df = df[df["Semantic Score"] >= min_score]
    df = df.drop_duplicates(subset=["New Component", "Aligned IVN Component"]) 

    # Filter common noisy link patterns.
    if "Aligned IVN URL" in df.columns:
        df = df[~df["Aligned IVN URL"].astype(str).str.contains(r"google\.com/search|^https\s", case=False, regex=True, na=False)]

    # Prefer authoritative sources and useful policy-like components.
    df = df[df["Aligned IVN URL"].astype(str).str.contains(r"federalregister\.gov|whitehouse\.gov|ecfr\.gov|acquisition\.gov", case=False, regex=True, na=False)]

    # Remove very short EO action snippets that are often parsing fragments.
    df = df[df["New Component Description"].astype(str).str.len() >= 100]

    # Rank and keep top matches per EO component.
    df = df.sort_values("Semantic Score", ascending=False)
    df = df.groupby("New Component", as_index=False).head(top_per_component)
    return df.reset_index(drop=True)


def _make_match_id(new_component: str, ivn_component: str) -> str:
    material = f"{str(new_component).strip().lower()}||{str(ivn_component).strip().lower()}"
    return hashlib.sha1(material.encode("utf-8")).hexdigest()[:16]


def with_match_ids(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Match ID"] = out.apply(
        lambda r: _make_match_id(r.get("New Component", ""), r.get("Aligned IVN Component", "")),
        axis=1,
    )
    return out


def normalize_decision(value: str) -> str:
    token = str(value or "").strip().lower()
    if token in {"accept", "accepted", "a", "yes", "y"}:
        return "Accept"
    if token in {"reject", "rejected", "r", "no", "n"}:
        return "Reject"
    return "Needs Review"


def ensure_decision_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if DECISION_COL not in out.columns and LEGACY_DECISION_COL in out.columns:
        out = out.rename(columns={LEGACY_DECISION_COL: DECISION_COL})
    if DECISION_COL not in out.columns:
        out[DECISION_COL] = ""
    return out


def load_adjudication_queue(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["Match ID", DECISION_COL, "Reviewer Notes", "Updated At"])

    queue = ensure_decision_column(pd.read_csv(path))
    required = ["Match ID", DECISION_COL, "Reviewer Notes", "Updated At"]
    for col in required:
        if col not in queue.columns:
            queue[col] = ""
    queue[DECISION_COL] = queue[DECISION_COL].map(normalize_decision)
    return queue


def build_adjudication_queue(candidates: pd.DataFrame, existing_queue: pd.DataFrame) -> pd.DataFrame:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cand = with_match_ids(candidates)

    queue_cols = [
        "Match ID",
        "New Component",
        "New Component Description",
        "New Component URL",
        "Aligned IVN Component",
        "Aligned IVN Description",
        "Aligned IVN URL",
        "Semantic Score",
        DECISION_COL,
        "Reviewer Notes",
        "Updated At",
    ]

    base = cand.merge(
        existing_queue[["Match ID", DECISION_COL, "Reviewer Notes", "Updated At"]],
        on="Match ID",
        how="left",
    )
    base[DECISION_COL] = base[DECISION_COL].map(normalize_decision)
    base["Reviewer Notes"] = base["Reviewer Notes"].fillna("")
    base["Updated At"] = base["Updated At"].fillna(now)

    # Default new rows to Needs Review.
    base.loc[base[DECISION_COL].isna() | (base[DECISION_COL] == "Needs Review"), DECISION_COL] = "Needs Review"

    # Preserve historical decisions not present in current candidates.
    historical = existing_queue[~existing_queue["Match ID"].isin(base["Match ID"])].copy()
    for col in queue_cols:
        if col not in historical.columns:
            historical[col] = ""
    combined = pd.concat([base[queue_cols], historical[queue_cols]], ignore_index=True)

    combined[DECISION_COL] = combined[DECISION_COL].map(normalize_decision)
    combined = combined.drop_duplicates(subset=["Match ID"], keep="first")
    return combined.sort_values([DECISION_COL, "Semantic Score"], ascending=[True, False], na_position="last")


def apply_adjudication(raw_df: pd.DataFrame, curated_df: pd.DataFrame, queue_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Use historical human decisions to improve precision and produce validated crosswalk."""
    if raw_df.empty:
        return curated_df.copy(), curated_df.iloc[0:0].copy()

    raw_ids = with_match_ids(raw_df)
    cur_ids = with_match_ids(curated_df)
    decision_map = {row["Match ID"]: normalize_decision(row[DECISION_COL]) for _, row in queue_df.iterrows()}

    # Precision improvement: remove historically rejected pairs from curation.
    cur_ids[DECISION_COL] = cur_ids["Match ID"].map(lambda x: decision_map.get(x, "Needs Review"))
    cur_ids = cur_ids[cur_ids[DECISION_COL] != "Reject"].copy()

    # Validated crosswalk is accepted pairs (auto-carries forward accepted decisions).
    raw_ids[DECISION_COL] = raw_ids["Match ID"].map(lambda x: decision_map.get(x, "Needs Review"))
    validated = raw_ids[raw_ids[DECISION_COL] == "Accept"].copy()

    # Keep one best row per accepted pair.
    if not validated.empty:
        validated = validated.sort_values("Semantic Score", ascending=False)
        validated = validated.drop_duplicates(subset=["Match ID"], keep="first")

    return cur_ids.reset_index(drop=True), validated.reset_index(drop=True)


def write_leadership_report(curated_df: pd.DataFrame, report_path: Path) -> None:
    lines: list[str] = []
    lines.append("# EO Leadership Alignment Report")
    lines.append("")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Curated alignment count: {len(curated_df)}")
    lines.append("")
    lines.append("## Specific Alignments")

    if curated_df.empty:
        lines.append("No alignments met the current quality threshold.")
    else:
        top = curated_df.sort_values("Semantic Score", ascending=False).head(12)
        for idx, row in enumerate(top.itertuples(index=False), start=1):
            lines.append(f"{idx}. EO Component: {row[0]}")
            lines.append(f"   - IVN Component: {row[3]}")
            lines.append(f"   - Semantic Score: {row[6]}")
            lines.append(f"   - EO URL: {row[2]}")
            lines.append(f"   - IVN URL: {row[5]}")
            lines.append("")

    lines.append("## Leadership Recommendations: Management")
    lines.append("1. Enforce fixed-price-first planning and require exception rationale for non-fixed-price selections.")
    lines.append("2. Establish executive approval controls for high-value non-fixed-price actions with auditable decision logs.")
    lines.append("3. Run a time-bound remediation cycle to renegotiate eligible contracts toward fixed-price/performance outcomes.")
    lines.append("4. Track quarterly metrics by aligned component: exception count, dollar exposure, and renegotiation completion.")
    lines.append("")
    lines.append("## Leadership Recommendations: Compliance Communication")
    lines.append("1. Publish a traceability crosswalk from EO clauses to IVN components with named owners.")
    lines.append("2. Present confidence-scored alignments in executive briefs and flag candidates requiring SME adjudication.")
    lines.append("3. Include evidence artifacts per alignment: approvals, policy memos, contract modifications, and training completion.")

    report_path.write_text("\n".join(lines), encoding="utf-8")


def write_governance_review_report(
    curated_df: pd.DataFrame,
    validated_df: pd.DataFrame,
    queue_df: pd.DataFrame,
    report_path: Path,
) -> None:
    counts = queue_df[DECISION_COL].map(normalize_decision).value_counts()
    accepted_count = int(counts.get("Accept", 0))
    rejected_count = int(counts.get("Reject", 0))
    review_count = int(counts.get("Needs Review", 0))

    lines: list[str] = []
    lines.append("# EO Governance Crosswalk Review Report")
    lines.append("")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(
        "Principle: No component has an inherent Enabling or Dependent feature; those are pair-specific relationship roles."
    )
    lines.append(
        "A component is considered enabling another in a given pair when it is >50% likely to progress the other toward delivery."
    )
    lines.append(f"Curated candidate count: {len(curated_df)}")
    lines.append(f"Validated crosswalk count (Accept): {len(validated_df)}")
    lines.append(f"Queue decisions: Accept={accepted_count}, Reject={rejected_count}, Needs Review={review_count}")
    lines.append("")

    lines.append("## Validated Crosswalk (Accepted Matches)")
    if validated_df.empty:
        lines.append("No accepted alignments yet. Update queue decisions to promote validated matches.")
    else:
        top_val = validated_df.sort_values("Semantic Score", ascending=False).head(15)
        for idx, row in enumerate(top_val.itertuples(index=False), start=1):
            lines.append(f"{idx}. EO Component: {row[0]}")
            lines.append(f"   - IVN Component: {row[3]}")
            lines.append(f"   - Semantic Score: {row[6]}")
            lines.append(f"   - EO URL: {row[2]}")
            lines.append(f"   - IVN URL: {row[5]}")
            lines.append("")

    lines.append("## Adjudication Operating Instructions")
    lines.append(f"1. Open the adjudication queue CSV and set {DECISION_COL} to Accept, Reject, or Needs Review.")
    lines.append("2. Add Reviewer Notes for any non-obvious decisions.")
    lines.append("3. Re-run this script; rejected pairs will be automatically filtered from candidates.")
    lines.append("4. Accepted pairs will be promoted into the validated crosswalk output for governance review.")

    report_path.write_text("\n".join(lines), encoding="utf-8")


def run_crosswalk_pipeline(
    eo_path: Path,
    ivn_path: Path,
    prefix: str,
    queue_path: Path,
    comparison_scope: str = SCOPE_ALL_COMPONENTS,
) -> dict[str, str]:
    """Run the full alignment pipeline and return output file paths."""
    print("Loading data...")
    eo_df, ivn_df = load_data(eo_path, ivn_path, comparison_scope)
    print(f"Comparison scope: {SCOPE_LABELS.get(comparison_scope, comparison_scope)}")

    print("Performing semantic search...")
    raw_df = perform_semantic_search(eo_df, ivn_df)
    curated_df = curate_alignments(raw_df)

    existing_queue = load_adjudication_queue(queue_path)
    updated_queue = build_adjudication_queue(curated_df, existing_queue)
    curated_df, validated_df = apply_adjudication(raw_df, curated_df, updated_queue)

    stamp = _stamp()
    raw_out = Path(f"{prefix}_raw_{stamp}.csv")
    curated_out = Path(f"{prefix}_curated_{stamp}.csv")
    report_out = Path(f"{prefix}_leadership_report_{stamp}.md")
    validated_out = Path(f"{prefix}_validated_crosswalk_{stamp}.csv")
    governance_report_out = Path(f"{prefix}_governance_review_report_{stamp}.md")

    raw_df.to_csv(raw_out, index=False)
    curated_df.to_csv(curated_out, index=False)
    validated_df.to_csv(validated_out, index=False)
    updated_queue.to_csv(queue_path, index=False)
    write_leadership_report(curated_df, report_out)
    write_governance_review_report(curated_df, validated_df, updated_queue, governance_report_out)

    print(f"Raw alignments: {raw_out} ({len(raw_df)} rows)")
    print(f"Curated alignments: {curated_out} ({len(curated_df)} rows)")
    print(f"Validated crosswalk: {validated_out} ({len(validated_df)} rows)")
    print(f"Adjudication queue: {queue_path} ({len(updated_queue)} rows)")
    print(f"Leadership report: {report_out}")
    print(f"Governance review report: {governance_report_out}")

    return {
        "eo_path": str(eo_path),
        "ivn_path": str(ivn_path),
        "prefix": prefix,
        "queue_path": str(queue_path),
        "comparison_scope": comparison_scope,
        "raw_out": str(raw_out),
        "curated_out": str(curated_out),
        "validated_out": str(validated_out),
        "leadership_report_out": str(report_out),
        "governance_report_out": str(governance_report_out),
        "timestamp": stamp,
    }


def _state_file() -> Path:
    return Path("ivn_semantic_alignment_menu_state.json")


def load_menu_state() -> dict:
    state_path = _state_file()
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_menu_state(state: dict) -> None:
    _state_file().write_text(json.dumps(state, indent=2), encoding="utf-8")


def prompt_path(prompt_text: str, must_exist: bool = True) -> Path:
    while True:
        raw = input(prompt_text).strip().strip('"')
        path = Path(raw)
        if must_exist and not path.exists():
            print(f"Path not found: {path}")
            continue
        return path


def prompt_menu_choice() -> str:
    print("\nIVN Semantic Alignment Menu")
    print("1. Run the crosswalk of a new governance document")
    print("2. Download alignment adjudication queue for new alignment decisions")
    print("3. Upload alignment adjudication queue with completed decisions")
    print("4. Download Validated crosswalk file based on new completed decisions")
    print("0. Exit")
    return input("Select an option: ").strip()


def option_run_new_crosswalk(state: dict) -> dict:
    print("\nOption 1: Run the crosswalk of a new governance document")
    ivn_path = prompt_path("Enter the full path to the IVN dataset (.xlsx): ")
    eo_path = prompt_path("Enter the full path to the new governance document components file (.csv/.xlsx/.tsv): ")

    default_prefix = f"ivn_semantic_alignments_{eo_path.stem}"
    prefix = input(f"Output prefix [{default_prefix}]: ").strip() or default_prefix

    default_queue = Path(state.get("queue_path", "ivn_alignment_adjudication_queue.csv"))
    queue_input = input(f"Queue file path [{default_queue}]: ").strip()
    queue_path = Path(queue_input) if queue_input else default_queue

    print("\nChoose IVN comparison scope:")
    print("(Role note: enabling/dependent are pair-specific relationship roles, not inherent component types.)")
    print("1. Enabling Components from Alignments table")
    print("2. Dependent Components from Alignments table")
    print("3. All components from Components table")
    scope_choice = input("Select scope [3]: ").strip() or "3"
    scope_map = {
        "1": SCOPE_ENABLING_COMPONENTS,
        "2": SCOPE_DEPENDENT_COMPONENTS,
        "3": SCOPE_ALL_COMPONENTS,
    }
    comparison_scope = scope_map.get(scope_choice, SCOPE_ALL_COMPONENTS)

    results = run_crosswalk_pipeline(eo_path, ivn_path, prefix, queue_path, comparison_scope)
    state.update(results)
    save_menu_state(state)
    return state


def option_download_queue(state: dict) -> None:
    print("\nOption 2: Download alignment adjudication queue")
    queue_path = Path(state.get("queue_path", "ivn_alignment_adjudication_queue.csv"))
    if not queue_path.exists():
        print("No adjudication queue found yet. Run Option 1 first to generate a queue.")
        return

    destination = prompt_path("Enter destination file path to save queue CSV: ", must_exist=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(queue_path, destination)
    print(f"Queue copied to: {destination}")


def option_upload_completed_queue(state: dict) -> dict:
    print("\nOption 3: Upload completed adjudication queue")
    source_queue = prompt_path("Enter path to completed queue CSV: ")
    queue_path = Path(state.get("queue_path", "ivn_alignment_adjudication_queue.csv"))

    queue_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_queue, queue_path)
    print(f"Completed queue imported to: {queue_path}")

    state["queue_path"] = str(queue_path)
    save_menu_state(state)
    return state


def option_download_validated_crosswalk(state: dict) -> dict:
    print("\nOption 4: Download validated crosswalk based on completed decisions")
    eo_path_str = state.get("eo_path")
    ivn_path_str = state.get("ivn_path")
    prefix = state.get("prefix")
    queue_path_str = state.get("queue_path", "ivn_alignment_adjudication_queue.csv")
    comparison_scope = state.get("comparison_scope", SCOPE_ALL_COMPONENTS)

    if not (eo_path_str and ivn_path_str and prefix):
        print("No prior run metadata found. Run Option 1 first.")
        return state

    eo_path = Path(eo_path_str)
    ivn_path = Path(ivn_path_str)
    queue_path = Path(queue_path_str)

    results = run_crosswalk_pipeline(eo_path, ivn_path, prefix, queue_path, comparison_scope)
    state.update(results)
    save_menu_state(state)

    validated_out = Path(results["validated_out"])
    if not validated_out.exists():
        print("Validated crosswalk file was not produced.")
        return state

    destination = prompt_path("Enter destination file path to save validated crosswalk CSV: ", must_exist=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(validated_out, destination)
    print(f"Validated crosswalk copied to: {destination}")
    return state


def run_menu() -> None:
    state = load_menu_state()
    while True:
        choice = prompt_menu_choice()
        if choice == "1":
            state = option_run_new_crosswalk(state)
        elif choice == "2":
            option_download_queue(state)
        elif choice == "3":
            state = option_upload_completed_queue(state)
        elif choice == "4":
            state = option_download_validated_crosswalk(state)
        elif choice == "0":
            print("Exiting menu.")
            return
        else:
            print("Invalid choice. Please select a valid option.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Semantic crosswalk between EO components and IVN Components sheet.")
    parser.add_argument("--eo", help="Path to EO component inventory file (csv/xlsx/tsv)")
    parser.add_argument("--ivn", help="Path to IVN dataset xlsx")
    parser.add_argument("--prefix", default="ivn_semantic_alignments", help="Output file prefix")
    parser.add_argument(
        "--queue",
        default="ivn_alignment_adjudication_queue.csv",
        help="Persistent adjudication queue CSV path",
    )
    parser.add_argument(
        "--scope",
        choices=[SCOPE_ENABLING_COMPONENTS, SCOPE_DEPENDENT_COMPONENTS, SCOPE_ALL_COMPONENTS],
        default=SCOPE_ALL_COMPONENTS,
        help="IVN comparison scope: relationship-role enabling/dependent from Alignments, or all components from Components",
    )
    parser.add_argument("--menu", action="store_true", help="Launch interactive user menu")
    args = parser.parse_args()

    if args.menu:
        run_menu()
        return

    if not args.eo or not args.ivn:
        print("Missing --eo or --ivn. Launching interactive menu.")
        run_menu()
        return

    eo_path = Path(args.eo)
    ivn_path = Path(args.ivn)
    queue_path = Path(args.queue)
    run_crosswalk_pipeline(eo_path, ivn_path, args.prefix, queue_path, args.scope)


if __name__ == "__main__":
    main()
