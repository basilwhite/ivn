# ivn_intelligent_component_crosswalk.py
"""Meta-specification for this script (abridged).

This file implements an intelligent component crosswalk for a federal knowledge
network. It reads `ivntest.xlsx` from the same folder, learns patterns from
known enabling–dependent component alignments, infers new alignments, enforces
component metadata, verifies component–source consistency, and writes a
production-quality Excel workbook with exactly two sheets:

- Inferred_Alignments
- verification_issues

A synchronized, detailed specification for this script lives in
`ivn_intelligent_component_crosswalk_context.txt`. Any substantive change to
this script must be reflected there.

This top-of-file docstring is intentionally high level and safe for Python to
parse. Implementation details are expressed in normal code below.
"""

from __future__ import annotations

import argparse
import importlib.util
import datetime as _dt
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, Any

import joblib
import networkx as nx
import numpy as np
import pandas as pd
from difflib import SequenceMatcher
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


def print_verbose(msg: str) -> None:
    ts = _dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def format_time(seconds: float) -> str:
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{mins}m {secs}s"


def get_timestamp() -> str:
    return _dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")


def sanitize_df_for_excel(df: Optional[pd.DataFrame], max_cell_length: int = 32000) -> Optional[pd.DataFrame]:
    """Return a copy of `df` with values converted to Excel-safe types/strings."""
    if df is None:
        return None

    control_chars_re = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

    def sanitize_value(v):  # type: ignore[override]
        try:
            if pd.isna(v):
                return ""
        except Exception:
            pass

        if isinstance(v, (list, dict, tuple)):
            try:
                s = json.dumps(v, ensure_ascii=False)
            except Exception:
                s = str(v)
            s = control_chars_re.sub("", s)
            return s[:max_cell_length]

        try:
            import numpy as _np  # local to avoid confusion with top-level np

            if isinstance(v, _np.generic):
                return v.item()
        except Exception:
            pass

        try:
            if isinstance(v, (_dt.date, _dt.datetime)):
                return v.isoformat()
        except Exception:
            pass

        s = str(v)
        s = control_chars_re.sub("", s)
        return s[:max_cell_length]

    try:
        safe_df = df.copy()
        for col in safe_df.columns:
            safe_df[col] = safe_df[col].apply(sanitize_value)
        return safe_df
    except Exception:
        try:
            return df.astype(str).replace("nan", "")
        except Exception:
            return df


def ask_file_path_from_list(script_dir: Path) -> Optional[str]:
    """List existing training JSONs and let the user choose one, or none."""
    json_files = sorted(
        f
        for f in os.listdir(script_dir)
        if f.startswith("crosswalk_inferences_training(") and f.endswith(".json")
    )
    if not json_files:
        print_verbose("No training JSON files found in the script folder.")
        return None

    print("Select a training JSON file:")
    for idx, fname in enumerate(json_files, 1):
        print(f"{idx}: {fname}")
    print("0: Run without a training file (train a new model)")

    while True:
        choice = input("Enter the number of the file to use (or 0): ").strip()
        if choice == "0":
            return None
        try:
            idx = int(choice)
            if 1 <= idx <= len(json_files):
                return str(script_dir / json_files[idx - 1])
        except Exception:
            pass
        print("Invalid selection. Please try again.")


def similar(a: str, b: str) -> float:
    return SequenceMatcher(None, str(a), str(b)).ratio()


# ---------------------------------------------------------------------------
# Excel loading helpers
# ---------------------------------------------------------------------------


def load_all_excel_tabs(excel_path: Path) -> Dict[str, pd.DataFrame]:
    try:
        xl = pd.ExcelFile(excel_path)
    except PermissionError as exc:
        print_verbose(
            f"PermissionError opening '{excel_path}': {exc}. "
            "Is the workbook open in Excel or locked by another process?"
        )
        raise
    tabs: Dict[str, pd.DataFrame] = {}
    print_verbose(f"Workbook sheets: {xl.sheet_names}")
    for sheet in xl.sheet_names:
        df = xl.parse(sheet_name=str(sheet))
        key = str(sheet).strip().lower()
        tabs[key] = df
        print_verbose(f"Sheet '{sheet}': columns={list(df.columns)}, rows={len(df)}")
    return tabs


def get_tab_by_name(tabs: Dict[str, pd.DataFrame], name: str) -> Optional[pd.DataFrame]:
    key = name.strip().lower()
    df = tabs.get(key)
    if df is None:
        print_verbose(f"Tab '{name}' not found in workbook.")
    return df


# ---------------------------------------------------------------------------
# Graph + features + model
# ---------------------------------------------------------------------------


def safe_get_str(row: pd.Series, *keys: str) -> str:
    """Safely get a string value from a row, trying multiple column names.
    
    Handles NaN values properly (pandas NaN is truthy but should be treated as empty).
    """
    for key in keys:
        val = row.get(key, None)
        if val is not None and not (isinstance(val, float) and pd.isna(val)):
            s = str(val).strip()
            if s and s.lower() != "nan":
                return s
    return ""


def build_alignment_graph(alignments_df: Optional[pd.DataFrame]) -> nx.Graph:
    G = nx.Graph()
    if alignments_df is not None:
        for _, row in alignments_df.iterrows():
            # Support both "Enabling Component" and "enabling_component_id" column names
            a = safe_get_str(row, "Enabling Component", "enabling_component_id", "enabling_component")
            b = safe_get_str(row, "Dependent Component", "dependent_component_id", "dependent_component")
            if a and b:
                G.add_edge(a, b)
    return G


def extract_features(pairs: Sequence[Tuple[str, str]], graph: nx.Graph) -> np.ndarray:
    feats: List[List[float]] = []
    for a, b in pairs:
        str_sim = similar(a, b)
        try:
            path_len = nx.shortest_path_length(graph, a, b)
            indirect = 1.0 / (path_len + 1)
        except (nx.NodeNotFound, nx.NetworkXNoPath):
            indirect = 0.0
        feats.append([str_sim, indirect])
    return np.array(feats, dtype=float)


def train_alignment_model(
    alignments_df: Optional[pd.DataFrame],
    nonaligned_df: Optional[pd.DataFrame],
    components_list: Sequence[str],  # kept for future use
) -> Dict[str, Any]:
    start = time.time()
    model: Dict[str, Any] = {"threshold": 0.6}

    print_verbose("Building alignment graph for training...")
    graph = build_alignment_graph(alignments_df)
    model["graph_edges"] = list(graph.edges())
    print_verbose(f"Graph has {len(graph.edges())} edges - elapsed {format_time(time.time() - start)}")
    pairs: List[Tuple[str, str]] = []
    labels: List[int] = []

    if alignments_df is not None:
        for _, row in alignments_df.iterrows():
            # Support both "Enabling Component" and "enabling_component_id" column names
            a = safe_get_str(row, "Enabling Component", "enabling_component_id", "enabling_component")
            b = safe_get_str(row, "Dependent Component", "dependent_component_id", "dependent_component")
            if a and b:
                pairs.append((a, b))
                labels.append(1)
        print_verbose(f"Positive training pairs: {sum(1 for l in labels if l == 1)}")

    if nonaligned_df is not None:
        for _, row in nonaligned_df.iterrows():
            # Support both "Enabling Component" and "enabling_component_id" column names
            a = safe_get_str(row, "Enabling Component", "enabling_component_id", "enabling_component")
            b = safe_get_str(row, "Dependent Component", "dependent_component_id", "dependent_component")
            if a and b:
                pairs.append((a, b))
                labels.append(0)
        print_verbose(f"Negative training pairs: {sum(1 for l in labels if l == 0)}")

    if not pairs:
        print_verbose("No training pairs found; returning minimal model.")
        return model

    print_verbose(f"Extracting features for {len(pairs)} training pairs...")
    X = extract_features(pairs, graph)
    y = np.array(labels, dtype=int)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    print_verbose("Training RandomForestClassifier...")
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_scaled, y)

    model["classifier"] = {
        "scaler_mean": scaler.mean_.tolist(),
        "scaler_scale": scaler.scale_.tolist(),
        "classes": clf.classes_.tolist(),
        "feature_importances": clf.feature_importances_.tolist(),
        "n_features_in": int(clf.n_features_in_),
        "n_classes": int(clf.n_classes_),
    }
    model["clf_obj"] = {"clf": clf, "scaler": scaler}

    print_verbose(f"Model training complete in {format_time(time.time() - start)}")
    return model


def convert_ndarrays(obj: Any) -> Any:
    """Recursively convert numpy arrays/generic numpy types into JSON-safe types.

    This helper is intentionally generic and does not depend on any outer
    variables like `model`. Any key named "clf_obj" is dropped so that
    non-serializable classifier objects are never passed to json.dump.
    """
    # Handle numpy arrays first
    if isinstance(obj, np.ndarray):
        return obj.tolist()

    # Handle numpy scalar types
    try:
        import numpy as _np

        if isinstance(obj, _np.generic):
            return obj.item()
    except Exception:
        # If numpy is not available or obj is not a numpy scalar, ignore
        pass

    # Recurse into dicts, skipping any clf_obj entries
    if isinstance(obj, dict):
        result: Dict[Any, Any] = {}
        for k, v in obj.items():
            if k == "clf_obj":
                continue
            result[k] = convert_ndarrays(v)
        return result

    # Recurse into lists/tuples
    if isinstance(obj, list):
        return [convert_ndarrays(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(convert_ndarrays(v) for v in obj)

    # Everything else is returned as-is
    return obj


def save_model_json(model: Dict[str, Any], out_path: Path) -> None:
    """Save a model dictionary to JSON, stripping non-serializable fields.

    The "clf_obj" entry (containing the RandomForest and scaler) is always
    removed and stored separately via joblib.
    """
    serializable_model: Dict[str, Any] = {k: v for k, v in model.items() if k != "clf_obj"}
    cleaned = convert_ndarrays(serializable_model)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=2)


def load_model_json(path: Path) -> Dict[str, Any]:
    """Load a model dictionary from JSON."""
    with open(path, "r", encoding="utf-8") as f:
        data: Dict[str, Any] = json.load(f)
    return data


# ---------------------------------------------------------------------------
# Candidate pairs + threshold estimation
# ---------------------------------------------------------------------------


def build_pairs_to_predict(
    components_df: Optional[pd.DataFrame],
    tobecrosswalked_df: Optional[pd.DataFrame],
    sources_df: Optional[pd.DataFrame] = None,  # kept for signature compatibility
) -> Tuple[List[Tuple[str, str]], List[Dict[str, str]]]:
    pairs: List[Tuple[str, str]] = []
    meta: List[Dict[str, str]] = []
    if components_df is None or tobecrosswalked_df is None:
        return pairs, meta

    comp_rows = components_df.to_dict("records")
    tobe_rows = tobecrosswalked_df.to_dict("records")

    for comp in comp_rows:
        for tobe in tobe_rows:
            comp_source = str(comp.get("Source", comp.get("source_id", ""))).strip()
            tobe_source = str(tobe.get("Source", "")).strip()
            if comp_source and tobe_source and comp_source == tobe_source:
                continue
            enabling = str(comp.get("Component", comp.get("component_name", ""))).strip()
            dependent = str(tobe.get("Component", tobe.get("component_name", ""))).strip()
            if not enabling or not dependent:
                continue
            pairs.append((enabling, dependent))
            meta.append({
                "Enabling Component": enabling,
                "Dependent Component": dependent,
            })
    return pairs, meta


def estimate_threshold_from_sample(
    pairs: Sequence[Tuple[str, str]],
    model: Dict,
    sample_size: int = 10000,
    target_rows: int = 90000,
) -> Tuple[float, int, int, Dict[float, Dict[str, float]]]:
    total_pairs = len(pairs)
    if total_pairs == 0:
        return 0.0, 0, 0, {}

    rng = np.random.default_rng(seed=42)
    sample_n = min(sample_size, total_pairs)
    idxs = rng.choice(total_pairs, size=sample_n, replace=False)
    sample_pairs = [pairs[i] for i in idxs]

    graph = nx.Graph()
    graph.add_edges_from(model.get("graph_edges", []))
    Xs = extract_features(sample_pairs, graph)

    clf_obj = model.get("clf_obj")
    if clf_obj and isinstance(clf_obj, dict) and "clf" in clf_obj and "scaler" in clf_obj:
        scaler = clf_obj["scaler"]
        clf = clf_obj["clf"]
        try:
            Xs_scaled = scaler.transform(Xs)
            sample_conf = clf.predict_proba(Xs_scaled)[:, 1]
        except Exception:
            sample_conf = np.random.rand(len(sample_pairs))
    else:
        sample_conf = np.random.rand(len(sample_pairs))

    ratio = float(target_rows) / float(total_pairs)
    if ratio >= 1.0:
        est_threshold = 0.0
    else:
        q = max(0.0, 1.0 - ratio)
        est_threshold = float(np.quantile(sample_conf, q))

    est_frac = float((sample_conf >= est_threshold).mean())
    est_rows = int(round(est_frac * total_pairs))
    SAFETY_FACTOR = 1.5
    est_rows_safe = int(round(est_rows * SAFETY_FACTOR))

    pct_estimates: Dict[float, Dict[str, float]] = {}
    for p in [0.5, 0.75, 0.9, 0.95, 0.99]:
        thr = float(np.quantile(sample_conf, p))
        frac = float((sample_conf >= thr).mean())
        rows = int(round(frac * total_pairs))
        rows_safe = int(round(rows * SAFETY_FACTOR))
        pct_estimates[p] = {"threshold": thr, "estimated_rows": rows, "estimated_rows_safe": rows_safe}

    return est_threshold, est_rows_safe, sample_n, pct_estimates


# ---------------------------------------------------------------------------
# Metadata enforcement & verification
# ---------------------------------------------------------------------------


def enforce_component_metadata(
    results_df: Optional[pd.DataFrame],
    components_df: Optional[pd.DataFrame],
    tobecrosswalked_df: Optional[pd.DataFrame],
    sources_df: Optional[pd.DataFrame],
) -> Tuple[pd.DataFrame, Dict]:
    if results_df is None or results_df.empty:
        return results_df if results_df is not None else pd.DataFrame(), {"total": 0, "enforced": 0, "issues": []}

    comp_meta: Dict[str, Dict[str, str]] = {}
    if components_df is not None:
        for _, row in components_df.iterrows():
            name = str(row.get("Component", row.get("component_name", ""))).strip()
            if not name:
                continue
            comp_meta[name] = {
                "source_id": str(row.get("Source", row.get("source_id", ""))).strip(),
                "description": str(row.get("Component Description", row.get("component_description", ""))).strip(),
                "url": str(row.get("Component URL", row.get("component_url", ""))).strip(),
                "office": str(row.get(
                    "Component Office of Primary Interest",
                    row.get("component_ofc_of_primary_interest", ""),
                )).strip(),
            }

    if tobecrosswalked_df is not None:
        for _, row in tobecrosswalked_df.iterrows():
            name = str(row.get("Component", "")).strip()
            if not name or name in comp_meta:
                continue
            comp_meta[name] = {
                "source_id": str(row.get("Source", "")).strip(),
                "description": str(row.get("Component Description", "")).strip(),
                "url": str(row.get("Component URL", "")).strip(),
                "office": str(row.get("Component Office of Primary Interest", "")).strip(),
            }

    source_agency_lookup: Dict[str, str] = {}
    if sources_df is not None:
        for _, row in sources_df.iterrows():
            sid = str(row.get("source_id", row.get("Source", ""))).strip()
            if not sid:
                continue
            agency = (
                str(row.get("source_agency", ""))
                or str(row.get("Source Agency", ""))
                or str(row.get("agency", ""))
                or str(row.get("agency_name", ""))
            ).strip()
            source_agency_lookup[sid] = agency

    df = results_df.copy()
    enforced = 0
    issues: List[str] = []
    for idx, row in df.iterrows():
        en = str(row.get("Enabling Component", "")).strip()
        de = str(row.get("Dependent Component", "")).strip()

        if en and en in comp_meta:
            meta = comp_meta[en]
            if str(row.get("Enabling Source", "")).strip() != meta.get("source_id", ""):
                df.at[idx, "Enabling Source"] = meta.get("source_id", "")
                enforced += 1
            df.at[idx, "Enabling Component Description"] = meta.get("description", "")
            df.at[idx, "Enabling Component URL"] = meta.get("url", "")
            df.at[idx, "Enabling Component Office of Primary Interest"] = meta.get("office", "")
            sid = meta.get("source_id", "")
            df.at[idx, "Enabling Source Agency"] = source_agency_lookup.get(sid, "")

        if de and de in comp_meta:
            meta = comp_meta[de]
            if str(row.get("Dependent Source", "")).strip() != meta.get("source_id", ""):
                df.at[idx, "Dependent Source"] = meta.get("source_id", "")
                enforced += 1
            df.at[idx, "Dependent Component Description"] = meta.get("description", "")
            df.at[idx, "Dependent Component URL"] = meta.get("url", "")
            df.at[idx, "Dependent Component Office of Primary Interest"] = meta.get("office", "")
            sid = meta.get("source_id", "")
            df.at[idx, "Dependent Source Agency"] = source_agency_lookup.get(sid, "")

    return df, {"total": len(df), "enforced": enforced, "issues": issues}


def verify_component_source_alignment(
    results_df: Optional[pd.DataFrame],
    components_df: Optional[pd.DataFrame],
) -> Dict:
    if results_df is None or results_df.empty or components_df is None:
        return {
            "total_rows": 0,
            "verified_enabling": 0,
            "unverified_enabling": 0,
            "mismatched_enabling": 0,
            "verified_dependent": 0,
            "unverified_dependent": 0,
            "mismatched_dependent": 0,
            "issues": [],
        }

    comp_source: Dict[str, str] = {}
    for _, row in components_df.iterrows():
        name = str(row.get("Component", row.get("component_name", ""))).strip()
        src = str(row.get("Source", row.get("source_id", ""))).strip()
        if name and src:
            comp_source[name] = src

    verified_en = 0
    unver_en = 0
    mismatch_en = 0
    verified_de = 0
    unver_de = 0
    mismatch_de = 0
    issues: List[str] = []

    for idx, row in results_df.iterrows():
        en = str(row.get("Enabling Component", "")).strip()
        en_src = str(row.get("Enabling Source", "")).strip()
        de = str(row.get("Dependent Component", "")).strip()
        de_src = str(row.get("Dependent Source", "")).strip()

        if en in comp_source:
            exp = comp_source[en]
            if en_src == exp:
                verified_en += 1
            else:
                mismatch_en += 1
                issues.append(
                    f"Row {idx + 2}: Enabling Component '{en}' has source '{en_src}' but Components tab shows '{exp}'"
                )
        else:
            unver_en += 1

        if de in comp_source:
            expd = comp_source[de]
            if de_src == expd:
                verified_de += 1
            else:
                mismatch_de += 1
                issues.append(
                    f"Row {idx + 2}: Dependent Component '{de}' has source '{de_src}' but Components tab shows '{expd}'"
                )
        else:
            if de_src:
                verified_de += 1
            else:
                unver_de += 1
                if de:
                    issues.append(f"Row {idx + 2}: Dependent Component '{de}' has no Dependent Source")

    return {
        "total_rows": len(results_df),
        "verified_enabling": verified_en,
        "unverified_enabling": unver_en,
        "mismatched_enabling": mismatch_en,
        "verified_dependent": verified_de,
        "unverified_dependent": unver_de,
        "mismatched_dependent": mismatch_de,
        "issues": issues,
    }


def build_verification_issues_df(
    enforcement_report: Dict,
    verification_report: Dict,
) -> pd.DataFrame:
    cols = ["row", "issue_type", "component", "field", "expected", "actual", "message"]
    records: List[Dict[str, str]] = []

    records.append(
        {
            "row": "",
            "issue_type": "summary",
            "component": "",
            "field": "",
            "expected": "",
            "actual": "",
            "message": f"verification generated at {get_timestamp()}",
        }
    )

    records.append(
        {
            "row": "",
            "issue_type": "enforcement_summary",
            "component": "",
            "field": "",
            "expected": str(enforcement_report.get("total", "")),
            "actual": str(enforcement_report.get("enforced", "")),
            "message": "rows processed vs. fields enforced",
        }
    )

    for msg in enforcement_report.get("issues", []) or []:
        records.append(
            {
                "row": "",
                "issue_type": "enforcement_issue",
                "component": "",
                "field": "",
                "expected": "",
                "actual": "",
                "message": str(msg),
            }
        )

    for msg in verification_report.get("issues", []) or []:
        text = str(msg)
        m_row = re.search(r"Row (\d+)", text)
        m_comp = re.search(r"'([^']+)'", text)
        rownum = m_row.group(1) if m_row else ""
        comp = m_comp.group(1) if m_comp else ""
        records.append(
            {
                "row": rownum,
                "issue_type": "verification_issue",
                "component": comp,
                "field": "",
                "expected": "",
                "actual": "",
                "message": text,
            }
        )

    if len(records) == 2:
        records.append(
            {
                "row": "",
                "issue_type": "note",
                "component": "",
                "field": "",
                "expected": "",
                "actual": "",
                "message": "NO ISSUES FOUND",
            }
        )

    return pd.DataFrame.from_records(records, columns=cols)


# ---------------------------------------------------------------------------
# Inference engine
# ---------------------------------------------------------------------------


def infer_alignments(
    components_df: Optional[pd.DataFrame],
    tobecrosswalked_df: Optional[pd.DataFrame],
    model: Dict,
    threshold: float,
    components_lookup_df: Optional[pd.DataFrame] = None,  # unused but kept
    sources_df: Optional[pd.DataFrame] = None,
    alignments_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    # Dynamically get output columns from Alignments tab
    if alignments_df is not None:
        output_columns = list(alignments_df.columns)
        if "SimilarityTimesConfidence" not in output_columns:
            output_columns.append("SimilarityTimesConfidence")
    else:
        output_columns = [
            "Enabling Source",
            "Enabling Component",
            "Enabling Component Description",
            "Dependent Component",
            "Dependent Component Description",
            "Dependent Source",
            "Linkage mandated by what US Code or OMB policy?",
            "Enabling Component URL",
            "Dependent Component URL",
            "Enabling Source Agency",
            "Dependent Source Agency",
            "Notes and keywords",
            "Keywords Tab Items Found",
            "Enabling Component Office of Primary Interest",
            "Dependent Component Office of Primary Interest",
            "Edits",
            "Valid",
            "Similarity",
            "Confidence",
            "Transitive Support",
            "Matched Enabling Index",
            "Matched Dependent Index",
            "Alignment Rationale",
            "Enabling Fetch Status",
            "Dependent Fetch Status",
            "SimilarityTimesConfidence",
        ]

    if components_df is None or tobecrosswalked_df is None:
        return pd.DataFrame(columns=output_columns)

    comp_rows = components_df.to_dict("records")
    tobe_rows = tobecrosswalked_df.to_dict("records")
    print_verbose(f"Components rows: {len(comp_rows)}; ToBeCrosswalked rows: {len(tobe_rows)}")

    # Source lookup
    source_lookup: Dict[str, Dict] = {}
    if sources_df is not None:
        for row in sources_df.to_dict("records"):
            sid = str(row.get("source_id", row.get("Source", ""))).strip()
            if sid:
                source_lookup[sid] = row

    # Component metadata lookup (Components first, then ToBeCrosswalked)
    all_components: Dict[str, Dict[str, str]] = {}
    for row in comp_rows:
        name = str(row.get("Component", row.get("component_name", ""))).strip()
        if not name:
            continue
        sid = str(row.get("Source", row.get("source_id", ""))).strip()
        all_components[name] = {
            "source_id": sid,
            "description": str(row.get("Component Description", row.get("component_description", ""))).strip(),
            "url": str(row.get("Component URL", row.get("component_url", ""))).strip(),
            "office": str(row.get("Component Office of Primary Interest", "")).strip(),
        }
    for row in tobe_rows:
        name = str(row.get("Component", "")).strip()
        if not name or name in all_components:
            continue
        sid = str(row.get("Source", row.get("source_id", ""))).strip()
        all_components[name] = {
            "source_id": sid,
            "description": str(row.get("Component Description", "")).strip(),
            "url": str(row.get("Component URL", "")).strip(),
            "office": str(row.get("Component Office of Primary Interest", "")).strip(),
        }

    # Graph and classifier
    graph = nx.Graph()
    graph.add_edges_from(model.get("graph_edges", []))

    pairs, pair_meta = build_pairs_to_predict(components_df, tobecrosswalked_df)
    total = len(pairs)
    print_verbose(f"Candidate pairs to score: {total:,}")
    if total == 0:
        return pd.DataFrame(columns=output_columns)

    batch_size = 100000
    features: List[List[float]] = []
    start = time.time()
    for i in range(0, total, batch_size):
        batch = pairs[i : i + batch_size]
        feats = extract_features(batch, graph)
        features.extend(feats.tolist())
        done = i + len(batch)
        elapsed = time.time() - start
        percent = 100.0 * done / total
        print_verbose(
            f"Feature extraction: {percent:5.1f}% ({done:,}/{total:,}) elapsed {format_time(elapsed)}"
        )

    X = np.array(features, dtype=float)

    clf_obj = model.get("clf_obj")
    probs = np.zeros(total, dtype=float)
    if clf_obj and isinstance(clf_obj, dict) and "clf" in clf_obj and "scaler" in clf_obj:
        scaler = clf_obj["scaler"]
        clf = clf_obj["clf"]
        for i in range(0, total, batch_size):
            Xb = X[i : i + batch_size]
            try:
                Xs = scaler.transform(Xb)
                probs[i : i + batch_size] = clf.predict_proba(Xs)[:, 1]
            except Exception:
                probs[i : i + batch_size] = np.random.rand(Xb.shape[0])
    else:
        print_verbose("No deterministic classifier; using random probabilities.")
        probs = np.random.rand(total)

    # Alignments lookup for copying optional fields
    align_lookup: Dict[Tuple[str, str], Dict] = {}
    if alignments_df is not None:
        for row in alignments_df.to_dict("records"):
            a = str(row.get("Enabling Component", row.get("enabling_component", ""))).strip()
            d = str(row.get("Dependent Component", row.get("dependent_component", ""))).strip()
            if a and d:
                align_lookup[(a, d)] = row

    def pick(row_dict: Dict, *keys: str) -> str:
        for k in keys:
            v = str(row_dict.get(k, "")).strip()
            if v:
                return v
        return ""

    def get_agency(sid: str) -> str:
        if not sid:
            return ""
        row = source_lookup.get(sid) or {}
        return (
            str(row.get("source_agency", ""))
            or str(row.get("Source Agency", ""))
            or str(row.get("agency", ""))
            or str(row.get("agency_name", ""))
        ).strip()

    results: List[Dict[str, object]] = []
    kept = 0
    for i, prob in enumerate(probs):
        if prob < threshold:
            continue
        kept += 1
        en = pair_meta[i]["Enabling Component"]
        de = pair_meta[i]["Dependent Component"]

        en_meta = all_components.get(en, {})
        de_meta = all_components.get(de, {})

        enabling_source = en_meta.get("source_id", "")
        dependent_source = de_meta.get("source_id", "")

        if not dependent_source:
            for row in tobe_rows:
                if str(row.get("Component", "")).strip() == de:
                    dependent_source = str(row.get("Source", "")).strip()
                    break

        # If the pair was in the original alignments, use its data as a base
        al_row = align_lookup.get((en, de), {})
        rec = al_row.copy()

        # Always overwrite with freshly calculated values for consistency
        sim_num = similar(en, de)
        conf_num = prob
        sim_x_conf = sim_num * conf_num

        rec.update({
            "Enabling Source": enabling_source,
            "Enabling Component": en,
            "Enabling Component Description": en_meta.get("description", ""),
            "Dependent Component": de,
            "Dependent Component Description": de_meta.get("description", ""),
            "Dependent Source": dependent_source,
            "Enabling Component URL": en_meta.get("url", ""),
            "Dependent Component URL": de_meta.get("url", ""),
            "Enabling Source Agency": get_agency(enabling_source),
            "Dependent Source Agency": get_agency(dependent_source),
            "Enabling Component Office of Primary Interest": en_meta.get("office", ""),
            "Dependent Component Office of Primary Interest": de_meta.get("office", ""),
            "Similarity": sim_num,
            "Confidence": conf_num,
            "SimilarityTimesConfidence": sim_x_conf,
        })
        results.append(rec)

    print_verbose(f"Kept {kept:,} inferred alignments above threshold {threshold:.3f}")

    df = pd.DataFrame(results)
    for col in output_columns:
        if col not in df.columns:
            df[col] = ""
    df = df[output_columns]
    return df


# ---------------------------------------------------------------------------
# Excel output
# ---------------------------------------------------------------------------


MAX_EXCEL_ROWS = 1_048_575 - 1  # leave room for header


def write_output_excel(
    output_path: Path,
    inferred_alignments: pd.DataFrame,
    verification_issues_df: Optional[pd.DataFrame],
) -> None:
    """Write dataframes to an Excel file with robust sanitization."""
    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            # Sanitize and write inferred alignments
            safe_inferred = sanitize_df_for_excel(inferred_alignments)
            if safe_inferred is None:  # Fallback if sanitizer fails
                print_verbose("Warning: Sanitizer failed for Inferred_Alignments, attempting direct write.")
                safe_inferred = inferred_alignments
            
            if len(safe_inferred) > MAX_EXCEL_ROWS:
                print_verbose(f"Warning: Truncating Inferred_Alignments from {len(safe_inferred)} to {MAX_EXCEL_ROWS} rows for Excel.")
                safe_inferred = safe_inferred.head(MAX_EXCEL_ROWS)

            safe_inferred.to_excel(writer, sheet_name="Inferred_Alignments"[:31], index=False)
            print_verbose(f"Wrote {len(safe_inferred)} rows to Inferred_Alignments sheet.")

            # Sanitize and write verification issues
            if verification_issues_df is None:
                verification_issues_df = pd.DataFrame(columns=["row", "issue_type", "message"])

            safe_ver = sanitize_df_for_excel(verification_issues_df)
            if safe_ver is None: # Fallback if sanitizer fails
                print_verbose("Warning: Sanitizer failed for verification_issues, attempting direct write.")
                safe_ver = verification_issues_df

            safe_ver.to_excel(writer, sheet_name="verification_issues"[:31], index=False)
            print_verbose(f"Wrote {len(safe_ver)} rows to verification_issues sheet.")

    except Exception as e:
        import traceback
        print_verbose(f"Error during Excel write to '{output_path}': {e}")
        traceback.print_exc()
        # Attempt to save as CSV as a fallback
        try:
            csv_path = output_path.with_suffix('.csv')
            print_verbose(f"Attempting to save main results as CSV to: {csv_path}")
            inferred_alignments.to_csv(csv_path, index=False)
            print_verbose("CSV fallback save successful.")
        except Exception as csv_e:
            print_verbose(f"CSV fallback save also failed: {csv_e}")
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def ask_confidence_threshold(default: float) -> float:
    while True:
        s = input(f"Enter confidence threshold [0-1] (Enter to accept {default:.3f}): ").strip()
        if not s:
            return default
        try:
            v = float(s)
            if 0.0 <= v <= 1.0:
                return v
        except Exception:
            pass
        print("Invalid threshold; please enter a number between 0 and 1.")


def main(argv: Optional[Sequence[str]] = None) -> int:

    parser = argparse.ArgumentParser(description="IVN Component Crosswalk Inference")
    parser.add_argument("--threshold", type=float, help="Confidence threshold [0-1]")
    parser.add_argument("--yes", action="store_true", help="Automatically accept recommended threshold")
    args = parser.parse_args(argv)

    script_dir = Path(__file__).parent
    print_verbose(f"Script running in: {script_dir}")

    excel_file_path = script_dir / "ivntest.xlsx"
    if not excel_file_path.exists():
        print_verbose(f"Input workbook not found at {excel_file_path}")
        return 1

    model_output_path = script_dir / f"crosswalk_inferences_training({get_timestamp()}).json"
    clf_output_path = model_output_path.with_suffix(".joblib")
    output_excel_path = script_dir / f"IVN_Component_Crosswalk_Output({get_timestamp()}).xlsx"

    training_file_path_str = ask_file_path_from_list(script_dir)

    print_verbose(f"Loading workbook '{excel_file_path.name}'...")
    all_tabs = load_all_excel_tabs(excel_file_path)

    components_df = get_tab_by_name(all_tabs, "Components")
    tobecrosswalked_df = get_tab_by_name(all_tabs, "ToBeCrosswalked")
    alignments_df = get_tab_by_name(all_tabs, "Alignments")
    nonaligned_df = get_tab_by_name(all_tabs, "Nonaligned-Edge-Cases")
    sources_df = get_tab_by_name(all_tabs, "Sources")

    if components_df is None or tobecrosswalked_df is None:
        print_verbose("Missing required Components or ToBeCrosswalked sheet; exiting.")
        return 1

    # Load or train model
    if training_file_path_str:
        training_file_path = Path(training_file_path_str)
        print_verbose(f"Loading model from {training_file_path}...")
        model = load_model_json(training_file_path)
        clf_path = training_file_path.with_suffix(".joblib")
        if clf_path.exists():
            try:
                clf_obj = joblib.load(clf_path)
                model["clf_obj"] = clf_obj
                print_verbose(f"Loaded classifier object from {clf_path.name}")
            except Exception as exc:  # pragma: no cover - defensive logging
                print_verbose(
                    f"Failed to load classifier .joblib at '{clf_path}'; "
                    f"using random scores instead. Error: {type(exc).__name__}: {exc}"
                )
                print_verbose(
                    "Common causes: file moved/deleted, version mismatch between scikit-learn/joblib, "
                    "or a corrupted .joblib file."
                )
        else:
            print_verbose(f"No classifier .joblib found at '{clf_path}'; will use random scores if needed.")
    else:
        print_verbose("No training JSON selected; training a new model...")
        components_list = sorted(
            set(
                str(r.get("Component", r.get("component_name", ""))).strip()
                for r in components_df.to_dict("records")
            )
        )
        model = train_alignment_model(alignments_df, nonaligned_df, components_list)
        print_verbose(f"Saving trained model JSON to {model_output_path.name}")
        save_model_json(model, model_output_path)
        if model.get("clf_obj"):
            try:
                joblib.dump(model["clf_obj"], clf_output_path)
                print_verbose(f"Saved classifier object to {clf_output_path.name}")
            except Exception:
                print_verbose("Failed to save classifier .joblib; continuing without.")

    # Threshold selection
    pairs, _ = build_pairs_to_predict(components_df, tobecrosswalked_df)
    total_pairs = len(pairs)
    print_verbose(f"Total candidate pairs: {total_pairs:,}")

    if total_pairs == 0:
        print_verbose("No candidate pairs; writing empty output.")
        empty_df = infer_alignments(components_df, tobecrosswalked_df, model, 1.0, sources_df=sources_df, alignments_df=alignments_df)
        ver_report = verify_component_source_alignment(empty_df, components_df)
        ver_df = build_verification_issues_df({"total": 0, "enforced": 0, "issues": []}, ver_report)
        write_output_excel(output_excel_path, empty_df, ver_df)
        print_verbose(f"Output written to {output_excel_path}")
        return 0

    if args.threshold is not None:
        threshold = float(args.threshold)
    else:
        est_thr, est_rows_safe, sample_n, pct = estimate_threshold_from_sample(pairs, model)
        print_verbose(
            f"Sampled {sample_n} pairs; recommended threshold ~{est_thr:.3f} (safe est rows {est_rows_safe:,})"
        )
        for p, info in sorted(pct.items()):
            print_verbose(
                f"Percentile {p:.2f}: thr={info['threshold']:.3f}, "
                f"rows~{info['estimated_rows']:,}, safe~{info['estimated_rows_safe']:,}"
            )
        if args.yes:
            threshold = est_thr
        else:
            threshold = ask_confidence_threshold(est_thr)

    print_verbose(f"Using confidence threshold {threshold:.3f}")

    inferred = infer_alignments(
        components_df,
        tobecrosswalked_df,
        model,
        threshold,
        sources_df=sources_df,
        alignments_df=alignments_df,
    )

    if len(inferred) > MAX_EXCEL_ROWS:
        print_verbose(
            f"Inferred rows {len(inferred):,} exceed Excel limit {MAX_EXCEL_ROWS:,}; user decision required."
        )
        print("1: Enter a higher threshold and rerun once")
        print("2: Truncate to Excel row limit")
        print("3: Abort")
        choice = input("Enter 1, 2, or 3: ").strip()
        if choice == "1":
            new_thr = ask_confidence_threshold(min(1.0, threshold + 0.05))
            print_verbose(f"Re-running inference with threshold {new_thr:.3f}")
            inferred = infer_alignments(
                components_df,
                tobecrosswalked_df,
                model,
                new_thr,
                sources_df=sources_df,
                alignments_df=alignments_df,
            )
        elif choice == "2":
            inferred = inferred.head(MAX_EXCEL_ROWS)
        else:
            print_verbose("User chose to abort due to Excel row limit.")
            return 0

    if not inferred.empty:
        enforced_df, enforcement_report = enforce_component_metadata(
            inferred, components_df, tobecrosswalked_df, sources_df
        )
        ver_report = verify_component_source_alignment(enforced_df, components_df)
        print_verbose(
            f"Verification: total={ver_report['total_rows']}, "
            f"enabling ok={ver_report['verified_enabling']}, enabling mismatched={ver_report['mismatched_enabling']}, "
            f"dependent ok={ver_report['verified_dependent']}, dependent mismatched={ver_report['mismatched_dependent']}"
        )
        for msg in ver_report.get("issues", [])[:20]:
            print_verbose(f"Issue: {msg}")
        ver_df = build_verification_issues_df(enforcement_report, ver_report)
        final_df = enforced_df
    else:
        print_verbose("No inferred alignments above threshold.")
        final_df = inferred
        ver_df = build_verification_issues_df({"total": 0, "enforced": 0, "issues": []}, {
            "total_rows": 0,
            "verified_enabling": 0,
            "unverified_enabling": 0,
            "mismatched_enabling": 0,
            "verified_dependent": 0,
            "unverified_dependent": 0,
            "mismatched_dependent": 0,
            "issues": [],
        })

    output_filename = f"IVN_Component_Crosswalk_Output({get_timestamp()}).xlsx"
    output_path = script_dir / output_filename
    write_output_excel(output_path, final_df, ver_df)
    print_verbose(f"Output successfully written to {output_path}")

    print_verbose("Done.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
