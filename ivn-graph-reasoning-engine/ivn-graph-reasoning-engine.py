#!/usr/bin/env python3
"""
Menu-driven workflow manager for script/support-file synchronization.

This tool maintains the following companion files for each current code file:
- <script>_prompt.txt
- <script>_reasoning.txt
- <script>_internal_state.txt

Legacy script versions are excluded if they match either pattern:
- <name>-YYYY-MM-DD-HH-MM.ext
- <name>YYYYMMDDHHMM.ext
"""

from __future__ import annotations

import ast
import datetime as dt
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import json
from collections import defaultdict

CODE_EXTENSIONS = {
    ".py",
    ".ps1",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".java",
    ".cs",
    ".go",
    ".rb",
    ".php",
    ".rs",
    ".sh",
    ".sql",
}

SUPPORT_SUFFIXES = ("_prompt.txt", "_reasoning.txt", "_internal_state.txt")
STATE_MARKER = "STATE_JSON:"
EXCLUDED_DIR_NAMES = {
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "venv",
    "env",
    "node_modules",
    "__pycache__",
}


@dataclass
class ScriptBundle:
    code_path: Path
    prompt_path: Path
    reasoning_path: Path
    internal_state_path: Path


def utc_now_text() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def is_support_file(path: Path) -> bool:
    return any(path.name.endswith(suffix) for suffix in SUPPORT_SUFFIXES)


def is_legacy_script(path: Path) -> bool:
    stem = path.stem
    # Pattern: myscript-2026-02-18-15-30
    if re.search(r"-\d{4}-\d{2}-\d{2}-\d{2}-\d{2}$", stem):
        return True
    # Pattern: myscript202602181530
    if re.search(r"\d{12}$", stem):
        return True
    return False


def is_code_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in CODE_EXTENSIONS and not is_support_file(path)


def build_bundle(code_path: Path) -> ScriptBundle:
    base = code_path.with_suffix("")
    return ScriptBundle(
        code_path=code_path,
        prompt_path=Path(str(base) + "_prompt.txt"),
        reasoning_path=Path(str(base) + "_reasoning.txt"),
        internal_state_path=Path(str(base) + "_internal_state.txt"),
    )


def discover_current_scripts(root: Path) -> List[Path]:
    scripts: List[Path] = []
    for path in root.rglob("*"):
        if any(part in EXCLUDED_DIR_NAMES for part in path.parts):
            continue
        if is_code_file(path) and not is_legacy_script(path):
            scripts.append(path)
    return sorted(scripts)


def read_text_or_empty(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def load_state(path: Path) -> Dict[str, object]:
    text = read_text_or_empty(path)
    if not text.strip():
        return {"files": {}, "last_sync_utc": "", "notes": []}

    marker_index = text.find(STATE_MARKER)
    if marker_index == -1:
        return {"files": {}, "last_sync_utc": "", "notes": ["State marker missing; reinitialized."]}

    payload = text[marker_index + len(STATE_MARKER) :].strip()
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return {"files": {}, "last_sync_utc": "", "notes": ["State JSON invalid; reinitialized."]}

    if not isinstance(data, dict):
        return {"files": {}, "last_sync_utc": "", "notes": ["State format invalid; reinitialized."]}

    data.setdefault("files", {})
    data.setdefault("last_sync_utc", "")
    data.setdefault("notes", [])
    return data


def save_state(path: Path, data: Dict[str, object], script_name: str) -> None:
    header = [
        f"Internal state for {script_name}",
        "Do not store hidden chain-of-thought. Keep concise operational notes only.",
        f"Updated: {utc_now_text()}",
        "",
        STATE_MARKER,
        json.dumps(data, indent=2, sort_keys=True),
        "",
    ]
    write_text(path, "\n".join(header))


def safe_python_summary(code_path: Path) -> Tuple[List[str], List[str]]:
    """Return (function_names, class_names) for Python files."""
    source = read_text_or_empty(code_path)
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return [], []

    funcs: List[str] = []
    classes: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            funcs.append(node.name)
        elif isinstance(node, ast.AsyncFunctionDef):
            funcs.append(node.name)
        elif isinstance(node, ast.ClassDef):
            classes.append(node.name)

    return sorted(set(funcs)), sorted(set(classes))


def generate_prompt_text(code_path: Path) -> str:
    script_name = code_path.name
    extension = code_path.suffix.lower()
    funcs: List[str] = []
    classes: List[str] = []

    if extension == ".py":
        funcs, classes = safe_python_summary(code_path)

    prompt_lines = [
        f"Prompt specification for {script_name}",
        "",
        "1) Problem and context",
        "This script exists to keep code and companion support files synchronized in a transparent, auditable workflow.",
        "It scans the workspace for current scripts, excludes timestamped legacy versions, and ensures each current script has prompt, reasoning, and internal-state files.",
        "",
        "2) Why this script exists",
        "It reduces drift between implementation and documentation by updating support files when scripts change and flagging manual reconciliation when prompt files change.",
        "",
        "3) Required concepts",
        "- Filesystem traversal and pattern filtering",
        "- Deterministic naming conventions for companion files",
        "- State tracking with file modification times",
        "- Append-only reasoning logs with UTC timestamps",
        "- Safe documentation logs that avoid hidden chain-of-thought",
        "",
        "4) Domain terms",
        "- Current script: non-legacy code file in scope",
        "- Legacy script: timestamped historical file to ignore",
        "- Prompt file: authoritative plain-English specification",
        "- Reasoning log: concise append-only event summary",
        "- Internal state: persistent sync metadata and diagnostics",
        "",
        "5) High-level behavior",
        "- Discover code files in this folder tree",
        "- Ignore support files and legacy timestamped versions",
        "- Create missing support files for each current script",
        "- Track mtimes and synchronize prompt/internal-state content",
        "- Provide a menu for sync, logging, and status reporting",
        "",
        "6) Inputs and outputs",
        "Input interfaces:",
        "- Interactive menu choices",
        "- Optional script path and speaker/text for reasoning entries",
        "",
        "Output artifacts:",
        "- <script>_prompt.txt",
        "- <script>_reasoning.txt",
        "- <script>_internal_state.txt",
        "- Console status lines",
        "",
        "7) Success criteria",
        "A successful run creates or updates required support files for all current scripts and records stable state metadata.",
        "Prompt files must remain sufficient for a naive model to reconstruct script intent and structure.",
        "",
        "8) Failure handling",
        "- Invalid menu input should be rejected and retried",
        "- Parse failures should degrade gracefully to generic summaries",
        "- Corrupt internal state should be reinitialized with a note",
        "",
        "9) Validation checks",
        "- Every current script has exactly one prompt, one reasoning log, and one internal-state file",
        "- No support files are generated for timestamped legacy scripts",
        "- Reasoning entries are append-only and UTC-stamped",
        "",
        "10) Learning progression",
        "Understanding improves as reasoning entries accumulate concrete user goals, code updates, and verification outcomes.",
        "The prompt should be revised whenever code behavior or interfaces change.",
    ]

    if classes:
        prompt_lines.extend(["", "11) Classes detected", *[f"- {name}" for name in classes]])
    if funcs:
        prompt_lines.extend(["", "12) Functions detected", *[f"- {name}" for name in funcs]])

    return "\n".join(prompt_lines) + "\n"


def append_reasoning_entry(bundle: ScriptBundle, speaker: str, message: str) -> None:
    timestamp = utc_now_text().replace(" UTC", "")
    entry = f"[{timestamp}] {speaker}: {message}\n"
    with bundle.reasoning_path.open("a", encoding="utf-8") as handle:
        handle.write(entry)


def ensure_support_files(bundle: ScriptBundle) -> None:
    if not bundle.reasoning_path.exists():
        write_text(bundle.reasoning_path, "")
    if not bundle.prompt_path.exists():
        write_text(bundle.prompt_path, generate_prompt_text(bundle.code_path))
    if not bundle.internal_state_path.exists():
        initial_state = {"files": {}, "last_sync_utc": "", "notes": ["Initialized state file."]}
        save_state(bundle.internal_state_path, initial_state, bundle.code_path.name)


def file_mtime(path: Path) -> float:
    return path.stat().st_mtime if path.exists() else 0.0


def sync_bundle(bundle: ScriptBundle) -> Dict[str, str]:
    ensure_support_files(bundle)

    state = load_state(bundle.internal_state_path)
    files_state: Dict[str, float] = {
        "code": file_mtime(bundle.code_path),
        "prompt": file_mtime(bundle.prompt_path),
        "reasoning": file_mtime(bundle.reasoning_path),
        "internal_state": file_mtime(bundle.internal_state_path),
    }

    previous_files = state.get("files", {}) if isinstance(state.get("files", {}), dict) else {}
    prev_code = float(previous_files.get("code", 0.0))
    prev_prompt = float(previous_files.get("prompt", 0.0))

    status = {
        "script": str(bundle.code_path),
        "prompt_action": "none",
        "code_action": "none",
        "note": "",
    }

    # Code changed -> regenerate prompt.
    if files_state["code"] > prev_code:
        write_text(bundle.prompt_path, generate_prompt_text(bundle.code_path))
        status["prompt_action"] = "regenerated_from_code"

    # Prompt changed after the previous sync -> manual reconcile note.
    files_state["prompt"] = file_mtime(bundle.prompt_path)
    if files_state["prompt"] > prev_prompt and status["prompt_action"] == "none":
        notes = state.get("notes", []) if isinstance(state.get("notes", []), list) else []
        notes.append("Prompt changed externally. Manual code reconciliation required.")
        state["notes"] = notes[-50:]
        status["code_action"] = "manual_reconcile_required"
        status["note"] = "Prompt changed externally; review code to match prompt requirements."

    state["files"] = {
        "code": file_mtime(bundle.code_path),
        "prompt": file_mtime(bundle.prompt_path),
        "reasoning": file_mtime(bundle.reasoning_path),
        "internal_state": file_mtime(bundle.internal_state_path),
    }
    state["last_sync_utc"] = utc_now_text()

    save_state(bundle.internal_state_path, state, bundle.code_path.name)
    return status


def sync_workspace(root: Path) -> List[Dict[str, str]]:
    scripts = discover_current_scripts(root)
    results: List[Dict[str, str]] = []
    for script in scripts:
        bundle = build_bundle(script)
        result = sync_bundle(bundle)
        results.append(result)
    return results


def print_status_rows(rows: Iterable[Dict[str, str]]) -> None:
    print("\nSynchronization summary")
    print("-" * 72)
    for row in rows:
        print(f"Script: {row.get('script', '')}")
        print(f"  prompt_action: {row.get('prompt_action', '')}")
        print(f"  code_action:   {row.get('code_action', '')}")
        note = row.get("note", "")
        if note:
            print(f"  note:          {note}")
    print("-" * 72)


def resolve_script(root: Path, raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        path = (root / path).resolve()
    return path


def detect_relevant_script(chat_text: str, scripts: List[Path]) -> Optional[Path]:
    if not chat_text.strip() or not scripts:
        return None

    lowered = chat_text.lower()

    # Highest confidence: explicit filename mention.
    for script in scripts:
        if script.name.lower() in lowered:
            return script

    # Secondary confidence: stem token overlap.
    scored: List[Tuple[int, Path]] = []
    for script in scripts:
        tokens = [t for t in re.split(r"[^a-zA-Z0-9]+", script.stem.lower()) if t]
        score = sum(1 for t in tokens if len(t) >= 3 and t in lowered)
        if score > 0:
            scored.append((score, script))

    if not scored:
        return None

    scored.sort(key=lambda item: (-item[0], str(item[1]).lower()))
    return scored[0][1]


def menu(root: Path) -> None:
    while True:
        print("\n=== Synchronization Manager ===")
        print("1) Scan and synchronize workspace")
        print("2) Append reasoning log entry")
        print("3) Append chat entry (auto-detect relevant script)")
        print("4) List current scripts")
        print("5) Exit")
        choice = input("Select option: ").strip()

        if choice == "1":
            rows = sync_workspace(root)
            print_status_rows(rows)
        elif choice == "2":
            raw_script = input("Script path (absolute or relative): ").strip()
            speaker = input("Speaker (User/Assistant/System): ").strip() or "System"
            message = input("Concise summary message: ").strip()

            script_path = resolve_script(root, raw_script)
            if not script_path.exists() or not is_code_file(script_path) or is_legacy_script(script_path):
                print("Invalid script path or legacy file; no update made.")
                continue

            bundle = build_bundle(script_path)
            ensure_support_files(bundle)
            append_reasoning_entry(bundle, speaker, message)
            sync_bundle(bundle)
            print("Reasoning entry appended and bundle synchronized.")
        elif choice == "3":
            speaker = input("Speaker (User/Assistant/System): ").strip() or "System"
            message = input("Chat entry text: ").strip()
            scripts = discover_current_scripts(root)
            script_path = detect_relevant_script(message, scripts)
            if script_path is None:
                print("Could not auto-detect a relevant script. Use option 2 with an explicit script path.")
                continue

            bundle = build_bundle(script_path)
            ensure_support_files(bundle)
            append_reasoning_entry(bundle, speaker, message)
            sync_bundle(bundle)
            print(f"Auto-detected script: {script_path}")
            print("Reasoning entry appended and bundle synchronized.")
        elif choice == "4":
            scripts = discover_current_scripts(root)
            if not scripts:
                print("No current scripts found.")
            else:
                for script in scripts:
                    print(script)
        elif choice == "5":
            print("Exiting.")
            return
        else:
            print("Invalid option. Enter 1, 2, 3, 4, or 5.")


def ensure_main_entry_script(root: Path) -> None:
    expected = root / f"{root.name}.py"
    if expected.exists():
        return
    # This script itself should be named as the folder. If copied/renamed,
    # create a compatibility notice file for visibility.
    write_text(
        expected,
        "# Entry script expected to be named as the folder.\n"
        "# If you see this file, copy the workflow manager code into this file and run again.\n",
    )


def main() -> None:
    root = Path(__file__).resolve().parent
    ensure_main_entry_script(root)
    rows = sync_workspace(root)
    print_status_rows(rows)
    menu(root)


if __name__ == "__main__":
    main()
