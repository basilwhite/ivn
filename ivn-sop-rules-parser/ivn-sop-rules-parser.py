#!/usr/bin/env python3
"""
IVN SOP Rules Parser Workflow Manager

This script maintains a single-script workflow with companion files:
- <script>_prompt.txt
- <script>_reasoning.txt
- <script>_internal_state.txt

Important: This tool stores concise decision summaries, not hidden model scratchpad.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

OLD_VERSION_PATTERNS = [
    re.compile(r".+-\d{4}-\d{2}-\d{2}-\d{2}-\d{2}\.py$"),
    re.compile(r".+\d{12}\.py$"),
]


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def is_old_version_script(path: Path) -> bool:
    return any(p.match(path.name) for p in OLD_VERSION_PATTERNS)


def folder_script_name(workspace: Path) -> str:
    return f"{workspace.name}.py"


def companion_paths(script_path: Path) -> Dict[str, Path]:
    stem = script_path.stem
    return {
        "prompt": script_path.with_name(f"{stem}_prompt.txt"),
        "reasoning": script_path.with_name(f"{stem}_reasoning.txt"),
        "internal": script_path.with_name(f"{stem}_internal_state.txt"),
        "state_json": script_path.with_name(f"{stem}_sync_state.json"),
    }


def read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8", newline="\n")


def append_text(path: Path, content: str) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as f:
        f.write(content)


def default_prompt_template(script_name: str) -> str:
    return f"""Title: Specification for {script_name}

Overarching Problem
- Define and enforce a repeatable workflow where a single Python script and companion text files remain synchronized.
- Support IVN-related data operations while preserving traceability and reproducibility.

Broader Context
- Users need consistent add/edit/query/report behavior and durable operational memory.
- Companion files help a simple model or analyst reconstruct intent, behavior, and expected outcomes.

How Understanding Improves Over Time
- The reasoning log captures concise user-assistant decision history.
- The internal state log captures durable operational state and policy flags.
- This prompt file is refreshed to match the current script behavior.

Concepts To Understand
- Single-script workflow ownership.
- Append-only logs for historical traceability.
- File synchronization checks using content hashes.
- Rule precedence and escalation handling.

Prerequisite Knowledge
- Basic Python execution from command line.
- Plain text file editing.
- Simple understanding of hash-based integrity checks.

Domain Terms
- Script file: the runnable Python file.
- Prompt file: plain-English specification for regeneration.
- Reasoning log: concise decision history (not hidden scratchpad).
- Internal state log: persistent operational state updates.
- Output artifact: any file produced by script runs.

High-Level Behavior
- Initialize companion files.
- Append chat entries and decision notes.
- Record code changes and output artifact paths.
- Check and repair synchronization metadata.

Why This Script Exists
- To make operational behavior explicit and auditable.
- To keep specification and implementation aligned.

Role Of The Script
- Orchestrator for logging, synchronization checks, and status reporting.

Input Definitions
- Interactive menu selections.
- Optional script path override.
- Chat message text.
- Output artifact file path.

Output Definitions
- Updated prompt/reasoning/internal-state files.
- Updated sync metadata JSON.
- Console status messages indicating actions taken.

Final Output Structure And Validity Checks
- Prompt file exists and contains all required sections.
- Reasoning log is append-only with timestamped entries.
- Internal state log is append-only with timestamped updates.
- Sync state file includes current hashes of managed files.

Successful Outcome Criteria
- Companion files exist for the active script.
- Every operation appends or updates the expected records.
- Sync check reports no unresolved mismatches.

Error Handling
- Reject old-version scripts by filename pattern.
- Warn when referenced output artifact does not exist.
- Continue safely when optional files are absent.

How To Evaluate Correctness
- Run sync check after each operation.
- Verify timestamps and append-only behavior in logs.
- Verify hashes change only when related files change.

Example Scenarios
1) Initialize workspace:
   - Choose Initialize.
   - Verify companion files were created.
2) Record a user request:
   - Choose Append chat entry.
   - Verify reasoning and internal state logs were appended.
3) Register output artifact:
   - Choose Record output artifact and provide a path.
   - Verify state metadata tracks the artifact.

Expected Learning Model Behavior
- Follow prompt file as authoritative plain-English specification.
- Generate code consistent with this specification.
- Preserve append-only logs and synchronization checks.
"""


def ensure_companions(script_path: Path) -> Dict[str, Path]:
    if is_old_version_script(script_path):
        raise ValueError("Refusing to create companion files for old-version script.")

    paths = companion_paths(script_path)

    if not script_path.exists():
        raise FileNotFoundError(f"Script file not found: {script_path}")

    if not paths["prompt"].exists():
        write_text(paths["prompt"], default_prompt_template(script_path.name))

    if not paths["reasoning"].exists():
        write_text(paths["reasoning"], "Reasoning Log\n")

    if not paths["internal"].exists():
        write_text(paths["internal"], "Internal State Log\n")

    if not paths["state_json"].exists():
        write_text(paths["state_json"], json.dumps({"output_artifacts": []}, indent=2) + "\n")

    return paths


def load_sync_state(path: Path) -> Dict[str, object]:
    try:
        data = json.loads(read_text(path) or "{}")
        if not isinstance(data, dict):
            return {"output_artifacts": []}
        data.setdefault("output_artifacts", [])
        return data
    except json.JSONDecodeError:
        return {"output_artifacts": []}


def save_sync_state(path: Path, state: Dict[str, object]) -> None:
    write_text(path, json.dumps(state, indent=2, sort_keys=True) + "\n")


def file_hash(path: Path) -> str:
    return sha256_text(read_text(path)) if path.exists() else ""


def refresh_sync_hashes(script_path: Path, paths: Dict[str, Path]) -> None:
    state = load_sync_state(paths["state_json"])
    state["last_sync_utc"] = utc_now()
    state["hashes"] = {
        "script": file_hash(script_path),
        "prompt": file_hash(paths["prompt"]),
        "reasoning": file_hash(paths["reasoning"]),
        "internal": file_hash(paths["internal"]),
    }
    save_sync_state(paths["state_json"], state)


def append_reasoning(paths: Dict[str, Path], speaker: str, message: str) -> None:
    entry = f"[{utc_now()}] {speaker}: {message}\n"
    append_text(paths["reasoning"], entry)


def append_internal(paths: Dict[str, Path], section: str, detail: str) -> None:
    entry = (
        f"=== UPDATE [{utc_now()}] ===\n"
        f"Section: {section}\n"
        f"Detail: {detail}\n"
        "=== END UPDATE ===\n"
    )
    append_text(paths["internal"], entry)


def record_output_artifact(paths: Dict[str, Path], artifact_path: str) -> None:
    state = load_sync_state(paths["state_json"])
    artifacts: List[str] = [str(x) for x in state.get("output_artifacts", [])]
    if artifact_path not in artifacts:
        artifacts.append(artifact_path)
    state["output_artifacts"] = artifacts
    state["last_output_update_utc"] = utc_now()
    save_sync_state(paths["state_json"], state)


def sync_check(script_path: Path, paths: Dict[str, Path]) -> Tuple[bool, List[str]]:
    state = load_sync_state(paths["state_json"])
    hashes = state.get("hashes", {}) if isinstance(state.get("hashes", {}), dict) else {}

    current = {
        "script": file_hash(script_path),
        "prompt": file_hash(paths["prompt"]),
        "reasoning": file_hash(paths["reasoning"]),
        "internal": file_hash(paths["internal"]),
    }

    issues: List[str] = []
    for k, v in current.items():
        old = str(hashes.get(k, ""))
        if old and old != v:
            issues.append(f"Detected change in {k} since last sync.")

    artifact_issues = []
    for artifact in state.get("output_artifacts", []):
        p = Path(str(artifact))
        if not p.exists():
            artifact_issues.append(f"Output artifact missing: {artifact}")
    issues.extend(artifact_issues)

    return (len(issues) == 0, issues)


def rebuild_prompt_from_state(script_path: Path, paths: Dict[str, Path]) -> None:
    # Keep prompt as plain English specification and refresh from current operational context.
    base = default_prompt_template(script_path.name).rstrip() + "\n\n"
    state = load_sync_state(paths["state_json"])
    recent_reasoning = read_text(paths["reasoning"]).splitlines()[-10:]

    additions = [
        "Current Operational Context",
        f"- Last synchronization: {state.get('last_sync_utc', 'unknown')}",
        f"- Registered output artifacts: {len(state.get('output_artifacts', []))}",
        "",
        "Recent Decision History (Last 10 Entries)",
    ]
    if recent_reasoning:
        additions.extend([f"- {line}" for line in recent_reasoning if line.strip()])
    else:
        additions.append("- No reasoning entries yet.")

    write_text(paths["prompt"], base + "\n".join(additions) + "\n")


def choose_script(workspace: Path) -> Path:
    default_script = workspace / folder_script_name(workspace)
    print(f"Default script: {default_script.name}")
    raw = input("Press Enter to use default, or provide script filename: ").strip()
    script = default_script if not raw else workspace / raw
    return script


def menu() -> None:
    workspace = Path.cwd()
    print(f"Workspace: {workspace}")

    script_path = choose_script(workspace)
    try:
        paths = ensure_companions(script_path)
        refresh_sync_hashes(script_path, paths)
    except Exception as exc:
        print(f"Initialization error: {exc}")
        return

    while True:
        print("\nSelect action:")
        print("1) Append chat entry")
        print("2) Record code change")
        print("3) Rebuild prompt from current state")
        print("4) Record output artifact path")
        print("5) Run synchronization check")
        print("6) Exit")

        choice = input("Choice: ").strip()

        if choice == "1":
            role = input("Role (User/Assistant): ").strip() or "Assistant"
            msg = input("Message summary: ").strip()
            append_reasoning(paths, role, msg)
            append_internal(paths, "chat-entry", f"{role} message logged")
            refresh_sync_hashes(script_path, paths)
            print("Chat entry appended.")
        elif choice == "2":
            summary = input("Code change summary: ").strip()
            append_reasoning(paths, "Assistant", f"Code change: {summary}")
            append_internal(paths, "code-change", summary)
            rebuild_prompt_from_state(script_path, paths)
            refresh_sync_hashes(script_path, paths)
            print("Code change recorded and prompt refreshed.")
        elif choice == "3":
            rebuild_prompt_from_state(script_path, paths)
            append_internal(paths, "prompt-refresh", "Prompt file refreshed from current state")
            refresh_sync_hashes(script_path, paths)
            print("Prompt rebuilt.")
        elif choice == "4":
            artifact = input("Output artifact path: ").strip()
            record_output_artifact(paths, artifact)
            append_internal(paths, "output-artifact", f"Registered artifact: {artifact}")
            refresh_sync_hashes(script_path, paths)
            print("Artifact path recorded.")
        elif choice == "5":
            ok, issues = sync_check(script_path, paths)
            if ok:
                print("Synchronization check passed.")
            else:
                print("Synchronization check found issues:")
                for issue in issues:
                    print(f"- {issue}")
            append_internal(paths, "sync-check", "pass" if ok else "; ".join(issues))
        elif choice == "6":
            print("Done.")
            break
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    menu()
