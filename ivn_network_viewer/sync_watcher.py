# Bidirectional Script-Prompt Synchronization System
# Created: 2026-01-28

import os
import time
import json
import hashlib
from pathlib import Path
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import urllib.request
import re

# Configuration
WORKSPACE_DIR = Path(__file__).parent
SYNC_LOG_FILE = WORKSPACE_DIR / "sync_log.txt"
FILE_HASHES_FILE = WORKSPACE_DIR / ".file_hashes.txt"

# Local LLM configuration (no external API keys required).
# Set LOCAL_LLM_ENDPOINT to a reachable HTTP endpoint that accepts JSON:
#   {
#     "prompt": "...user prompt...",
#     "system": "...system guidance...",
#     "max_tokens": 4000,
#     "temperature": 0.3
#   }
# Response should include a top-level "text" (preferred) or "content" or "response" field.
LOCAL_LLM_ENDPOINT = os.getenv("LOCAL_LLM_ENDPOINT") or os.getenv("LLM_URL")
LLM_ENABLED = bool(LOCAL_LLM_ENDPOINT)

def log_sync_event(message):
    """Log synchronization events to file and console"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    print(log_entry.strip())
    with open(SYNC_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_entry)


def call_local_llm(user_prompt, system_prompt, max_tokens=4000, temperature=0.3):
    """Call a local LLM HTTP endpoint. Returns text or None on failure."""
    if not LLM_ENABLED:
        return None

    payload = {
        "prompt": user_prompt,
        "system": system_prompt,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }

    try:
        req = urllib.request.Request(
            LOCAL_LLM_ENDPOINT,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        # Try multiple common response shapes
        if isinstance(data, dict):
            for key in ("text", "content", "response", "output"):
                if key in data and isinstance(data[key], str):
                    return data[key]
        # Fallback: if endpoint streams or returns list, join strings
        if isinstance(data, list):
            return "\n".join([str(x) for x in data])
    except Exception as exc:  # noqa: BLE001
        log_sync_event(f"  ✗ Local LLM call failed: {exc}")
    return None


def generate_prompt_stub(script_path, script_content, existing_prompt):
    """Offline stub: produce a structured prompt template without LLM."""
    return f"""# AUTO-GENERATED PROMPT STUB (no LLM available)
SCRIPT: {script_path.name}

PROBLEM STATEMENT
================================================================================
[Describe the problem this script solves]

BROADER CONTEXT
================================================================================
[Explain why this matters]

CONCEPTS THAT MUST BE UNDERSTOOD
================================================================================
[List core concepts used in the script]

PREREQUISITE KNOWLEDGE
================================================================================
[List what someone must know first]

DOMAIN-SPECIFIC TERMS
================================================================================
[Define key terms and acronyms]

HIGH-LEVEL DESCRIPTION
================================================================================
[Summarize what the script does overall]

WHY THE SCRIPT EXISTS
================================================================================
[Rationale and motivation]

ROLE OF THE SCRIPT
================================================================================
[Explain its role and boundaries]

KEY CONCEPTS AND LEARNING DATA
================================================================================
[Provide detailed concept explanations]

INPUT DEFINITIONS
================================================================================
[Define inputs, formats, validations]

OUTPUT DEFINITIONS
================================================================================
[Define outputs, formats, expectations]

VALIDITY CHECKS
================================================================================
[Describe how to verify correctness]

SUCCESS CRITERIA
================================================================================
[State what a successful run looks like]

ERROR HANDLING LOGIC
================================================================================
[Document how failures are managed]

CORRECT LEARNING MODEL BEHAVIOR
================================================================================
[What "working correctly" means]

EXAMPLE SCENARIOS
================================================================================
[List concrete input/output scenarios]

TEST CASES
================================================================================
[List validation cases]

REFERENCE: SCRIPT CONTENT (for manual authoring)
================================================================================
```python
{script_content}
```

EXISTING PROMPT CONTENT (if any)
================================================================================
{existing_prompt if existing_prompt else '[none]'}
"""


def generate_script_stub(prompt_content, existing_script, script_path):
    """Offline stub: produce a placeholder script when no LLM is available."""
    header = (
        "# AUTO-GENERATED SCRIPT STUB (no LLM available)\n"
        "# Prompt file is treated as source of truth.\n"
        "# TODO: Implement the required behavior described in the prompt.\n\n"
    )
    body = existing_script if existing_script else (
        "def main():\n"
        "    print('TODO: implement script per prompt requirements')\n\n"
        "if __name__ == '__main__':\n"
        "    main()\n"
    )
    return header + body

def calculate_file_hash(filepath):
    """Calculate MD5 hash of file contents"""
    if not os.path.exists(filepath):
        return None
    hasher = hashlib.md5()
    with open(filepath, "rb") as f:
        hasher.update(f.read())
    return hasher.hexdigest()

def load_file_hashes():
    """Load stored file hashes from disk"""
    hashes = {}
    if os.path.exists(FILE_HASHES_FILE):
        with open(FILE_HASHES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if "|" in line:
                    filepath, file_hash = line.strip().split("|", 1)
                    hashes[filepath] = file_hash
    return hashes

def save_file_hashes(hashes):
    """Save file hashes to disk"""
    with open(FILE_HASHES_FILE, "w", encoding="utf-8") as f:
        for filepath, file_hash in hashes.items():
            f.write(f"{filepath}|{file_hash}\n")

def update_prompt_from_script(script_path):
    """Generate/update prompt file based on script changes"""
    script_path = Path(script_path)
    prompt_path = script_path.parent / f"{script_path.stem}_prompt.txt"
    
    log_sync_event(f"SYNC TRIGGERED: Script changed -> Update prompt")
    log_sync_event(f"  Script: {script_path.name}")
    log_sync_event(f"  Prompt: {prompt_path.name}")

    # Read the current script
    with open(script_path, "r", encoding="utf-8") as f:
        script_content = f.read()
    
    # Read existing prompt if it exists
    existing_prompt = ""
    if os.path.exists(prompt_path):
        with open(prompt_path, "r", encoding="utf-8") as f:
            existing_prompt = f.read()
    
    # Construct prompt for the local LLM to update the prompt file
    system_prompt = """You are an expert at creating comprehensive training prompts for naive learning models. Your task is to analyze Python scripts and generate or update detailed prompt files that would allow a naive learning model to fully reconstruct and understand the script.

The prompt file MUST contain ALL of the following sections in plain English (not code):
1. PROBLEM STATEMENT - What problem does the script solve?
2. BROADER CONTEXT - Why does this problem matter?
3. CONCEPTS THAT MUST BE UNDERSTOOD - Core concepts used in the script
4. PREREQUISITE KNOWLEDGE - What must be known beforehand
5. DOMAIN-SPECIFIC TERMS - Vocabulary definitions
6. HIGH-LEVEL DESCRIPTION - What the script does
7. WHY THE SCRIPT EXISTS - Rationale and motivation
8. ROLE OF THE SCRIPT - Its function in the ecosystem
9. KEY CONCEPTS AND LEARNING DATA - Detailed concept explanations
10. INPUT DEFINITIONS - All inputs with formats and specifications
11. OUTPUT DEFINITIONS - All outputs with structure and format
12. VALIDITY CHECKS - How to verify output correctness
13. SUCCESS CRITERIA - What defines successful execution
14. ERROR HANDLING LOGIC - How failures are managed
15. CORRECT LEARNING MODEL BEHAVIOR - What "working correctly" means
16. EXAMPLE SCENARIOS - Concrete use cases with expected results
17. TEST CASES - Validation scenarios

Make the prompt comprehensive enough that a naive learning model given ONLY this prompt file could recreate the script faithfully."""

    user_prompt = f"""Analyze this Python script and generate a complete, comprehensive prompt file for training a naive learning model.

SCRIPT PATH: {script_path.name}

SCRIPT CONTENT:
```python
{script_content}
```

{"EXISTING PROMPT (update this if provided, otherwise create new):" if existing_prompt else "CREATE NEW PROMPT FILE with all required sections."}
{existing_prompt if existing_prompt else ""}

Generate the complete prompt file content. Be thorough and specific."""

    new_prompt_content = call_local_llm(
        user_prompt=user_prompt,
        system_prompt=system_prompt,
        max_tokens=16000,
        temperature=0.3,
    )

    if new_prompt_content is None:
        log_sync_event("  ⚠ Local LLM unavailable; generating offline prompt stub")
        new_prompt_content = generate_prompt_stub(script_path, script_content, existing_prompt)

    # Write updated prompt file
    with open(prompt_path, "w", encoding="utf-8") as f:
        f.write(new_prompt_content)

    # Update hash to prevent re-triggering
    hashes = load_file_hashes()
    hashes[str(prompt_path)] = calculate_file_hash(prompt_path)
    save_file_hashes(hashes)

    log_sync_event(f"  ✓ Prompt file updated successfully")
    log_sync_event(f"  Generated {len(new_prompt_content)} characters")

def update_script_from_prompt(prompt_path):
    """Generate/update script based on prompt file changes"""
    prompt_path = Path(prompt_path)
    
    # Determine script path from prompt path
    # Remove _prompt suffix and change extension to .py
    script_name = prompt_path.stem.replace("_prompt", "") + ".py"
    script_path = prompt_path.parent / script_name
    
    log_sync_event(f"SYNC TRIGGERED: Prompt changed -> Update script")
    log_sync_event(f"  Prompt: {prompt_path.name}")
    log_sync_event(f"  Script: {script_path.name}")

    # Read the current prompt
    with open(prompt_path, "r", encoding="utf-8") as f:
        prompt_content = f.read()
    
    # Read existing script if it exists
    existing_script = ""
    if os.path.exists(script_path):
        with open(script_path, "r", encoding="utf-8") as f:
            existing_script = f.read()
    
    # Construct prompt for the local LLM to update the script
    system_prompt = """You are an expert Python developer. Your task is to generate or update Python scripts based on comprehensive specification prompt files.

The prompt file contains complete specifications for what the script should do, including:
- Problem statement and context
- Required concepts and prerequisites  
- Input/output specifications
- Success criteria and error handling
- Example scenarios and test cases

Generate clean, well-structured, idiomatic Python code that faithfully implements ALL requirements from the prompt file. Include:
- Appropriate comments for clarity
- Proper error handling as specified
- All imports needed
- Correct implementation of all specified functionality

Output ONLY the complete Python script code, no explanations."""

    user_prompt = f"""Based on this comprehensive prompt file, generate the complete Python script.

PROMPT FILE CONTENT:
{prompt_content}

{"EXISTING SCRIPT (update/refactor as needed based on prompt changes):" if existing_script else "CREATE NEW SCRIPT implementing all prompt requirements."}
{f"```python\n{existing_script}\n```" if existing_script else ""}

Generate the complete, executable Python script that implements all requirements."""

    new_script_content = call_local_llm(
        user_prompt=user_prompt,
        system_prompt=system_prompt,
        max_tokens=8000,
        temperature=0.3,
    )

    if new_script_content is None:
        log_sync_event("  ⚠ Local LLM unavailable; generating offline script stub")
        new_script_content = generate_script_stub(prompt_content, existing_script, script_path)

    # Extract code from markdown if wrapped
    code_match = re.search(r"```python\n(.*?)\n```", new_script_content, re.DOTALL)
    if code_match:
        new_script_content = code_match.group(1)

    # Write updated script file
    with open(script_path, "w", encoding="utf-8") as f:
        f.write(new_script_content)

    # Update hash to prevent re-triggering
    hashes = load_file_hashes()
    hashes[str(script_path)] = calculate_file_hash(script_path)
    save_file_hashes(hashes)

    log_sync_event(f"  ✓ Script file updated successfully")
    log_sync_event(f"  Generated {len(new_script_content)} characters")

class SyncEventHandler(FileSystemEventHandler):
    """Handle file system events for .py and _prompt.txt files"""
    
    def __init__(self):
        super().__init__()
        self.file_hashes = load_file_hashes()
        self.cooldown_until = {}  # Prevent rapid re-triggers
    
    def on_modified(self, event):
        if event.is_directory:
            return
        
        filepath = Path(event.src_path)
        
        # Only process .py scripts and _prompt.txt files
        if not (filepath.suffix == ".py" or filepath.name.endswith("_prompt.txt")):
            return
        
        # Ignore the sync script itself and hidden files
        if filepath.name == Path(__file__).name or filepath.name.startswith("."):
            return
        
        # Check cooldown to prevent rapid re-triggers
        current_time = time.time()
        if filepath in self.cooldown_until:
            if current_time < self.cooldown_until[filepath]:
                return  # Still in cooldown
        
        # Calculate current hash
        current_hash = calculate_file_hash(filepath)
        if current_hash is None:
            return
        
        # Check if file actually changed
        stored_hash = self.file_hashes.get(str(filepath))
        if current_hash == stored_hash:
            return  # No actual change
        
        # Update stored hash
        self.file_hashes[str(filepath)] = current_hash
        save_file_hashes(self.file_hashes)
        
        # Set cooldown (5 seconds)
        self.cooldown_until[filepath] = current_time + 5
        
        # Determine sync direction and execute
        if filepath.suffix == ".py":
            # Script changed -> Update prompt
            update_prompt_from_script(filepath)
        elif filepath.name.endswith("_prompt.txt"):
            # Prompt changed -> Update script
            update_script_from_prompt(filepath)

def initialize_hashes():
    """Initialize hash tracking for all existing files"""
    log_sync_event("Initializing file tracking...")
    hashes = {}
    
    for py_file in WORKSPACE_DIR.glob("*.py"):
        if py_file.name != Path(__file__).name:
            hashes[str(py_file)] = calculate_file_hash(py_file)
            log_sync_event(f"  Tracking: {py_file.name}")
    
    for prompt_file in WORKSPACE_DIR.glob("*_prompt.txt"):
        hashes[str(prompt_file)] = calculate_file_hash(prompt_file)
        log_sync_event(f"  Tracking: {prompt_file.name}")
    
    save_file_hashes(hashes)
    log_sync_event(f"Now tracking {len(hashes)} files")

def main():
    """Main watcher loop"""
    log_sync_event("=" * 80)
    log_sync_event("BIDIRECTIONAL SCRIPT-PROMPT SYNCHRONIZATION SYSTEM STARTING")
    log_sync_event("=" * 80)
    log_sync_event(f"Workspace: {WORKSPACE_DIR}")
    log_sync_event(f"Log file: {SYNC_LOG_FILE}")
    
    # Initialize file tracking
    initialize_hashes()
    
    # Set up file system observer
    event_handler = SyncEventHandler()
    observer = Observer()
    observer.schedule(event_handler, str(WORKSPACE_DIR), recursive=False)
    observer.start()
    
    log_sync_event("File watcher active. Monitoring for changes...")
    log_sync_event("Press Ctrl+C to stop.")
    log_sync_event("")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        log_sync_event("")
        log_sync_event("=" * 80)
        log_sync_event("SYNCHRONIZATION SYSTEM STOPPED")
        log_sync_event("=" * 80)
    
    observer.join()

if __name__ == "__main__":
    main()
