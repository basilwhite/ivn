# Setup Script for Bidirectional Script-Prompt Synchronization System
# Run this to validate your environment and get started

import os
import sys
import subprocess

print("=" * 80)
print("BIDIRECTIONAL SCRIPT-PROMPT SYNC - SETUP VALIDATION")
print("=" * 80)
print()

# Check Python version
print("Checking Python version...")
version = sys.version_info
if version.major >= 3 and version.minor >= 7:
    print(f"  ✓ Python {version.major}.{version.minor}.{version.micro} (OK)")
else:
    print(f"  ✗ Python {version.major}.{version.minor} (Need 3.7+)")
    sys.exit(1)

print()

# Check required packages
print("Checking required packages...")
required_packages = ["anthropic", "watchdog"]
missing_packages = []

for package in required_packages:
    try:
        __import__(package)
        print(f"  ✓ {package} installed")
    except ImportError:
        print(f"  ✗ {package} NOT installed")
        missing_packages.append(package)

if missing_packages:
    print()
    print("Missing packages detected. Install with:")
    print(f"  pip install {' '.join(missing_packages)}")
    print()
    response = input("Install now? (y/n): ").strip().lower()
    if response == 'y':
        print()
        print("Installing packages...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing_packages)
            print("  ✓ Installation complete")
        except subprocess.CalledProcessError:
            print("  ✗ Installation failed")
            sys.exit(1)
    else:
        print("Setup incomplete. Please install packages manually.")
        sys.exit(1)

print()

# Check API key
print("Checking Anthropic API key...")
api_key = os.getenv("ANTHROPIC_API_KEY")
if api_key:
    print(f"  ✓ ANTHROPIC_API_KEY is set ({api_key[:12]}...)")
else:
    print("  ✗ ANTHROPIC_API_KEY not found in environment")
    print()
    print("You need an API key from: https://console.anthropic.com/")
    print()
    print("Set it with:")
    print("  PowerShell: $env:ANTHROPIC_API_KEY = 'your-key-here'")
    print("  CMD:        set ANTHROPIC_API_KEY=your-key-here")
    print("  Bash:       export ANTHROPIC_API_KEY='your-key-here'")
    print()
    response = input("Enter API key now (or press Enter to skip): ").strip()
    if response:
        os.environ["ANTHROPIC_API_KEY"] = response
        print("  ✓ API key set for this session")
        print()
        print("  NOTE: To persist, add to your shell profile or system environment variables")
    else:
        print()
        print("Setup incomplete. Set API key before running sync_watcher.py")
        sys.exit(1)

print()

# Test API connection
print("Testing API connection...")
try:
    import anthropic
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    
    # Simple test call
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=50,
        messages=[{"role": "user", "content": "Say 'Connection successful' and nothing else."}]
    )
    
    if "successful" in response.content[0].text.lower():
        print("  ✓ API connection verified")
    else:
        print("  ✓ API responded (authentication OK)")
except Exception as e:
    print(f"  ✗ API test failed: {str(e)}")
    print()
    print("Please verify:")
    print("  - API key is correct")
    print("  - Internet connection is active")
    print("  - No firewall blocking api.anthropic.com")
    sys.exit(1)

print()

# Check current directory
print("Checking workspace...")
from pathlib import Path
workspace = Path(__file__).parent

py_files = list(workspace.glob("*.py"))
py_files = [f for f in py_files if not f.name.startswith(".")]
prompt_files = list(workspace.glob("*_prompt.txt"))

print(f"  Workspace: {workspace}")
print(f"  Python files found: {len(py_files)}")
for f in py_files:
    print(f"    - {f.name}")

print(f"  Prompt files found: {len(prompt_files)}")
for f in prompt_files:
    print(f"    - {f.name}")

print()

# Summary
print("=" * 80)
print("SETUP COMPLETE!")
print("=" * 80)
print()
print("You're ready to use the bidirectional sync system.")
print()
print("Quick Start:")
print("  1. Run the watcher:")
print("       python sync_watcher.py")
print()
print("  2. Edit any .py file or _prompt.txt file")
print()
print("  3. Watch automatic synchronization happen!")
print()
print("Documentation:")
print("  - Full guide: SYNC_WORKFLOW_README.md")
print("  - Quick start: QUICKSTART.md")
print()
print("Happy coding with living documentation! 🚀")
print()
