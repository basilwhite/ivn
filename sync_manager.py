"""
Bidirectional Synchronization Manager for Python Scripts and Prompt Files

This script monitors and synchronizes changes between Python scripts (.py) 
and their corresponding prompt files (_prompt.txt) to maintain perfect consistency.
"""

import os
import time
import hashlib
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Set, Tuple, Optional
import ast
import re
import shutil
import sys


class SyncManager:
    """Manages bidirectional synchronization between .py and _prompt.txt files."""
    
    def __init__(self, watch_directory: str, state_file: str = ".sync_state.json"):
        """
        Initialize the sync manager.
        
        Args:
            watch_directory: Directory to monitor for changes
            state_file: JSON file to track file states and hashes
        """
        self.watch_dir = Path(watch_directory)
        self.state_file = self.watch_dir / state_file
        self.file_hashes: Dict[str, str] = {}
        self.load_state()
        
    def load_state(self):
        """Load previous file state from JSON."""
        if self.state_file.exists():
            with open(self.state_file, 'r') as f:
                self.file_hashes = json.load(f)
        else:
            self.file_hashes = {}
    
    def save_state(self):
        """Save current file state to JSON."""
        with open(self.state_file, 'w') as f:
            json.dump(self.file_hashes, indent=2, fp=f)
    
    def compute_hash(self, filepath: Path) -> str:
        """Compute SHA256 hash of file contents."""
        if not filepath.exists():
            return ""
        with open(filepath, 'rb') as f:
            return hashlib.sha256(f.read()).hexdigest()
    
    def get_script_pairs(self) -> Set[Tuple[Path, Path]]:
        """
        Find all Python scripts and their corresponding prompt files.
        
        Returns:
            Set of tuples (script_path, prompt_path)
        """
        pairs = set()
        
        # Find all .py files (excluding sync scripts)
        py_files = [f for f in self.watch_dir.glob("*.py") 
                    if not f.name.startswith("sync_") and not f.name.startswith("_")]
        
        for py_file in py_files:
            # Expected prompt file name: script_name_prompt.txt
            prompt_name = py_file.stem + "_prompt.txt"
            prompt_file = self.watch_dir / prompt_name
            pairs.add((py_file, prompt_file))
        
        # Also check for orphaned prompt files
        prompt_files = self.watch_dir.glob("*_prompt.txt")
        for prompt_file in prompt_files:
            # Extract script name by removing _prompt suffix
            script_name = prompt_file.name.replace("_prompt.txt", ".py")
            script_file = self.watch_dir / script_name
            pairs.add((script_file, prompt_file))
        
        return pairs
    
    def detect_changes(self) -> Dict[str, list]:
        """
        Detect which files have changed since last check.
        
        Returns:
            Dictionary with 'scripts' and 'prompts' lists of changed files
        """
        changes = {'scripts': [], 'prompts': []}
        
        for script_path, prompt_path in self.get_script_pairs():
            script_str = str(script_path)
            prompt_str = str(prompt_path)
            
            # Check script changes
            if script_path.exists():
                current_hash = self.compute_hash(script_path)
                if self.file_hashes.get(script_str) != current_hash:
                    changes['scripts'].append((script_path, prompt_path))
                    self.file_hashes[script_str] = current_hash
            
            # Check prompt changes
            if prompt_path.exists():
                current_hash = self.compute_hash(prompt_path)
                if self.file_hashes.get(prompt_str) != current_hash:
                    changes['prompts'].append((prompt_path, script_path))
                    self.file_hashes[prompt_str] = current_hash
        
        return changes
    
    def analyze_python_script(self, script_path: Path) -> Dict:
        """
        Analyze a Python script to extract structural information.
        
        Args:
            script_path: Path to the Python script
            
        Returns:
            Dictionary with script metadata and structure
        """
        if not script_path.exists():
            return {}
        
        with open(script_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        try:
            tree = ast.parse(content)
        except SyntaxError:
            return {'error': 'Syntax error in script'}
        
        analysis = {
            'imports': [],
            'classes': [],
            'functions': [],
            'global_vars': [],
            'docstring': ast.get_docstring(tree) or "",
            'lines_of_code': len(content.splitlines())
        }
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for name in node.names:
                    analysis['imports'].append(name.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for name in node.names:
                    analysis['imports'].append(f"{module}.{name.name}")
            elif isinstance(node, ast.ClassDef):
                analysis['classes'].append({
                    'name': node.name,
                    'docstring': ast.get_docstring(node) or "",
                    'methods': [m.name for m in node.body if isinstance(m, ast.FunctionDef)]
                })
            elif isinstance(node, ast.FunctionDef):
                if not any(node in cls.body for cls in ast.walk(tree) if isinstance(cls, ast.ClassDef)):
                    analysis['functions'].append({
                        'name': node.name,
                        'docstring': ast.get_docstring(node) or "",
                        'args': [arg.arg for arg in node.args.args]
                    })
        
        return analysis
    
    def generate_prompt_from_script(self, script_path: Path, prompt_path: Path):
        """
        Generate a comprehensive prompt file from a Python script.
        
        Args:
            script_path: Path to the Python script
            prompt_path: Path where prompt file should be created
        """
        analysis = self.analyze_python_script(script_path)
        
        if 'error' in analysis:
            print(f"⚠️  Cannot analyze {script_path.name}: {analysis['error']}")
            return
        
        # Read the actual script content
        with open(script_path, 'r', encoding='utf-8') as f:
            script_content = f.read()
        
        # Generate comprehensive prompt
        prompt_content = self._create_prompt_template(script_path.stem, analysis, script_content)
        
        with open(prompt_path, 'w', encoding='utf-8') as f:
            f.write(prompt_content)
        
        print(f"✓ Generated prompt: {prompt_path.name}")
    
    def _create_prompt_template(self, script_name: str, analysis: Dict, script_content: str) -> str:
        """Create a comprehensive prompt template with all required sections."""
        
        template = f"""# Prompt File for {script_name}.py
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## PROBLEM STATEMENT
[Describe the specific problem this script solves]

{analysis.get('docstring', '[Add description of the problem domain]')}


## BROADER CONTEXT
[Explain the larger system or workflow this script is part of]

This script is part of the IVN bidirectional synchronization system for maintaining consistency between Python scripts and their documentation prompts.


## CONCEPTS TO UNDERSTAND

### Core Concepts
[List the fundamental concepts a learner must grasp to understand this script]

1. File I/O operations in Python
2. Data structures used in the script
3. Algorithm or approach employed
4. Design patterns utilized


### Prerequisite Knowledge
[What should a learner know before approaching this script?]

- Basic Python syntax and data types
- Understanding of file system operations
- Familiarity with the problem domain


### Domain-Specific Terms
[Define specialized vocabulary used in this script]

{self._extract_domain_terms(analysis)}


## SCRIPT OVERVIEW

### High-Level Description
[What does this script do at a conceptual level?]

Lines of code: {analysis.get('lines_of_code', 'N/A')}


### Purpose and Role
[Why does this script exist? What is its role in the larger system?]

This script provides functionality for...


## TECHNICAL DETAILS

### Imports and Dependencies
{self._format_imports(analysis.get('imports', []))}


### Classes
{self._format_classes(analysis.get('classes', []))}


### Functions
{self._format_functions(analysis.get('functions', []))}


## INPUT/OUTPUT SPECIFICATIONS

### Input Requirements
[Define all inputs this script accepts]

- Command-line arguments: [Specify arguments, their types, and purposes]
- Configuration files: [List any config files read]
- Data files: [Specify expected file formats and structures]
- Environment variables: [List any environment dependencies]


### Output Specifications
[Define what this script produces]

- Output format: [File format, data structure, or stdout format]
- Output location: [Where outputs are written]
- Output structure: [Detailed structure of the output data]


### Output Validation Criteria
[How to verify the output is correct]

1. Format validation: [Specific format requirements]
2. Content validation: [Expected content characteristics]
3. Completeness checks: [What makes output complete]


## SUCCESS CRITERIA

### Successful Execution Defined
[What constitutes a successful run?]

A successful execution means:
1. All input files are processed without errors
2. Output files are created with valid content
3. No exceptions are raised during execution
4. Exit code is 0


### Successful Outcome Defined
[What is the desired end state after running this script?]

The desired outcome is...


### Error Handling Logic
[How does the script handle failures?]

Error handling strategy:
- Input validation: [How invalid inputs are handled]
- Exception handling: [What exceptions are caught and how]
- Logging: [What is logged during errors]
- Graceful degradation: [How the script fails safely]


## CORRECT BEHAVIOR DEFINITION

### What "Working Correctly" Means
[Explicit definition of correct behavior]

The script works correctly when:
1. [Specific behavioral criterion 1]
2. [Specific behavioral criterion 2]
3. [Specific behavioral criterion 3]


### Learning Model Behavior Guidelines
[How should a learning model behave when implementing this?]

A naive learning model should:
1. Follow the exact logic flow described
2. Implement all error handling as specified
3. Maintain the same input/output contracts
4. Preserve all validation criteria


## EXAMPLE SCENARIOS

### Example 1: [Typical Use Case]
**Scenario**: [Describe the scenario]

**Input**:
```
[Provide example input]
```

**Expected Output**:
```
[Provide expected output]
```

**Process**:
1. [Step-by-step description]


### Example 2: [Edge Case]
**Scenario**: [Describe edge case]

**Input**:
```
[Provide example input]
```

**Expected Behavior**:
[What should happen]


### Example 3: [Error Case]
**Scenario**: [Describe error condition]

**Input**:
```
[Provide example input]
```

**Expected Error Handling**:
[How the error should be handled]


## TEST CASES

### Unit Tests
[Specific test cases that validate functionality]

1. Test Name: [test_function_name]
   - Input: [test input]
   - Expected: [expected result]
   - Validates: [what this test proves]


### Integration Tests
[Tests that validate interaction with other components]


### Validation Tests
[Tests that verify output correctness]


## IMPLEMENTATION GUIDE

### Step-by-Step Implementation
[How to implement this script from scratch]

1. Set up imports and dependencies
2. Define data structures
3. Implement core logic
4. Add error handling
5. Implement I/O operations
6. Add validation
7. Test thoroughly


### Code Structure
[Recommended code organization]

```
1. Imports
2. Constants and configuration
3. Helper functions
4. Main classes
5. Main logic
6. Entry point
```


## ACTUAL SCRIPT CONTENT

```python
{script_content}
```


## REVISION HISTORY
- {datetime.now().strftime('%Y-%m-%d')}: Initial prompt generation


## NOTES FOR LEARNING MODEL
[Additional guidance for AI/ML models learning from this]

- Pay attention to: [Key aspects to focus on]
- Common pitfalls: [What to avoid]
- Best practices: [Recommended approaches]
"""
        return template
    
    def _extract_domain_terms(self, analysis: Dict) -> str:
        """Extract and format domain-specific terminology."""
        terms = []
        
        for cls in analysis.get('classes', []):
            terms.append(f"- **{cls['name']}**: [Define this class's purpose]")
        
        for func in analysis.get('functions', []):
            terms.append(f"- **{func['name']}**: [Define this function's purpose]")
        
        return '\n'.join(terms) if terms else "[No domain-specific terms identified]"
    
    def _format_imports(self, imports: list) -> str:
        """Format imports list."""
        if not imports:
            return "No external imports"
        return '\n'.join(f"- {imp}" for imp in sorted(set(imports)))
    
    def _format_classes(self, classes: list) -> str:
        """Format classes list with methods."""
        if not classes:
            return "No classes defined"
        
        result = []
        for cls in classes:
            result.append(f"\n**{cls['name']}**")
            if cls['docstring']:
                result.append(f"  Description: {cls['docstring']}")
            if cls['methods']:
                result.append(f"  Methods: {', '.join(cls['methods'])}")
        
        return '\n'.join(result)
    
    def _format_functions(self, functions: list) -> str:
        """Format functions list with arguments."""
        if not functions:
            return "No standalone functions defined"
        
        result = []
        for func in functions:
            args = ', '.join(func['args']) if func['args'] else 'no arguments'
            result.append(f"\n**{func['name']}({args})**")
            if func['docstring']:
                result.append(f"  {func['docstring']}")
        
        return '\n'.join(result)
    
    def update_script_from_prompt(self, prompt_path: Path, script_path: Path):
        """
        Update script based on changes in prompt file.
        
        This is a placeholder for AI-assisted script regeneration.
        In practice, this would require an LLM to interpret the prompt
        and regenerate the script accordingly.
        
        Args:
            prompt_path: Path to the prompt file
            script_path: Path to the script to update
        """
        print(f"⚠️  Prompt file changed: {prompt_path.name}")
        print(f"   Review prompt changes and manually update {script_path.name}")
        print(f"   Or use an LLM to regenerate the script from the prompt.")
        
        # Log the change for manual review
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'action': 'prompt_modified',
            'prompt_file': str(prompt_path),
            'script_file': str(script_path),
            'message': 'Manual review required: prompt file modified'
        }
        self._log_sync_event(log_entry)
    
    def _log_sync_event(self, event: Dict):
        """Log synchronization events to a file."""
        log_file = self.watch_dir / "sync_log.json"
        
        logs = []
        if log_file.exists():
            with open(log_file, 'r') as f:
                logs = json.load(f)
        
        logs.append(event)
        
        # Keep only last 100 entries
        logs = logs[-100:]
        
        with open(log_file, 'w') as f:
            json.dump(logs, indent=2, fp=f)
    
    def synchronize_all(self, force: bool = False):
        """
        Perform full synchronization of all script-prompt pairs.
        
        Args:
            force: If True, regenerate all prompts regardless of changes
        """
        print("🔄 Starting synchronization...")
        
        pairs = self.get_script_pairs()
        
        for script_path, prompt_path in pairs:
            # If script exists but prompt doesn't, generate it
            if script_path.exists() and not prompt_path.exists():
                print(f"📝 Creating missing prompt for {script_path.name}")
                self.generate_prompt_from_script(script_path, prompt_path)
            
            # If forcing regeneration
            elif force and script_path.exists():
                print(f"♻️  Regenerating prompt for {script_path.name}")
                self.generate_prompt_from_script(script_path, prompt_path)
        
        self.save_state()
        print("✓ Synchronization complete")
    
    def watch(self, interval: int = 5):
        """
        Continuously watch for changes and synchronize.
        
        Args:
            interval: Check interval in seconds
        """
        print(f"👀 Watching directory: {self.watch_dir}")
        print(f"   Check interval: {interval} seconds")
        print("   Press Ctrl+C to stop\n")
        
        try:
            while True:
                changes = self.detect_changes()
                
                # Handle script changes -> update prompts
                for script_path, prompt_path in changes['scripts']:
                    print(f"🔄 Script changed: {script_path.name}")
                    self.generate_prompt_from_script(script_path, prompt_path)
                    
                    log_entry = {
                        'timestamp': datetime.now().isoformat(),
                        'action': 'script_to_prompt',
                        'script_file': str(script_path),
                        'prompt_file': str(prompt_path)
                    }
                    self._log_sync_event(log_entry)
                
                # Handle prompt changes -> notify for script updates
                for prompt_path, script_path in changes['prompts']:
                    self.update_script_from_prompt(prompt_path, script_path)
                
                if changes['scripts'] or changes['prompts']:
                    self.save_state()
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n\n⏹️  Stopping file watcher")
            self.save_state()
    
    def create_script_from_prompt(self, prompt_path: Path) -> Optional[Path]:
        """
        Create a Python script from a prompt file.
        
        Args:
            prompt_path: Path to the prompt file
            
        Returns:
            Path to created script, or None if failed
        """
        if not prompt_path.exists():
            print(f"❌ Prompt file not found: {prompt_path}")
            return None
        
        # Read prompt content
        try:
            with open(prompt_path, 'r', encoding='utf-8') as f:
                prompt_content = f.read()
        except Exception as e:
            print(f"❌ Error reading prompt: {e}")
            return None
        
        # Extract script content from prompt (from ACTUAL SCRIPT CONTENT section)
        script_code = self._extract_script_from_prompt(prompt_content)
        
        if not script_code:
            print(f"⚠️  No script content found in prompt file")
            print(f"   Creating basic template from specifications...")
            script_code = self._generate_script_template_from_prompt(prompt_content, prompt_path.stem.replace('_prompt', ''))
        
        # Determine script name
        script_name = prompt_path.stem.replace('_prompt', '') + '.py'
        script_path = self.watch_dir / script_name
        
        if script_path.exists():
            response = input(f"⚠️  Script {script_name} already exists. Overwrite? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                print("❌ Cancelled")
                return None
        
        # Write script file
        try:
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write(script_code)
            print(f"✅ Created script: {script_path}")
            
            # Update hash
            self.file_hashes[str(script_path)] = self.compute_hash(script_path)
            self.save_state()
            
            return script_path
        except Exception as e:
            print(f"❌ Error creating script: {e}")
            return None
    
    def _extract_script_from_prompt(self, prompt_content: str) -> Optional[str]:
        """Extract Python code from ACTUAL SCRIPT CONTENT section."""
        # Look for code block in ACTUAL SCRIPT CONTENT section
        pattern = r'##\s+ACTUAL SCRIPT CONTENT.*?```python\n(.*?)```'
        match = re.search(pattern, prompt_content, re.DOTALL | re.IGNORECASE)
        
        if match:
            return match.group(1).strip()
        
        # Alternative: look for any Python code block
        pattern = r'```python\n(.*?)```'
        matches = re.findall(pattern, prompt_content, re.DOTALL)
        
        if matches:
            # Return the largest code block (likely the actual script)
            return max(matches, key=len).strip()
        
        return None
    
    def _generate_script_template_from_prompt(self, prompt_content: str, script_name: str) -> str:
        """Generate a basic script template from prompt specifications."""
        # Extract problem statement
        problem_match = re.search(r'##\s+PROBLEM STATEMENT\s*\n+(.*?)(?=\n##|\Z)', prompt_content, re.DOTALL | re.IGNORECASE)
        problem_statement = problem_match.group(1).strip() if problem_match else "TODO: Add problem statement"
        
        template = f'''"""
{script_name}

{problem_statement}

TODO: This is a template generated from the prompt file.
      Implement the functionality described in the prompt.
"""

def main():
    """Main entry point."""
    print("TODO: Implement functionality")
    pass


if __name__ == '__main__':
    main()
'''
        return template
    
    def add_script_from_file(self, source_path: str) -> bool:
        """
        Copy a script file to the watch directory and generate its prompt.
        
        Args:
            source_path: Path to the source script file
            
        Returns:
            True if successful
        """
        source = Path(source_path)
        
        if not source.exists():
            print(f"❌ File not found: {source_path}")
            return False
        
        if not source.suffix == '.py':
            print(f"❌ Not a Python file: {source_path}")
            return False
        
        # Copy to watch directory
        dest_path = self.watch_dir / source.name
        
        if dest_path.exists():
            response = input(f"⚠️  {source.name} already exists in directory. Overwrite? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                print("❌ Cancelled")
                return False
        
        try:
            shutil.copy2(source, dest_path)
            print(f"✅ Copied script: {dest_path}")
            
            # Generate prompt
            prompt_path = self.watch_dir / (dest_path.stem + "_prompt.txt")
            self.generate_prompt_from_script(dest_path, prompt_path)
            
            # Update hashes
            self.file_hashes[str(dest_path)] = self.compute_hash(dest_path)
            self.file_hashes[str(prompt_path)] = self.compute_hash(prompt_path)
            self.save_state()
            
            return True
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def add_prompt_from_file(self, source_path: str) -> bool:
        """
        Copy a prompt file to the watch directory and generate its script.
        
        Args:
            source_path: Path to the source prompt file
            
        Returns:
            True if successful
        """
        source = Path(source_path)
        
        if not source.exists():
            print(f"❌ File not found: {source_path}")
            return False
        
        # Copy to watch directory
        dest_path = self.watch_dir / source.name
        
        if dest_path.exists():
            response = input(f"⚠️  {source.name} already exists in directory. Overwrite? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                print("❌ Cancelled")
                return False
        
        try:
            shutil.copy2(source, dest_path)
            print(f"✅ Copied prompt: {dest_path}")
            
            # Generate script from prompt
            script_path = self.create_script_from_prompt(dest_path)
            
            if script_path:
                # Update hash for prompt
                self.file_hashes[str(dest_path)] = self.compute_hash(dest_path)
                self.save_state()
                return True
            else:
                return False
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def create_prompt_from_text(self, prompt_text: str, base_name: str) -> bool:
        """
        Create a prompt file from pasted text and generate the script.
        
        Args:
            prompt_text: The prompt content
            base_name: Base name for the files (without extension)
            
        Returns:
            True if successful
        """
        # Clean up base name
        base_name = base_name.replace('.py', '').replace('_prompt', '').replace('.txt', '')
        
        prompt_path = self.watch_dir / f"{base_name}_prompt.txt"
        
        if prompt_path.exists():
            response = input(f"⚠️  {prompt_path.name} already exists. Overwrite? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                print("❌ Cancelled")
                return False
        
        try:
            # Write prompt file
            with open(prompt_path, 'w', encoding='utf-8') as f:
                f.write(prompt_text)
            print(f"✅ Created prompt: {prompt_path}")
            
            # Generate script from prompt
            script_path = self.create_script_from_prompt(prompt_path)
            
            if script_path:
                # Update hash for prompt
                self.file_hashes[str(prompt_path)] = self.compute_hash(prompt_path)
                self.save_state()
                return True
            else:
                return False
        except Exception as e:
            print(f"❌ Error: {e}")
            return False


def show_menu():
    """Display the interactive menu."""
    print("\n" + "="*70)
    print("  BIDIRECTIONAL SCRIPT-PROMPT SYNCHRONIZATION MANAGER")
    print("="*70)
    print("\nChoose an option:")
    print("\n  [1] Add a Python script file (will generate prompt)")
    print("  [2] Add a prompt file (will generate script)")
    print("  [3] Paste prompt text (will generate script)")
    print("  [4] Start continuous sync (watch mode)")
    print("  [5] Sync all existing files once")
    print("  [6] Validate all script-prompt pairs")
    print("  [?] Explain these options")
    print("  [0] Exit")
    print("\n" + "="*70)


def show_help():
    """Display detailed explanation of menu options."""
    print("\n" + "="*70)
    print("  MENU OPTIONS EXPLAINED")
    print("="*70)
    
    print("\n[1] Add a Python script file")
    print("    • Use when: You have a .py file and want documentation")
    print("    • What happens: Copies your script here, generates comprehensive")
    print("                    prompt file with all required sections")
    print("    • Example: Add 'calculator.py' → get 'calculator_prompt.txt'")
    
    print("\n[2] Add a prompt file")
    print("    • Use when: You have a prompt/spec file and want the code")
    print("    • What happens: Copies your prompt here, extracts or generates")
    print("                    Python script from the prompt content")
    print("    • Example: Add 'parser_prompt.txt' → get 'parser.py'")
    
    print("\n[3] Paste prompt text")
    print("    • Use when: You have requirements/specs to paste directly")
    print("    • What happens: You paste specs, system creates prompt file")
    print("                    and generates script from it")
    print("    • Perfect for: Turning requirements into working code quickly")
    print("    • Tip: Press Ctrl+Z (Windows) or Ctrl+D (Unix) when done pasting")
    
    print("\n[4] Start continuous sync (watch mode)")
    print("    • Use when: You want automatic bidirectional synchronization")
    print("    • What happens: Monitors all files, auto-updates prompts when")
    print("                    scripts change, notifies when prompts change")
    print("    • Runs until: You press Ctrl+C")
    print("    • Tip: Run in separate terminal while you code")
    
    print("\n[5] Sync all existing files once")
    print("    • Use when: You have scripts but no prompts (or vice versa)")
    print("    • What happens: Scans directory, generates missing prompts")
    print("                    for any scripts that need them")
    print("    • Perfect for: Initial setup or batch processing")
    
    print("\n[6] Validate all script-prompt pairs")
    print("    • Use when: You want to check synchronization quality")
    print("    • What happens: Runs validator, shows detailed report with")
    print("                    errors, warnings, and suggestions")
    print("    • Best practice: Run before committing code")
    
    print("\n[?] Explain these options")
    print("    • Shows this help screen")
    
    print("\n[0] Exit")
    print("    • Exits the interactive menu")
    
    print("\n" + "="*70)
    print("\nTip: Start with option [1] or [3] to try the system!")
    print("     Read INTERACTIVE_MODE.md for detailed workflows.")
    print("="*70)


def get_multiline_input(prompt_msg: str) -> str:
    """Get multiline input from user."""
    print(prompt_msg)
    print("(Paste your content, then press Ctrl+Z and Enter on Windows, or Ctrl+D on Unix)")
    print("-" * 70)
    
    lines = []
    try:
        while True:
            line = input()
            lines.append(line)
    except EOFError:
        pass
    
    return '\n'.join(lines)


def interactive_mode(manager: 'SyncManager'):
    """Run interactive menu loop."""
    while True:
        show_menu()
        
        choice = input("\nEnter your choice: ").strip()
        
        if choice == '0':
            print("\n👋 Goodbye!")
            break
        
        elif choice == '1':
            # Add script file
            print("\n📄 Add Python Script File")
            print("-" * 70)
            script_path = input("Enter path to Python script: ").strip()
            
            if script_path:
                manager.add_script_from_file(script_path)
            else:
                print("❌ No path provided")
        
        elif choice == '2':
            # Add prompt file
            print("\n📝 Add Prompt File")
            print("-" * 70)
            prompt_path = input("Enter path to prompt file: ").strip()
            
            if prompt_path:
                manager.add_prompt_from_file(prompt_path)
            else:
                print("❌ No path provided")
        
        elif choice == '3':
            # Paste prompt text
            print("\n✍️  Paste Prompt Text")
            print("-" * 70)
            base_name = input("Enter base name for the script (e.g., 'my_script'): ").strip()
            
            if not base_name:
                print("❌ No name provided")
                continue
            
            print()
            prompt_text = get_multiline_input("Paste your prompt content below:")
            
            if prompt_text.strip():
                manager.create_prompt_from_text(prompt_text, base_name)
            else:
                print("❌ No content provided")
        
        elif choice == '4':
            # Watch mode
            print("\n👀 Starting Continuous Sync (Watch Mode)")
            print("-" * 70)
            print("Press Ctrl+C to stop\n")
            
            try:
                manager.watch(interval=5)
            except KeyboardInterrupt:
                print("\n⏹️  Stopped watching")
        
        elif choice == '5':
            # Sync all once
            print("\n🔄 Synchronizing All Files")
            print("-" * 70)
            manager.synchronize_all()
        
        elif choice == '6':
            # Validate
            print("\n✅ Validating Script-Prompt Pairs")
            print("-" * 70)
            
            # Import and run validator
            try:
                import subprocess
                result = subprocess.run(
                    [sys.executable, 'sync_validator.py'],
                    cwd=manager.watch_dir,
                    capture_output=False
                )
            except Exception as e:
                print(f"❌ Error running validator: {e}")
        
        elif choice == '?':
            # Show help
            show_help()
        
        else:
            print("❌ Invalid choice. Please try again.")
        
        input("\nPress Enter to continue...")


def main():
    """Main entry point for the sync manager."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Bidirectional synchronization manager for Python scripts and prompts"
    )
    parser.add_argument(
        '--directory', '-d',
        default='.',
        help='Directory to watch (default: current directory)'
    )
    parser.add_argument(
        '--watch', '-w',
        action='store_true',
        help='Continuously watch for changes'
    )
    parser.add_argument(
        '--interval', '-i',
        type=int,
        default=5,
        help='Watch interval in seconds (default: 5)'
    )
    parser.add_argument(
        '--sync-all', '-s',
        action='store_true',
        help='Synchronize all files once'
    )
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Force regeneration of all prompts'
    )
    parser.add_argument(
        '--interactive', '-I',
        action='store_true',
        help='Start interactive mode with menu'
    )
    
    args = parser.parse_args()
    
    manager = SyncManager(args.directory)
    
    # If no arguments provided, start interactive mode
    if len(sys.argv) == 1:
        interactive_mode(manager)
    elif args.interactive:
        interactive_mode(manager)
    elif args.sync_all:
        manager.synchronize_all(force=args.force)
    elif args.watch:
        manager.watch(interval=args.interval)
    else:
        # Default: show help and suggest interactive mode
        parser.print_help()
        print("\n💡 Tip: Run without arguments for interactive mode!")


if __name__ == '__main__':
    main()
