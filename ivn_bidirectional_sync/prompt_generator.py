"""
Prompt Template Generator for Python Scripts

This utility generates comprehensive prompt files for existing Python scripts,
following the standardized format required for naive learning model training.
"""

import ast
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional


class PromptGenerator:
    """Generates standardized prompt files from Python scripts."""
    
    def __init__(self):
        self.required_sections = [
            'PROBLEM STATEMENT',
            'BROADER CONTEXT',
            'CONCEPTS TO UNDERSTAND',
            'SCRIPT OVERVIEW',
            'TECHNICAL DETAILS',
            'INPUT/OUTPUT SPECIFICATIONS',
            'SUCCESS CRITERIA',
            'CORRECT BEHAVIOR DEFINITION',
            'EXAMPLE SCENARIOS',
            'TEST CASES',
            'IMPLEMENTATION GUIDE',
            'ACTUAL SCRIPT CONTENT'
        ]
    
    def analyze_script(self, script_path: Path) -> Dict:
        """
        Perform deep analysis of a Python script.
        
        Args:
            script_path: Path to the Python script
            
        Returns:
            Dictionary containing comprehensive script analysis
        """
        with open(script_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        try:
            tree = ast.parse(content)
        except SyntaxError as e:
            return {
                'error': f'Syntax error: {str(e)}',
                'content': content
            }
        
        analysis = {
            'content': content,
            'lines': len(content.splitlines()),
            'docstring': ast.get_docstring(tree) or "",
            'imports': self._extract_imports(tree),
            'classes': self._extract_classes(tree),
            'functions': self._extract_functions(tree),
            'constants': self._extract_constants(tree),
            'main_block': self._has_main_block(content),
            'dependencies': self._extract_dependencies(tree)
        }
        
        return analysis
    
    def _extract_imports(self, tree: ast.AST) -> List[Dict]:
        """Extract all import statements."""
        imports = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append({
                        'type': 'import',
                        'module': alias.name,
                        'alias': alias.asname
                    })
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for alias in node.names:
                    imports.append({
                        'type': 'from',
                        'module': module,
                        'name': alias.name,
                        'alias': alias.asname
                    })
        
        return imports
    
    def _extract_classes(self, tree: ast.AST) -> List[Dict]:
        """Extract all class definitions."""
        classes = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_info = {
                    'name': node.name,
                    'docstring': ast.get_docstring(node) or "",
                    'bases': [self._get_name(base) for base in node.bases],
                    'methods': [],
                    'attributes': []
                }
                
                for item in node.body:
                    if isinstance(item, ast.FunctionDef):
                        class_info['methods'].append({
                            'name': item.name,
                            'docstring': ast.get_docstring(item) or "",
                            'args': [arg.arg for arg in item.args.args],
                            'is_private': item.name.startswith('_'),
                            'is_static': any(isinstance(d, ast.Name) and d.id == 'staticmethod' 
                                           for d in item.decorator_list),
                            'is_classmethod': any(isinstance(d, ast.Name) and d.id == 'classmethod' 
                                                for d in item.decorator_list)
                        })
                
                classes.append(class_info)
        
        return classes
    
    def _extract_functions(self, tree: ast.AST) -> List[Dict]:
        """Extract all standalone function definitions."""
        functions = []
        
        # Get all class bodies to exclude class methods
        class_bodies = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_bodies.extend(node.body)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node not in class_bodies:
                func_info = {
                    'name': node.name,
                    'docstring': ast.get_docstring(node) or "",
                    'args': [arg.arg for arg in node.args.args],
                    'returns': self._get_return_annotation(node),
                    'is_async': isinstance(node, ast.AsyncFunctionDef),
                    'decorators': [self._get_name(d) for d in node.decorator_list]
                }
                functions.append(func_info)
        
        return functions
    
    def _extract_constants(self, tree: ast.AST) -> List[Dict]:
        """Extract module-level constants."""
        constants = []
        
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id.isupper():
                        constants.append({
                            'name': target.id,
                            'value': ast.unparse(node.value) if hasattr(ast, 'unparse') else str(node.value)
                        })
        
        return constants
    
    def _extract_dependencies(self, tree: ast.AST) -> Dict:
        """Identify external dependencies and their purposes."""
        stdlib = {
            'os', 'sys', 'time', 'datetime', 'json', 'csv', 're', 'math',
            'pathlib', 'argparse', 'logging', 'collections', 'itertools',
            'functools', 'typing', 'io', 'copy', 'ast', 'hashlib'
        }
        
        imports = self._extract_imports(tree)
        
        dependencies = {
            'stdlib': [],
            'third_party': []
        }
        
        for imp in imports:
            module = imp['module'].split('.')[0]
            if module in stdlib:
                dependencies['stdlib'].append(imp['module'])
            else:
                dependencies['third_party'].append(imp['module'])
        
        return dependencies
    
    def _has_main_block(self, content: str) -> bool:
        """Check if script has a main block."""
        return "if __name__ == '__main__':" in content or 'if __name__ == "__main__":' in content
    
    def _get_name(self, node: ast.AST) -> str:
        """Safely get name from an AST node."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return f"{self._get_name(node.value)}.{node.attr}"
        return str(node)
    
    def _get_return_annotation(self, node: ast.FunctionDef) -> Optional[str]:
        """Extract return type annotation if present."""
        if node.returns:
            return ast.unparse(node.returns) if hasattr(ast, 'unparse') else str(node.returns)
        return None
    
    def generate_prompt(self, script_path: Path, output_path: Optional[Path] = None) -> str:
        """
        Generate a comprehensive prompt file for a Python script.
        
        Args:
            script_path: Path to the Python script
            output_path: Optional path for output; if None, uses script_name_prompt.txt
            
        Returns:
            Path to the generated prompt file
        """
        if output_path is None:
            output_path = script_path.parent / f"{script_path.stem}_prompt.txt"
        
        analysis = self.analyze_script(script_path)
        
        if 'error' in analysis:
            print(f"⚠️  Warning: {analysis['error']}")
        
        prompt_content = self._build_prompt_content(script_path.stem, analysis)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(prompt_content)
        
        print(f"✓ Generated: {output_path}")
        return str(output_path)
    
    def _build_prompt_content(self, script_name: str, analysis: Dict) -> str:
        """Build the complete prompt content with all required sections."""
        
        sections = []
        
        # Header
        sections.append(f"# Prompt File for {script_name}.py")
        sections.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        sections.append("")
        
        # Problem Statement
        sections.append("## PROBLEM STATEMENT")
        sections.append("")
        docstring = analysis.get('docstring', '')
        if docstring:
            sections.append(docstring)
        else:
            sections.append("[Describe the specific problem this script solves]")
            sections.append("")
            sections.append("What pain point does this address?")
            sections.append("What would happen if this script didn't exist?")
        sections.append("")
        sections.append("")
        
        # Broader Context
        sections.append("## BROADER CONTEXT")
        sections.append("")
        sections.append("[Explain how this script fits into the larger system or workflow]")
        sections.append("")
        sections.append("- What other scripts or systems does this interact with?")
        sections.append("- What is the user's workflow when using this script?")
        sections.append("- What business process or technical workflow does this support?")
        sections.append("")
        sections.append("")
        
        # Concepts to Understand
        sections.append("## CONCEPTS TO UNDERSTAND")
        sections.append("")
        sections.append("### Core Concepts")
        sections.append("")
        sections.append("To understand this script, a learner must grasp these fundamental concepts:")
        sections.append("")
        
        # Infer concepts from analysis
        concepts = []
        if analysis.get('classes'):
            concepts.append("- Object-oriented programming (classes and methods)")
        if analysis.get('functions'):
            concepts.append("- Functional programming patterns")
        if any('async' in str(f) for f in analysis.get('functions', [])):
            concepts.append("- Asynchronous programming")
        if analysis.get('dependencies', {}).get('third_party'):
            concepts.append("- Third-party library integration")
        
        sections.extend(concepts if concepts else ["[List the core concepts]"])
        sections.append("")
        
        sections.append("### Prerequisite Knowledge")
        sections.append("")
        sections.append("Before studying this script, you should understand:")
        sections.append("")
        sections.append("- Python syntax fundamentals")
        sections.append("- [Additional prerequisites based on script complexity]")
        sections.append("")
        
        sections.append("### Domain-Specific Terms")
        sections.append("")
        sections.append(self._format_domain_terms(analysis))
        sections.append("")
        sections.append("")
        
        # Script Overview
        sections.append("## SCRIPT OVERVIEW")
        sections.append("")
        sections.append("### High-Level Description")
        sections.append("")
        sections.append(f"Lines of code: {analysis.get('lines', 'N/A')}")
        sections.append(f"Number of functions: {len(analysis.get('functions', []))}")
        sections.append(f"Number of classes: {len(analysis.get('classes', []))}")
        sections.append("")
        sections.append("[Describe what this script does at a high level]")
        sections.append("")
        
        sections.append("### Purpose and Role")
        sections.append("")
        sections.append("[Why does this script exist?]")
        sections.append("")
        sections.append("This script serves to...")
        sections.append("")
        sections.append("")
        
        # Technical Details
        sections.append("## TECHNICAL DETAILS")
        sections.append("")
        
        sections.append("### Dependencies")
        sections.append("")
        deps = analysis.get('dependencies', {})
        sections.append("**Standard Library:**")
        sections.append(self._format_list(deps.get('stdlib', [])))
        sections.append("")
        sections.append("**Third-Party Libraries:**")
        sections.append(self._format_list(deps.get('third_party', [])))
        sections.append("")
        
        sections.append("### Classes and Methods")
        sections.append("")
        sections.append(self._format_classes(analysis.get('classes', [])))
        sections.append("")
        
        sections.append("### Functions")
        sections.append("")
        sections.append(self._format_functions(analysis.get('functions', [])))
        sections.append("")
        
        if analysis.get('constants'):
            sections.append("### Constants")
            sections.append("")
            for const in analysis['constants']:
                sections.append(f"- **{const['name']}**: {const['value']}")
            sections.append("")
        sections.append("")
        
        # Input/Output Specifications
        sections.append("## INPUT/OUTPUT SPECIFICATIONS")
        sections.append("")
        sections.append("### Input Requirements")
        sections.append("")
        sections.append("This script accepts the following inputs:")
        sections.append("")
        
        if analysis.get('main_block'):
            sections.append("**Command-Line Interface:**")
            sections.append("- Arguments: [Specify each argument, its type, and purpose]")
            sections.append("- Flags: [List optional flags]")
            sections.append("- Default values: [Document defaults]")
            sections.append("")
        
        sections.append("**File Inputs:**")
        sections.append("- File format: [e.g., CSV, JSON, text]")
        sections.append("- Expected structure: [Describe schema or format]")
        sections.append("- Validation: [How inputs are validated]")
        sections.append("")
        
        sections.append("**Data Structure Inputs:**")
        sections.append("[If accepting data structures, specify their format]")
        sections.append("")
        
        sections.append("### Output Specifications")
        sections.append("")
        sections.append("**Output Format:**")
        sections.append("- Type: [File, stdout, data structure, etc.]")
        sections.append("- Format: [JSON, CSV, plain text, etc.]")
        sections.append("- Location: [Where outputs are written]")
        sections.append("")
        
        sections.append("**Output Structure:**")
        sections.append("```")
        sections.append("[Provide example or schema of output]")
        sections.append("```")
        sections.append("")
        
        sections.append("### Output Validation Criteria")
        sections.append("")
        sections.append("The output is valid if:")
        sections.append("")
        sections.append("1. Format matches specification exactly")
        sections.append("2. All required fields are present")
        sections.append("3. Data types are correct")
        sections.append("4. Values are within expected ranges")
        sections.append("5. [Additional validation criteria]")
        sections.append("")
        sections.append("")
        
        # Success Criteria
        sections.append("## SUCCESS CRITERIA")
        sections.append("")
        sections.append("### Successful Execution Defined")
        sections.append("")
        sections.append("A successful execution means:")
        sections.append("")
        sections.append("1. The script completes without raising unhandled exceptions")
        sections.append("2. Exit code is 0 (or expected non-zero code for specific scenarios)")
        sections.append("3. All expected outputs are generated")
        sections.append("4. Outputs pass validation criteria")
        sections.append("5. [Additional execution criteria]")
        sections.append("")
        
        sections.append("### Successful Outcome Defined")
        sections.append("")
        sections.append("The desired outcome after execution:")
        sections.append("")
        sections.append("- [Specific end state 1]")
        sections.append("- [Specific end state 2]")
        sections.append("- [Specific end state 3]")
        sections.append("")
        
        sections.append("### Error Handling Logic")
        sections.append("")
        sections.append("**Input Validation:**")
        sections.append("- Invalid inputs are detected by: [method]")
        sections.append("- Response to invalid input: [behavior]")
        sections.append("")
        sections.append("**Exception Handling:**")
        sections.append("- Expected exceptions: [list exceptions that may occur]")
        sections.append("- Handling strategy: [how each is handled]")
        sections.append("")
        sections.append("**Logging and Error Messages:**")
        sections.append("- Error logging level: [INFO, WARNING, ERROR, etc.]")
        sections.append("- Message format: [structure of error messages]")
        sections.append("")
        sections.append("**Graceful Degradation:**")
        sections.append("- Partial failure handling: [how partial failures are managed]")
        sections.append("- Cleanup on error: [what cleanup occurs]")
        sections.append("")
        sections.append("")
        
        # Correct Behavior Definition
        sections.append("## CORRECT BEHAVIOR DEFINITION")
        sections.append("")
        sections.append("### What 'Working Correctly' Means")
        sections.append("")
        sections.append("This script works correctly when:")
        sections.append("")
        sections.append("1. [Behavioral criterion 1]")
        sections.append("2. [Behavioral criterion 2]")
        sections.append("3. [Behavioral criterion 3]")
        sections.append("")
        sections.append("### Learning Model Behavior Guidelines")
        sections.append("")
        sections.append("When a naive learning model implements this script, it should:")
        sections.append("")
        sections.append("1. Preserve the exact input/output contract")
        sections.append("2. Implement identical error handling logic")
        sections.append("3. Maintain the same algorithmic approach")
        sections.append("4. Follow the code structure described in this prompt")
        sections.append("5. Include all validation and edge case handling")
        sections.append("")
        sections.append("")
        
        # Example Scenarios
        sections.append("## EXAMPLE SCENARIOS")
        sections.append("")
        sections.append("### Example 1: Typical Use Case")
        sections.append("")
        sections.append("**Scenario:** [Describe normal usage]")
        sections.append("")
        sections.append("**Input:**")
        sections.append("```")
        sections.append("[Provide concrete example input]")
        sections.append("```")
        sections.append("")
        sections.append("**Expected Output:**")
        sections.append("```")
        sections.append("[Provide expected output]")
        sections.append("```")
        sections.append("")
        sections.append("**Process Flow:**")
        sections.append("1. [Step 1]")
        sections.append("2. [Step 2]")
        sections.append("3. [Step 3]")
        sections.append("")
        
        sections.append("### Example 2: Edge Case")
        sections.append("")
        sections.append("**Scenario:** [Describe edge case]")
        sections.append("")
        sections.append("**Input:**")
        sections.append("```")
        sections.append("[Edge case input]")
        sections.append("```")
        sections.append("")
        sections.append("**Expected Behavior:**")
        sections.append("[How the script should handle this]")
        sections.append("")
        
        sections.append("### Example 3: Error Handling")
        sections.append("")
        sections.append("**Scenario:** [Describe error condition]")
        sections.append("")
        sections.append("**Input:**")
        sections.append("```")
        sections.append("[Invalid or problematic input]")
        sections.append("```")
        sections.append("")
        sections.append("**Expected Error Handling:**")
        sections.append("[How the error should be caught and handled]")
        sections.append("")
        sections.append("")
        
        # Test Cases
        sections.append("## TEST CASES")
        sections.append("")
        sections.append("### Unit Tests")
        sections.append("")
        sections.append("The following unit tests should pass:")
        sections.append("")
        
        for func in analysis.get('functions', [])[:3]:  # First 3 functions
            sections.append(f"**Test: test_{func['name']}**")
            sections.append("- Input: [test input]")
            sections.append("- Expected output: [expected result]")
            sections.append("- Validates: [what aspect of functionality]")
            sections.append("")
        
        sections.append("### Integration Tests")
        sections.append("")
        sections.append("[Tests that validate interaction with other components]")
        sections.append("")
        
        sections.append("### Validation Tests")
        sections.append("")
        sections.append("[Tests that verify output correctness and format]")
        sections.append("")
        sections.append("")
        
        # Implementation Guide
        sections.append("## IMPLEMENTATION GUIDE")
        sections.append("")
        sections.append("### Step-by-Step Implementation")
        sections.append("")
        sections.append("To implement this script from scratch:")
        sections.append("")
        sections.append("1. **Set up imports and dependencies**")
        sections.append("   - Import required standard library modules")
        sections.append("   - Import third-party libraries")
        sections.append("")
        sections.append("2. **Define constants and configuration**")
        sections.append("   - Set module-level constants")
        sections.append("   - Define configuration parameters")
        sections.append("")
        sections.append("3. **Implement data structures**")
        sections.append("   - Define classes with their methods")
        sections.append("   - Set up any custom data types")
        sections.append("")
        sections.append("4. **Implement core logic**")
        sections.append("   - Build main processing functions")
        sections.append("   - Implement algorithms")
        sections.append("")
        sections.append("5. **Add input/output handling**")
        sections.append("   - Implement file reading/writing")
        sections.append("   - Handle command-line arguments")
        sections.append("")
        sections.append("6. **Implement error handling**")
        sections.append("   - Add try/except blocks")
        sections.append("   - Implement validation")
        sections.append("")
        sections.append("7. **Add logging and debugging**")
        sections.append("   - Set up logging")
        sections.append("   - Add debug output")
        sections.append("")
        sections.append("8. **Create main execution block**")
        sections.append("   - Implement main() function")
        sections.append("   - Add if __name__ == '__main__' block")
        sections.append("")
        
        sections.append("### Recommended Code Structure")
        sections.append("")
        sections.append("```")
        sections.append("1. Module docstring")
        sections.append("2. Imports (standard library, then third-party, then local)")
        sections.append("3. Module-level constants")
        sections.append("4. Helper functions")
        sections.append("5. Class definitions")
        sections.append("6. Main logic functions")
        sections.append("7. Main entry point")
        sections.append("```")
        sections.append("")
        sections.append("")
        
        # Actual Script Content
        sections.append("## ACTUAL SCRIPT CONTENT")
        sections.append("")
        sections.append("```python")
        sections.append(analysis.get('content', ''))
        sections.append("```")
        sections.append("")
        sections.append("")
        
        # Revision History
        sections.append("## REVISION HISTORY")
        sections.append("")
        sections.append(f"- {datetime.now().strftime('%Y-%m-%d')}: Initial prompt generation")
        sections.append("")
        sections.append("")
        
        # Notes for Learning Model
        sections.append("## NOTES FOR LEARNING MODEL")
        sections.append("")
        sections.append("**Key Points to Remember:**")
        sections.append("")
        sections.append("- Pay special attention to: [Critical aspects]")
        sections.append("- Common pitfalls to avoid: [Known issues]")
        sections.append("- Best practices to follow: [Recommended patterns]")
        sections.append("- Performance considerations: [Optimization notes]")
        sections.append("")
        sections.append("**Learning Objectives:**")
        sections.append("")
        sections.append("After studying this script, you should be able to:")
        sections.append("1. [Learning objective 1]")
        sections.append("2. [Learning objective 2]")
        sections.append("3. [Learning objective 3]")
        sections.append("")
        
        return '\n'.join(sections)
    
    def _format_domain_terms(self, analysis: Dict) -> str:
        """Format domain-specific terminology."""
        terms = []
        
        for cls in analysis.get('classes', []):
            terms.append(f"- **{cls['name']}**: [Define the purpose and role of this class]")
        
        for func in analysis.get('functions', []):
            if not func['name'].startswith('_'):  # Skip private functions
                terms.append(f"- **{func['name']}**: [Define what this function does]")
        
        return '\n'.join(terms) if terms else "[Define domain-specific terms used in this script]"
    
    def _format_classes(self, classes: List[Dict]) -> str:
        """Format class information."""
        if not classes:
            return "No classes defined in this script."
        
        result = []
        for cls in classes:
            result.append(f"**{cls['name']}**")
            if cls['docstring']:
                result.append(f"  Description: {cls['docstring']}")
            if cls['bases']:
                result.append(f"  Inherits from: {', '.join(cls['bases'])}")
            if cls['methods']:
                result.append(f"  Methods:")
                for method in cls['methods']:
                    args_str = ', '.join(method['args'])
                    result.append(f"    - {method['name']}({args_str})")
                    if method['docstring']:
                        result.append(f"      {method['docstring']}")
            result.append("")
        
        return '\n'.join(result)
    
    def _format_functions(self, functions: List[Dict]) -> str:
        """Format function information."""
        if not functions:
            return "No standalone functions defined."
        
        result = []
        for func in functions:
            args_str = ', '.join(func['args'])
            result.append(f"**{func['name']}({args_str})**")
            if func['docstring']:
                result.append(f"  {func['docstring']}")
            if func['returns']:
                result.append(f"  Returns: {func['returns']}")
            if func['decorators']:
                result.append(f"  Decorators: {', '.join(func['decorators'])}")
            result.append("")
        
        return '\n'.join(result)
    
    def _format_list(self, items: List[str]) -> str:
        """Format a list of items."""
        if not items:
            return "None"
        return '\n'.join(f"- {item}" for item in sorted(set(items)))
    
    def batch_generate(self, directory: Path, pattern: str = "*.py", force: bool = False):
        """
        Generate prompts for all Python scripts in a directory.
        
        Args:
            directory: Directory containing Python scripts
            pattern: Glob pattern for matching files (default: *.py)
            force: Regenerate even if prompt file already exists
        """
        script_files = list(directory.glob(pattern))
        
        # Exclude sync scripts and internal files
        script_files = [f for f in script_files 
                       if not f.name.startswith('sync_') 
                       and not f.name.startswith('_')
                       and not f.name.startswith('.')]
        
        print(f"Found {len(script_files)} scripts to process\n")
        
        for script_path in script_files:
            prompt_path = script_path.parent / f"{script_path.stem}_prompt.txt"
            
            if prompt_path.exists() and not force:
                print(f"⏭️  Skipping {script_path.name} (prompt exists)")
                continue
            
            self.generate_prompt(script_path, prompt_path)
        
        print(f"\n✓ Batch generation complete")


def main():
    """Main entry point for the prompt generator."""
    parser = argparse.ArgumentParser(
        description="Generate comprehensive prompt files for Python scripts"
    )
    parser.add_argument(
        'script',
        nargs='?',
        help='Path to Python script (or directory for batch mode)'
    )
    parser.add_argument(
        '--output', '-o',
        help='Output path for prompt file'
    )
    parser.add_argument(
        '--batch', '-b',
        action='store_true',
        help='Batch process all scripts in directory'
    )
    parser.add_argument(
        '--pattern', '-p',
        default='*.py',
        help='File pattern for batch mode (default: *.py)'
    )
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Overwrite existing prompt files'
    )
    
    args = parser.parse_args()
    
    generator = PromptGenerator()
    
    if not args.script:
        args.script = '.'
        args.batch = True
    
    script_path = Path(args.script)
    
    if args.batch or script_path.is_dir():
        generator.batch_generate(script_path, args.pattern, args.force)
    else:
        output_path = Path(args.output) if args.output else None
        generator.generate_prompt(script_path, output_path)


if __name__ == '__main__':
    main()
