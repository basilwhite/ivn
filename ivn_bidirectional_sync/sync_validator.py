"""
Synchronization Validator for Script-Prompt Pairs

This script validates that Python scripts and their corresponding prompt files
are properly synchronized and contain all required information.
"""

import argparse
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import re


class SyncValidator:
    """Validates synchronization between scripts and prompts."""
    
    # Required sections in every prompt file
    REQUIRED_SECTIONS = [
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
    
    def __init__(self, directory: str = '.'):
        """
        Initialize the validator.
        
        Args:
            directory: Directory to validate
        """
        self.directory = Path(directory)
        self.validation_results = []
    
    def find_script_pairs(self) -> List[Tuple[Path, Path]]:
        """
        Find all script-prompt pairs in the directory.
        
        Returns:
            List of tuples (script_path, prompt_path)
        """
        pairs = []
        
        # Find all .py files (excluding internal files)
        py_files = [f for f in self.directory.glob("*.py") 
                    if not f.name.startswith('sync_') 
                    and not f.name.startswith('_')
                    and not f.name.startswith('.')]
        
        for py_file in py_files:
            prompt_name = py_file.stem + "_prompt.txt"
            prompt_file = self.directory / prompt_name
            pairs.append((py_file, prompt_file))
        
        return pairs
    
    def validate_pair(self, script_path: Path, prompt_path: Path) -> Dict:
        """
        Validate a single script-prompt pair.
        
        Args:
            script_path: Path to Python script
            prompt_path: Path to prompt file
            
        Returns:
            Dictionary with validation results
        """
        result = {
            'script': str(script_path),
            'prompt': str(prompt_path),
            'timestamp': datetime.now().isoformat(),
            'errors': [],
            'warnings': [],
            'info': [],
            'valid': True
        }
        
        # Check if files exist
        if not script_path.exists():
            result['errors'].append(f"Script file not found: {script_path}")
            result['valid'] = False
            return result
        
        if not prompt_path.exists():
            result['errors'].append(f"Prompt file not found: {prompt_path}")
            result['valid'] = False
            return result
        
        # Read files
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                script_content = f.read()
        except Exception as e:
            result['errors'].append(f"Cannot read script: {str(e)}")
            result['valid'] = False
            return result
        
        try:
            with open(prompt_path, 'r', encoding='utf-8') as f:
                prompt_content = f.read()
        except Exception as e:
            result['errors'].append(f"Cannot read prompt: {str(e)}")
            result['valid'] = False
            return result
        
        # Validate prompt structure
        self._validate_prompt_structure(prompt_content, result)
        
        # Validate script content in prompt
        self._validate_script_content(script_content, prompt_content, result)
        
        # Validate completeness
        self._validate_completeness(prompt_content, result)
        
        # Check for placeholder text
        self._check_placeholders(prompt_content, result)
        
        # Validate synchronization freshness
        self._validate_freshness(script_path, prompt_path, result)
        
        # Set overall validity
        result['valid'] = len(result['errors']) == 0
        
        return result
    
    def _validate_prompt_structure(self, prompt_content: str, result: Dict):
        """Validate that all required sections are present."""
        missing_sections = []
        
        for section in self.REQUIRED_SECTIONS:
            # Look for section header (## SECTION_NAME)
            pattern = rf'##\s+{re.escape(section)}'
            if not re.search(pattern, prompt_content, re.IGNORECASE):
                missing_sections.append(section)
        
        if missing_sections:
            result['errors'].append(
                f"Missing required sections: {', '.join(missing_sections)}"
            )
    
    def _validate_script_content(self, script_content: str, prompt_content: str, result: Dict):
        """Validate that the prompt contains the actual script content."""
        # Check if script content is embedded in prompt
        if '```python' not in prompt_content:
            result['errors'].append("Prompt does not contain code block with script content")
            return
        
        # Extract code blocks
        code_blocks = re.findall(r'```python\n(.*?)```', prompt_content, re.DOTALL)
        
        if not code_blocks:
            result['errors'].append("No Python code blocks found in prompt")
            return
        
        # Find the largest code block (should be the actual script)
        largest_block = max(code_blocks, key=len)
        
        # Normalize whitespace for comparison
        script_normalized = script_content.strip()
        block_normalized = largest_block.strip()
        
        # Check if script content matches
        if script_normalized != block_normalized:
            # Calculate similarity
            similarity = self._calculate_similarity(script_normalized, block_normalized)
            
            if similarity < 0.95:  # Less than 95% similar
                result['warnings'].append(
                    f"Script content in prompt may be outdated (similarity: {similarity:.1%})"
                )
            else:
                result['info'].append(
                    f"Script content matches (similarity: {similarity:.1%})"
                )
    
    def _validate_completeness(self, prompt_content: str, result: Dict):
        """Check that sections are not just headers but contain content."""
        # Define minimum content length for each section (in characters)
        min_content_length = 50
        
        # Split content by sections
        sections = re.split(r'##\s+', prompt_content)
        
        incomplete_sections = []
        for section in sections[1:]:  # Skip first split (before first ##)
            lines = section.strip().split('\n')
            if len(lines) < 1:
                continue
            
            section_name = lines[0].strip()
            section_content = '\n'.join(lines[1:]).strip()
            
            # Check if section has meaningful content
            if len(section_content) < min_content_length:
                incomplete_sections.append(section_name)
        
        if incomplete_sections:
            result['warnings'].append(
                f"Sections with minimal content: {', '.join(incomplete_sections)}"
            )
    
    def _check_placeholders(self, prompt_content: str, result: Dict):
        """Check for unresolved placeholder text."""
        placeholders = [
            r'\[.*?\]',  # [Describe something]
            r'TODO',
            r'FIXME',
            r'XXX'
        ]
        
        found_placeholders = []
        for pattern in placeholders:
            matches = re.findall(pattern, prompt_content)
            found_placeholders.extend(matches)
        
        if found_placeholders:
            unique_placeholders = list(set(found_placeholders))[:5]  # First 5 unique
            result['warnings'].append(
                f"Found {len(found_placeholders)} placeholder(s): {', '.join(unique_placeholders)}"
            )
    
    def _validate_freshness(self, script_path: Path, prompt_path: Path, result: Dict):
        """Check if prompt is up-to-date with script."""
        script_mtime = script_path.stat().st_mtime
        prompt_mtime = prompt_path.stat().st_mtime
        
        # If script was modified after prompt, it might be out of sync
        if script_mtime > prompt_mtime:
            time_diff = script_mtime - prompt_mtime
            
            # If more than 1 minute difference, warn
            if time_diff > 60:
                result['warnings'].append(
                    f"Script modified after prompt (difference: {self._format_time_diff(time_diff)})"
                )
    
    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity ratio between two texts."""
        # Simple similarity based on character overlap
        # For production, consider using difflib.SequenceMatcher
        
        if not text1 or not text2:
            return 0.0
        
        # Count matching characters in order
        matches = sum(c1 == c2 for c1, c2 in zip(text1, text2))
        max_len = max(len(text1), len(text2))
        
        return matches / max_len if max_len > 0 else 0.0
    
    def _format_time_diff(self, seconds: float) -> str:
        """Format time difference in human-readable format."""
        if seconds < 60:
            return f"{int(seconds)} seconds"
        elif seconds < 3600:
            return f"{int(seconds / 60)} minutes"
        elif seconds < 86400:
            return f"{int(seconds / 3600)} hours"
        else:
            return f"{int(seconds / 86400)} days"
    
    def validate_all(self) -> List[Dict]:
        """
        Validate all script-prompt pairs in the directory.
        
        Returns:
            List of validation results
        """
        pairs = self.find_script_pairs()
        
        if not pairs:
            print("⚠️  No script-prompt pairs found")
            return []
        
        results = []
        for script_path, prompt_path in pairs:
            result = self.validate_pair(script_path, prompt_path)
            results.append(result)
        
        self.validation_results = results
        return results
    
    def print_report(self, results: List[Dict] = None):
        """
        Print a human-readable validation report.
        
        Args:
            results: Validation results (uses self.validation_results if None)
        """
        if results is None:
            results = self.validation_results
        
        if not results:
            print("No validation results to report")
            return
        
        print("\n" + "=" * 80)
        print("SYNCHRONIZATION VALIDATION REPORT")
        print("=" * 80)
        print(f"Directory: {self.directory}")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total pairs checked: {len(results)}")
        print("=" * 80 + "\n")
        
        valid_count = sum(1 for r in results if r['valid'])
        invalid_count = len(results) - valid_count
        
        print(f"✓ Valid: {valid_count}")
        print(f"✗ Invalid: {invalid_count}\n")
        
        for i, result in enumerate(results, 1):
            script_name = Path(result['script']).name
            prompt_name = Path(result['prompt']).name
            
            status = "✓ VALID" if result['valid'] else "✗ INVALID"
            print(f"{i}. {script_name} ↔ {prompt_name}")
            print(f"   Status: {status}")
            
            if result['errors']:
                print(f"   Errors ({len(result['errors'])}):")
                for error in result['errors']:
                    print(f"     ✗ {error}")
            
            if result['warnings']:
                print(f"   Warnings ({len(result['warnings'])}):")
                for warning in result['warnings']:
                    print(f"     ⚠ {warning}")
            
            if result['info']:
                print(f"   Info ({len(result['info'])}):")
                for info in result['info']:
                    print(f"     ℹ {info}")
            
            print()
        
        print("=" * 80)
        print(f"Summary: {valid_count}/{len(results)} pairs are valid")
        print("=" * 80 + "\n")
    
    def save_report(self, output_path: Path = None):
        """
        Save validation report as JSON.
        
        Args:
            output_path: Path to save report (default: validation_report.json)
        """
        if output_path is None:
            output_path = self.directory / "validation_report.json"
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'directory': str(self.directory),
            'total_pairs': len(self.validation_results),
            'valid_pairs': sum(1 for r in self.validation_results if r['valid']),
            'results': self.validation_results
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        print(f"✓ Report saved to: {output_path}")
    
    def get_invalid_pairs(self) -> List[Dict]:
        """Get list of invalid pairs."""
        return [r for r in self.validation_results if not r['valid']]
    
    def get_warnings(self) -> List[Tuple[str, List[str]]]:
        """Get all warnings across all pairs."""
        warnings = []
        for result in self.validation_results:
            if result['warnings']:
                warnings.append((result['script'], result['warnings']))
        return warnings


def main():
    """Main entry point for the validator."""
    parser = argparse.ArgumentParser(
        description="Validate synchronization between scripts and prompts"
    )
    parser.add_argument(
        '--directory', '-d',
        default='.',
        help='Directory to validate (default: current directory)'
    )
    parser.add_argument(
        '--output', '-o',
        help='Save JSON report to specified file'
    )
    parser.add_argument(
        '--json-only', '-j',
        action='store_true',
        help='Only output JSON report (no console output)'
    )
    parser.add_argument(
        '--show-valid', '-v',
        action='store_true',
        help='Show details for valid pairs too'
    )
    
    args = parser.parse_args()
    
    validator = SyncValidator(args.directory)
    results = validator.validate_all()
    
    if not args.json_only:
        validator.print_report(results)
    
    if args.output or args.json_only:
        output_path = Path(args.output) if args.output else None
        validator.save_report(output_path)
    
    # Exit with error code if any pairs are invalid
    invalid_count = len(validator.get_invalid_pairs())
    if invalid_count > 0:
        exit(1)
    else:
        exit(0)


if __name__ == '__main__':
    main()
