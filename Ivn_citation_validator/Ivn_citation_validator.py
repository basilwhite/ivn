"""
IVN Citation Validator
======================
This script validates that component citations (descriptions, sources, and URLs) 
are preserved correctly when transforming ivntest.xlsx to CSV output.

It identifies corruptions where components have misassigned or duplicated 
descriptions/sources/URLs, then iteratively fixes the Python script causing the issues.
"""

import pandas as pd
import os
import sys
import shutil
import subprocess
from datetime import datetime
from typing import Dict, List, Tuple, Set
import difflib
import re
import tkinter as tk
from tkinter import filedialog


class CitationValidator:
    """Validates and fixes citation integrity issues in IVN data transformations."""
    
    def __init__(self):
        self.input_file = None
        self.output_file = None
        self.script_file = None
        self.corruption_profile = []
        self.max_iterations = 10
        self.backup_dir = "script_backups"
    
    def prompt_for_files(self):
        """Prompt user for input files and script using file dialogs."""
        print("=" * 70)
        print("IVN Citation Validator")
        print("=" * 70)
        print()
        print("Please select the required files using the file dialogs...")
        print()
        
        # Create a hidden root window
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        
        # Prompt for input Excel file
        print("1. Select the input Excel file (ivntest.xlsx)...")
        self.input_file = filedialog.askopenfilename(
            title="Select Input Excel File (ivntest.xlsx)",
            filetypes=[
                ("Excel files", "*.xlsx *.xls"),
                ("All files", "*.*")
            ]
        )
        
        if not self.input_file:
            print("Error: No input file selected. Exiting.")
            sys.exit(1)
        
        print(f"   Selected: {self.input_file}")
        print()
        
        # Prompt for output CSV file
        print("2. Select the output CSV file...")
        self.output_file = filedialog.askopenfilename(
            title="Select Output CSV File",
            filetypes=[
                ("CSV files", "*.csv"),
                ("All files", "*.*")
            ]
        )
        
        if not self.output_file:
            print("Error: No output file selected. Exiting.")
            sys.exit(1)
        
        print(f"   Selected: {self.output_file}")
        print()
        
        # Prompt for Python script
        print("3. Select the Python script that created the CSV...")
        self.script_file = filedialog.askopenfilename(
            title="Select Python Script",
            filetypes=[
                ("Python files", "*.py"),
                ("All files", "*.*")
            ]
        )
        
        if not self.script_file:
            print("Error: No script file selected. Exiting.")
            sys.exit(1)
        print(f"   Selected: {self.script_file}")
        print()
        
        # Destroy the root window
        root.destroy()
        
        print("-" * 70)
        print("Files selected:")
        print(f"  Input file:  {self.input_file}")
        print(f"  Output file: {self.output_file}")
        print(f"  Script file: {self.script_file}")
        print("-" * 70)
        print()
    
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load input Excel and output CSV files with retry capability."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            print("Loading data files...")
            
            try:
                # Load Excel file
                input_df = pd.read_excel(self.input_file)
                print(f"  ✓ Loaded {len(input_df)} rows from {os.path.basename(self.input_file)}")
                
                # Load CSV file
                output_df = pd.read_csv(self.output_file)
                print(f"  ✓ Loaded {len(output_df)} rows from {os.path.basename(self.output_file)}")
                
                return input_df, output_df
                
            except PermissionError as e:
                retry_count += 1
                print(f"\n{'='*70}")
                print("ERROR: Permission denied when accessing files")
                print(f"{'='*70}")
                print(f"\nCannot access: {e.filename}")
                print("\nMost common cause:")
                print("  → The file is currently OPEN in Excel or another program")
                print("\nPlease:")
                print("  1. Close the Excel file")
                print("  2. Close the CSV file (if open)")
                print("  3. Save any changes first if needed")
                print(f"\nFiles being accessed:")
                print(f"  - {os.path.basename(self.input_file)}")
                print(f"  - {os.path.basename(self.output_file)}")
                
                if retry_count < max_retries:
                    print(f"\nAttempt {retry_count} of {max_retries}")
                    response = input(f"\nClose the files and press ENTER to retry (or 'q' to quit): ").strip().lower()
                    if response == 'q':
                        print("\nExiting...")
                        sys.exit(0)
                    print()  # Add blank line before retry
                else:
                    print(f"\n{'='*70}")
                    print(f"Maximum retry attempts ({max_retries}) reached.")
                    print("Please close all files and run the script again.")
                    print(f"{'='*70}")
                    sys.exit(1)
                    
            except FileNotFoundError as e:
                print(f"\n{'='*70}")
                print("ERROR: File not found")
                print(f"{'='*70}")
                print(f"\nCannot find: {e.filename}")
                print("\nPlease verify the file path is correct.")
                print(f"{'='*70}")
                sys.exit(1)
            except Exception as e:
                print(f"\n{'='*70}")
                print("ERROR: Unexpected error loading data")
                print(f"{'='*70}")
                print(f"\nError type: {type(e).__name__}")
                print(f"Details: {e}")
                print("\nPlease check:")
                print("  1. The file format is correct (Excel for input, CSV for output)")
                print("  2. The files are not corrupted")
                print("  3. You have sufficient permissions to read the files")
                print(f"{'='*70}")
                sys.exit(1)
    
    def identify_component_columns(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Identify component, description, source, and URL columns."""
        component_cols = []
        description_cols = []
        source_cols = []
        url_cols = []
        
        for col in df.columns:
            col_lower = col.lower()
            
            # Component columns
            if any(term in col_lower for term in ['component', 'unaligned', 'enabling', 'dependent']):
                if 'description' not in col_lower and 'source' not in col_lower and 'url' not in col_lower:
                    component_cols.append(col)
            
            # Description columns
            if 'description' in col_lower:
                description_cols.append(col)
            
            # Source columns
            if 'source' in col_lower and 'url' not in col_lower:
                source_cols.append(col)
            
            # URL columns
            if 'url' in col_lower:
                url_cols.append(col)
        
        return {
            'components': component_cols,
            'descriptions': description_cols,
            'sources': source_cols,
            'urls': url_cols
        }
    
    def validate_citations(self, input_df: pd.DataFrame, output_df: pd.DataFrame) -> List[Dict]:
        """Perform multiple quality checks to validate citation integrity."""
        print("\nPerforming citation validation checks...")
        print("-" * 70)
        
        corruptions = []
        
        # Identify columns in both dataframes
        input_cols = self.identify_component_columns(input_df)
        output_cols = self.identify_component_columns(output_df)
        
        print(f"Input file columns:")
        print(f"  Components: {input_cols['components']}")
        print(f"  Descriptions: {input_cols['descriptions']}")
        print(f"  Sources: {input_cols['sources']}")
        print(f"  URLs: {input_cols['urls']}")
        print()
        print(f"Output file columns:")
        print(f"  Components: {output_cols['components']}")
        print(f"  Descriptions: {output_cols['descriptions']}")
        print(f"  Sources: {output_cols['sources']}")
        print(f"  URLs: {output_cols['urls']}")
        print()
        
        # Check 1: One-to-one mapping validation
        corruptions.extend(self._check_one_to_one_mapping(input_df, output_df, input_cols, output_cols))
        
        # Check 2: Citation consistency across rows
        corruptions.extend(self._check_citation_consistency(input_df, output_df, input_cols, output_cols))
        
        # Check 3: Missing or null citations
        corruptions.extend(self._check_missing_citations(output_df, output_cols))
        
        # Check 4: Duplicate components with different citations
        corruptions.extend(self._check_duplicate_components(output_df, output_cols))
        
        # Check 5: Citation swapping detection
        corruptions.extend(self._check_citation_swapping(input_df, output_df, input_cols, output_cols))
        
        return corruptions
    
    def _check_one_to_one_mapping(self, input_df: pd.DataFrame, output_df: pd.DataFrame,
                                   input_cols: Dict, output_cols: Dict) -> List[Dict]:
        """Check that each component maps to exactly one description, source, and URL."""
        print("Check 1: One-to-one mapping validation...")
        corruptions = []
        
        # Build lookup dictionary from input
        input_lookup = {}
        for comp_col in input_cols['components']:
            if comp_col not in input_df.columns:
                continue
                
            desc_col = self._find_matching_column(comp_col, input_cols['descriptions'], 'description')
            source_col = self._find_matching_column(comp_col, input_cols['sources'], 'source')
            url_col = self._find_matching_column(comp_col, input_cols['urls'], 'url')
            
            for idx, row in input_df.iterrows():
                component = str(row[comp_col]) if pd.notna(row[comp_col]) else None
                if not component or component == 'nan':
                    continue
                
                description = str(row[desc_col]) if desc_col and pd.notna(row.get(desc_col)) else None
                source = str(row[source_col]) if source_col and pd.notna(row.get(source_col)) else None
                url = str(row[url_col]) if url_col and pd.notna(row.get(url_col)) else None
                
                if component not in input_lookup:
                    input_lookup[component] = {
                        'description': description,
                        'source': source,
                        'url': url,
                        'component_type': comp_col
                    }
        
        # Validate output against input lookup
        for comp_col in output_cols['components']:
            if comp_col not in output_df.columns:
                continue
                
            desc_col = self._find_matching_column(comp_col, output_cols['descriptions'], 'description')
            source_col = self._find_matching_column(comp_col, output_cols['sources'], 'source')
            url_col = self._find_matching_column(comp_col, output_cols['urls'], 'url')
            
            for idx, row in output_df.iterrows():
                component = str(row[comp_col]) if pd.notna(row[comp_col]) else None
                if not component or component == 'nan':
                    continue
                
                description = str(row[desc_col]) if desc_col and pd.notna(row.get(desc_col)) else None
                source = str(row[source_col]) if source_col and pd.notna(row.get(source_col)) else None
                url = str(row[url_col]) if url_col and pd.notna(row.get(url_col)) else None
                
                if component in input_lookup:
                    expected = input_lookup[component]
                    
                    if description != expected['description']:
                        corruptions.append({
                            'type': 'description_mismatch',
                            'row': idx,
                            'component': component,
                            'component_column': comp_col,
                            'expected': expected['description'],
                            'actual': description,
                            'field': 'description'
                        })
                    
                    if source != expected['source']:
                        corruptions.append({
                            'type': 'source_mismatch',
                            'row': idx,
                            'component': component,
                            'component_column': comp_col,
                            'expected': expected['source'],
                            'actual': source,
                            'field': 'source'
                        })
                    
                    if url != expected['url']:
                        corruptions.append({
                            'type': 'url_mismatch',
                            'row': idx,
                            'component': component,
                            'component_column': comp_col,
                            'expected': expected['url'],
                            'actual': url,
                            'field': 'url'
                        })
        
        if corruptions:
            print(f"  Found {len(corruptions)} one-to-one mapping violations")
        else:
            print(f"  ✓ No one-to-one mapping violations found")
        
        return corruptions
    
    def _check_citation_consistency(self, input_df: pd.DataFrame, output_df: pd.DataFrame,
                                     input_cols: Dict, output_cols: Dict) -> List[Dict]:
        """Check that the same component always has the same citations."""
        print("Check 2: Citation consistency validation...")
        corruptions = []
        
        # Check output file for inconsistent citations for the same component
        component_citations = {}
        
        for comp_col in output_cols['components']:
            if comp_col not in output_df.columns:
                continue
                
            desc_col = self._find_matching_column(comp_col, output_cols['descriptions'], 'description')
            source_col = self._find_matching_column(comp_col, output_cols['sources'], 'source')
            url_col = self._find_matching_column(comp_col, output_cols['urls'], 'url')
            
            for idx, row in output_df.iterrows():
                component = str(row[comp_col]) if pd.notna(row[comp_col]) else None
                if not component or component == 'nan':
                    continue
                
                description = str(row[desc_col]) if desc_col and pd.notna(row.get(desc_col)) else None
                source = str(row[source_col]) if source_col and pd.notna(row.get(source_col)) else None
                url = str(row[url_col]) if url_col and pd.notna(row.get(url_col)) else None
                
                citation_key = (component, comp_col)
                citation_value = (description, source, url)
                
                if citation_key in component_citations:
                    if component_citations[citation_key] != citation_value:
                        corruptions.append({
                            'type': 'inconsistent_citation',
                            'row': idx,
                            'component': component,
                            'component_column': comp_col,
                            'expected': component_citations[citation_key],
                            'actual': citation_value,
                            'field': 'all'
                        })
                else:
                    component_citations[citation_key] = citation_value
        
        if corruptions:
            print(f"  Found {len(corruptions)} citation consistency violations")
        else:
            print(f"  ✓ No citation consistency violations found")
        
        return corruptions
    
    def _check_missing_citations(self, output_df: pd.DataFrame, output_cols: Dict) -> List[Dict]:
        """Check for components with missing descriptions, sources, or URLs."""
        print("Check 3: Missing citation validation...")
        corruptions = []
        
        for comp_col in output_cols['components']:
            if comp_col not in output_df.columns:
                continue
                
            desc_col = self._find_matching_column(comp_col, output_cols['descriptions'], 'description')
            source_col = self._find_matching_column(comp_col, output_cols['sources'], 'source')
            url_col = self._find_matching_column(comp_col, output_cols['urls'], 'url')
            
            for idx, row in output_df.iterrows():
                component = str(row[comp_col]) if pd.notna(row[comp_col]) else None
                if not component or component == 'nan':
                    continue
                
                description = row.get(desc_col) if desc_col else None
                source = row.get(source_col) if source_col else None
                url = row.get(url_col) if url_col else None
                
                if desc_col and (pd.isna(description) or str(description) == 'nan'):
                    corruptions.append({
                        'type': 'missing_description',
                        'row': idx,
                        'component': component,
                        'component_column': comp_col,
                        'field': 'description'
                    })
                
                if source_col and (pd.isna(source) or str(source) == 'nan'):
                    corruptions.append({
                        'type': 'missing_source',
                        'row': idx,
                        'component': component,
                        'component_column': comp_col,
                        'field': 'source'
                    })
                
                if url_col and (pd.isna(url) or str(url) == 'nan'):
                    corruptions.append({
                        'type': 'missing_url',
                        'row': idx,
                        'component': component,
                        'component_column': comp_col,
                        'field': 'url'
                    })
        
        if corruptions:
            print(f"  Found {len(corruptions)} missing citation violations")
        else:
            print(f"  ✓ No missing citation violations found")
        
        return corruptions
    
    def _check_duplicate_components(self, output_df: pd.DataFrame, output_cols: Dict) -> List[Dict]:
        """Check for duplicate component values with different citations."""
        print("Check 4: Duplicate component validation...")
        corruptions = []
        
        for comp_col in output_cols['components']:
            if comp_col not in output_df.columns:
                continue
                
            desc_col = self._find_matching_column(comp_col, output_cols['descriptions'], 'description')
            source_col = self._find_matching_column(comp_col, output_cols['sources'], 'source')
            url_col = self._find_matching_column(comp_col, output_cols['urls'], 'url')
            
            # Group by component and check for different citations
            component_groups = {}
            
            for idx, row in output_df.iterrows():
                component = str(row[comp_col]) if pd.notna(row[comp_col]) else None
                if not component or component == 'nan':
                    continue
                
                description = str(row[desc_col]) if desc_col and pd.notna(row.get(desc_col)) else None
                source = str(row[source_col]) if source_col and pd.notna(row.get(source_col)) else None
                url = str(row[url_col]) if url_col and pd.notna(row.get(url_col)) else None
                
                citation = (description, source, url)
                
                if component not in component_groups:
                    component_groups[component] = set()
                component_groups[component].add(citation)
            
            # Find components with multiple different citations
            for component, citations in component_groups.items():
                if len(citations) > 1:
                    corruptions.append({
                        'type': 'duplicate_component_different_citations',
                        'component': component,
                        'component_column': comp_col,
                        'citation_count': len(citations),
                        'citations': list(citations),
                        'field': 'all'
                    })
        
        if corruptions:
            print(f"  Found {len(corruptions)} duplicate component violations")
        else:
            print(f"  ✓ No duplicate component violations found")
        
        return corruptions
    
    def _check_citation_swapping(self, input_df: pd.DataFrame, output_df: pd.DataFrame,
                                  input_cols: Dict, output_cols: Dict) -> List[Dict]:
        """Detect if citations from one component were assigned to another."""
        print("Check 5: Citation swapping detection...")
        corruptions = []
        
        # Build a set of all valid component-citation pairs from input
        valid_pairs = set()
        for comp_col in input_cols['components']:
            if comp_col not in input_df.columns:
                continue
                
            desc_col = self._find_matching_column(comp_col, input_cols['descriptions'], 'description')
            source_col = self._find_matching_column(comp_col, input_cols['sources'], 'source')
            
            for idx, row in input_df.iterrows():
                component = str(row[comp_col]) if pd.notna(row[comp_col]) else None
                description = str(row[desc_col]) if desc_col and pd.notna(row.get(desc_col)) else None
                source = str(row[source_col]) if source_col and pd.notna(row.get(source_col)) else None
                
                if component and component != 'nan':
                    if description and description != 'nan':
                        valid_pairs.add((component, 'description', description))
                    if source and source != 'nan':
                        valid_pairs.add((component, 'source', source))
        
        # Check output for invalid component-citation pairs
        for comp_col in output_cols['components']:
            if comp_col not in output_df.columns:
                continue
                
            desc_col = self._find_matching_column(comp_col, output_cols['descriptions'], 'description')
            source_col = self._find_matching_column(comp_col, output_cols['sources'], 'source')
            
            for idx, row in output_df.iterrows():
                component = str(row[comp_col]) if pd.notna(row[comp_col]) else None
                description = str(row[desc_col]) if desc_col and pd.notna(row.get(desc_col)) else None
                source = str(row[source_col]) if source_col and pd.notna(row.get(source_col)) else None
                
                if component and component != 'nan':
                    if description and description != 'nan':
                        if (component, 'description', description) not in valid_pairs:
                            corruptions.append({
                                'type': 'citation_swap',
                                'row': idx,
                                'component': component,
                                'component_column': comp_col,
                                'invalid_citation': description,
                                'field': 'description'
                            })
                    
                    if source and source != 'nan':
                        if (component, 'source', source) not in valid_pairs:
                            corruptions.append({
                                'type': 'citation_swap',
                                'row': idx,
                                'component': component,
                                'component_column': comp_col,
                                'invalid_citation': source,
                                'field': 'source'
                            })
        
        if corruptions:
            print(f"  Found {len(corruptions)} citation swapping violations")
        else:
            print(f"  ✓ No citation swapping violations found")
        
        return corruptions
    
    def _find_matching_column(self, component_col: str, related_cols: List[str], 
                              col_type: str) -> str:
        """Find the matching description/source/URL column for a component column."""
        component_type = None
        
        # Determine component type
        comp_lower = component_col.lower()
        if 'unaligned' in comp_lower:
            component_type = 'unaligned'
        elif 'enabling' in comp_lower:
            component_type = 'enabling'
        elif 'dependent' in comp_lower:
            component_type = 'dependent'
        
        # Try to find matching column
        for col in related_cols:
            col_lower = col.lower()
            if component_type and component_type in col_lower:
                return col
            # Fallback: just match on type
            if col_type in col_lower:
                return col
        
        return None
    
    def build_corruption_profile(self, corruptions: List[Dict]) -> Dict:
        """Build a profile of corruption patterns."""
        print("\nBuilding corruption profile...")
        print("-" * 70)
        
        if not corruptions:
            print("✓ No corruptions found! The script maintains citation integrity.")
            return {}
        
        profile = {
            'total_corruptions': len(corruptions),
            'by_type': {},
            'by_field': {},
            'by_component_column': {},
            'affected_components': set(),
            'patterns': []
        }
        
        for corruption in corruptions:
            # Count by type
            c_type = corruption['type']
            profile['by_type'][c_type] = profile['by_type'].get(c_type, 0) + 1
            
            # Count by field
            field = corruption.get('field', 'unknown')
            profile['by_field'][field] = profile['by_field'].get(field, 0) + 1
            
            # Count by component column
            comp_col = corruption.get('component_column', 'unknown')
            profile['by_component_column'][comp_col] = profile['by_component_column'].get(comp_col, 0) + 1
            
            # Track affected components
            if 'component' in corruption:
                profile['affected_components'].add(corruption['component'])
        
        # Print profile
        print(f"Total corruptions found: {profile['total_corruptions']}")
        print()
        print("By corruption type:")
        for c_type, count in sorted(profile['by_type'].items(), key=lambda x: -x[1]):
            print(f"  {c_type}: {count}")
        print()
        print("By field:")
        for field, count in sorted(profile['by_field'].items(), key=lambda x: -x[1]):
            print(f"  {field}: {count}")
        print()
        print("By component column:")
        for col, count in sorted(profile['by_component_column'].items(), key=lambda x: -x[1]):
            print(f"  {col}: {count}")
        print()
        print(f"Affected components: {len(profile['affected_components'])}")
          # Show sample corruptions
        print()
        print("Sample corruptions (first 5):")
        for i, corruption in enumerate(corruptions[:5], 1):
            print(f"\n  {i}. {corruption['type']}")
            for key, value in corruption.items():
                if key != 'type':
                    print(f"     {key}: {value}")
        
        self.corruption_profile = corruptions
        
        # Generate detailed mismatch report
        self._generate_mismatch_report(corruptions)
        
        return profile
    
    def _generate_mismatch_report(self, corruptions: List[Dict]):
        """Generate a detailed mismatch report with file names and row numbers."""
        if not corruptions:
            return
        
        print("\n" + "=" * 70)
        print("DETAILED MISMATCH REPORT")
        print("=" * 70)
        
        # Group corruptions by type
        by_type = {}
        for corruption in corruptions:
            c_type = corruption['type']
            if c_type not in by_type:
                by_type[c_type] = []
            by_type[c_type].append(corruption)
        
        # Report each type
        for c_type in sorted(by_type.keys()):
            items = by_type[c_type]
            print(f"\n{c_type.upper().replace('_', ' ')} ({len(items)} instances)")
            print("-" * 70)
            
            for i, item in enumerate(items, 1):
                print(f"\n  Issue #{i}:")
                
                # Always show component and location
                if 'component' in item:
                    print(f"    Component: {item['component']}")
                if 'component_column' in item:
                    print(f"    Column: {item['component_column']}")
                if 'row' in item:
                    print(f"    Row Number: {item['row']}")
                
                # Show field-specific information
                if c_type in ['description_mismatch', 'source_mismatch', 'url_mismatch']:
                    print(f"    Expected: {item.get('expected', 'N/A')}")
                    print(f"    Actual: {item.get('actual', 'N/A')}")
                    print(f"    Field: {item.get('field', 'N/A')}")
                
                elif c_type == 'citation_swap':
                    print(f"    Invalid Citation: {item.get('invalid_citation', 'N/A')}")
                    print(f"    Field: {item.get('field', 'N/A')}")
                
                elif c_type == 'duplicate_component_different_citations':
                    print(f"    Citation Count: {item.get('citation_count', 0)}")
                    if 'citations' in item:
                        print(f"    Different Citations Found:")
                        for j, citation in enumerate(item['citations'], 1):
                            print(f"      {j}. {citation}")
                
                elif c_type in ['missing_description', 'missing_source', 'missing_url']:
                    print(f"    Missing Field: {item.get('field', 'N/A')}")
                
                elif c_type == 'inconsistent_citation':
                    print(f"    Expected: {item.get('expected', 'N/A')}")
                    print(f"    Actual: {item.get('actual', 'N/A')}")
        
        # Save report to file
        self._save_report_to_file(corruptions, by_type)
    
    def _save_report_to_file(self, corruptions: List[Dict], by_type: Dict):
        """Save the detailed report to a CSV file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"citation_validation_report_{timestamp}.csv"
        
        try:
            import csv
            
            with open(report_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Write header
                writer.writerow([
                    'Issue Type',
                    'Component',
                    'Column',
                    'Row Number',
                    'Field',
                    'Expected Value',
                    'Actual Value',
                    'Additional Info'
                ])
                
                # Write data
                for corruption in corruptions:
                    writer.writerow([
                        corruption.get('type', ''),
                        corruption.get('component', ''),
                        corruption.get('component_column', ''),
                        corruption.get('row', ''),
                        corruption.get('field', ''),
                        str(corruption.get('expected', '')),
                        str(corruption.get('actual', '')),
                        str(corruption.get('invalid_citation', corruption.get('citation_count', '')))
                    ])
            
            print(f"\n{'='*70}")
            print(f"Detailed report saved to: {report_file}")
            print(f"{'='*70}")
            
        except Exception as e:
            print(f"\nWarning: Could not save report to file: {e}")
    
    def backup_script(self, iteration: int):
        """Create a backup of the current script."""
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{os.path.basename(self.script_file)}.backup_iter{iteration}_{timestamp}"
        backup_path = os.path.join(self.backup_dir, backup_name)
        
        shutil.copy2(self.script_file, backup_path)
        print(f"Backed up script to: {backup_path}")
        return backup_path
    
    def analyze_script(self) -> str:
        """Analyze the Python script to understand its structure."""
        with open(self.script_file, 'r', encoding='utf-8') as f:
            script_content = f.read()
        return script_content
    
    def generate_fix_suggestions(self, profile: Dict, script_content: str) -> List[Dict]:
        """Generate suggestions to fix the script based on corruption profile."""
        suggestions = []
        
        # Analyze corruption patterns
        if 'description_mismatch' in profile['by_type']:
            suggestions.append({
                'issue': 'Description mismatch detected',
                'suggestion': 'Check for operations that modify or reassign description columns. '
                             'Ensure descriptions stay linked to their original components.',
                'pattern': r'(description|desc)\s*=',
                'priority': 'high'
            })
        
        if 'source_mismatch' in profile['by_type']:
            suggestions.append({
                'issue': 'Source mismatch detected',
                'suggestion': 'Check for operations that modify or reassign source columns. '
                             'Ensure sources stay linked to their original components.',
                'pattern': r'(source|src)\s*=',
                'priority': 'high'
            })
        
        if 'url_mismatch' in profile['by_type']:
            suggestions.append({
                'issue': 'URL mismatch detected',
                'suggestion': 'Check for operations that modify or reassign URL columns. '
                             'Ensure URLs stay linked to their original components.',
                'pattern': r'(url|link)\s*=',
                'priority': 'high'
            })
        
        if 'missing_description' in profile['by_type'] or 'missing_source' in profile['by_type']:
            suggestions.append({
                'issue': 'Missing citations detected',
                'suggestion': 'Check for fillna(), dropna(), or column selection operations that might '
                             'exclude citation columns. Ensure all component-citation pairs are preserved.',
                'pattern': r'(fillna|dropna|drop)\s*\(',
                'priority': 'medium'
            })
        
        if 'inconsistent_citation' in profile['by_type'] or 'duplicate_component_different_citations' in profile['by_type']:
            suggestions.append({
                'issue': 'Inconsistent citations for same component',
                'suggestion': 'Check for merge/join operations that might create duplicate rows with '
                             'different citations. Use appropriate merge strategies (e.g., left, inner) '
                             'and verify key columns.',
                'pattern': r'(merge|join)\s*\(',
                'priority': 'high'
            })
        
        if 'citation_swap' in profile['by_type']:
            suggestions.append({
                'issue': 'Citation swapping detected',
                'suggestion': 'Check for row indexing or sorting operations that might misalign components '
                             'with their citations. Ensure all operations maintain row integrity.',
                'pattern': r'(sort_values|reindex|reset_index|iloc|loc)\s*[\[\(]',
                'priority': 'critical'
            })
        
        # Look for common problematic patterns in the script
        if 'merge' in script_content or 'join' in script_content:
            suggestions.append({
                'issue': 'Merge/join operations found',
                'suggestion': 'Verify that merge/join operations preserve all citation columns and '
                             'do not create duplicate or misaligned citations. Consider using validate="one_to_one".',
                'pattern': r'\.merge\(|\.join\(',
                'priority': 'high'
            })
        
        if 'drop_duplicates' in script_content:
            suggestions.append({
                'issue': 'drop_duplicates found',
                'suggestion': 'Ensure drop_duplicates() considers all citation columns as part of the '
                             'subset to avoid removing legitimate component-citation combinations.',
                'pattern': r'drop_duplicates\s*\(',
                'priority': 'medium'
            })
        
        return suggestions
    
    def fix_script(self, script_content: str, suggestions: List[Dict]) -> str:
        """Attempt to fix the script based on suggestions."""
        print("\nAttempting to fix script...")
        print("-" * 70)
        
        fixed_content = script_content
        fixes_applied = []
        
        # Look for merge operations without validation
        merge_pattern = r'\.merge\s*\([^)]*\)'
        for match in re.finditer(merge_pattern, fixed_content):
            merge_call = match.group(0)
            if 'validate=' not in merge_call:
                # Try to add validation
                new_merge_call = merge_call[:-1] + ", validate='one_to_one')"
                fixed_content = fixed_content.replace(merge_call, new_merge_call, 1)
                fixes_applied.append(f"Added validation to merge: {merge_call[:50]}...")
        
        # Look for drop_duplicates without proper subset
        dup_pattern = r'\.drop_duplicates\s*\([^)]*\)'
        for match in re.finditer(dup_pattern, fixed_content):
            dup_call = match.group(0)
            if 'subset=' not in dup_call and 'keep=' not in dup_call:
                # Add keep='first' to be safe
                new_dup_call = dup_call[:-1] + ", keep='first')"
                fixed_content = fixed_content.replace(dup_call, new_dup_call, 1)
                fixes_applied.append(f"Added keep='first' to drop_duplicates: {dup_call[:50]}...")
        
        # Look for potential column assignment issues
        # Add comments highlighting potential issues
        for suggestion in suggestions:
            if suggestion['priority'] in ['high', 'critical']:
                pattern = suggestion['pattern']
                for match in re.finditer(pattern, fixed_content):
                    line_start = fixed_content.rfind('\n', 0, match.start()) + 1
                    comment = f"# WARNING: {suggestion['issue']} - {suggestion['suggestion']}\n"
                    if comment not in fixed_content[line_start:match.start()]:
                        fixed_content = fixed_content[:line_start] + comment + fixed_content[line_start:]
                        fixes_applied.append(f"Added warning comment for: {suggestion['issue']}")
        
        if fixes_applied:
            print("Fixes applied:")
            for fix in fixes_applied:
                print(f"  - {fix}")
        else:
            print("No automatic fixes could be applied. Manual intervention may be required.")
            print("\nSuggestions for manual fixes:")
            for i, suggestion in enumerate(suggestions, 1):
                print(f"\n{i}. {suggestion['issue']} (Priority: {suggestion['priority']})")
                print(f"   {suggestion['suggestion']}")
        
        return fixed_content
    
    def test_script(self) -> bool:
        """Run the script and check if it executes successfully."""
        print("\nTesting script execution...")
        
        try:
            result = subprocess.run(
                [sys.executable, self.script_file],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                print("✓ Script executed successfully")
                return True
            else:
                print(f"✗ Script failed with return code {result.returncode}")
                print(f"Error output:\n{result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print("✗ Script execution timed out")
            return False
        except Exception as e:
            print(f"✗ Error running script: {e}")
            return False
    
    def iterative_fix_loop(self):
        """Iteratively fix and test the script until corruptions are eliminated."""
        print("\n" + "=" * 70)
        print("Starting iterative fix loop")
        print("=" * 70)
        
        for iteration in range(1, self.max_iterations + 1):
            print(f"\n{'='*70}")
            print(f"ITERATION {iteration}")
            print(f"{'='*70}")
            
            # Load and validate data
            input_df, output_df = self.load_data()
            corruptions = self.validate_citations(input_df, output_df)
            profile = self.build_corruption_profile(corruptions)
            
            if not corruptions:
                print(f"\n{'='*70}")
                print("SUCCESS! No corruptions found.")
                print(f"{'='*70}")
                break
            
            # Backup current script
            self.backup_script(iteration)
            
            # Analyze and fix script
            script_content = self.analyze_script()
            suggestions = self.generate_fix_suggestions(profile, script_content)
            fixed_content = self.fix_script(script_content, suggestions)
            
            # Save fixed script
            with open(self.script_file, 'w', encoding='utf-8') as f:
                f.write(fixed_content)
            
            print(f"\nScript updated. Re-running to generate new output...")
            
            # Test the fixed script
            if not self.test_script():
                print("\nScript execution failed. Restoring previous version...")
                backup_path = os.path.join(
                    self.backup_dir,
                    [f for f in os.listdir(self.backup_dir) if f'iter{iteration}' in f][0]
                )
                shutil.copy2(backup_path, self.script_file)
                print("Script restored. Manual intervention required.")
                break
            
            print(f"\nIteration {iteration} complete. Re-validating...")
        
        else:
            print(f"\n{'='*70}")
            print(f"Maximum iterations ({self.max_iterations}) reached.")
            print("Manual intervention may be required to fully resolve all corruptions.")
            print(f"{'='*70}")
    
    def run(self):
        """Main execution flow."""
        self.prompt_for_files()
        
        # Initial validation
        input_df, output_df = self.load_data()
        corruptions = self.validate_citations(input_df, output_df)
        profile = self.build_corruption_profile(corruptions)
        
        if not corruptions:
            print("\n✓ Validation complete! No corruptions found.")
            return
        
        # Ask user if they want to attempt automatic fixes
        print("\n" + "=" * 70)
        response = input("Attempt automatic fixes? (yes/no): ").strip().lower()
        
        if response in ['yes', 'y']:
            self.iterative_fix_loop()
        else:
            print("\nValidation complete. Review the corruption profile above.")
            
            # Generate suggestions
            script_content = self.analyze_script()
            suggestions = self.generate_fix_suggestions(profile, script_content)
            
            print("\nSuggestions for manual fixes:")
            for i, suggestion in enumerate(suggestions, 1):
                print(f"\n{i}. {suggestion['issue']} (Priority: {suggestion['priority']})")
                print(f"   {suggestion['suggestion']}")


def main():
    """Entry point for the script."""
    validator = CitationValidator()
    validator.run()


if __name__ == "__main__":
    main()
