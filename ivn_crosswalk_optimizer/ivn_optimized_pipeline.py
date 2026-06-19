"""
IVN Optimized Crosswalk Pipeline
================================

Optimized version using vectorized pandas operations for faster execution.
Uses cross-join and batch processing instead of nested loops.

Input: ivntest.xlsx with tabs:
    - ToBeCrosswalked: Components to find matches for
    - Components: Master component list to match against
    - Sources: Source metadata
    - Dataset: Existing alignments for index lookups

Output: Dataset-format CSV with candidate alignment pairs
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging
from tqdm import tqdm

try:
    from rapidfuzz import fuzz
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


# Dataset tab output columns (in exact order)
DATASET_OUTPUT_COLS = [
    'Enabling Source',
    'Enabling Component',
    'Enabling Component Description',
    'Dependent Component',
    'Dependent Component Description',
    'Dependent Source',
    'Enabling Component URL',
    'Dependent Component URL',
    'Enabling Source Agency',
    'Dependent Source Agency',
    'Enabling Component Office of Primary Interest',
    'Dependent Component Office of Primary Interest',
    'Matched Enabling Index',
    'Matched Dependent Index',
    'Enabling Fetch Status',
    'Dependent Fetch Status'
]


class IVNOptimizedPipeline:
    """
    Optimized production pipeline using vectorized operations.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or self._get_default_config()
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # DataFrames
        self.to_be_crosswalked_df: pd.DataFrame = None
        self.components_df: pd.DataFrame = None
        self.sources_df: pd.DataFrame = None
        self.dataset_df: pd.DataFrame = None
        self.output_df: pd.DataFrame = None
        
        logger.info("IVNOptimizedPipeline initialized")
    
    def _get_default_config(self) -> Dict[str, Any]:
        return {
            'thresholds': {
                'high_confidence': 0.8,
                'medium_confidence': 0.6,
                'min_score': 0.3  # Minimum score to keep (filter low matches)
            },
            'rules': {
                'reject_same_source': True
            }
        }
    
    def load_data(self, xlsx_path: str) -> None:
        """Load all tabs from ivntest.xlsx."""
        logger.info("Loading ivntest.xlsx...")
        xlsx_path = Path(xlsx_path)
        
        self.to_be_crosswalked_df = pd.read_excel(xlsx_path, sheet_name='ToBeCrosswalked')
        logger.info(f"  ToBeCrosswalked: {len(self.to_be_crosswalked_df)} rows")
        
        self.components_df = pd.read_excel(xlsx_path, sheet_name='Components')
        logger.info(f"  Components: {len(self.components_df)} rows")
        
        self.sources_df = pd.read_excel(xlsx_path, sheet_name='Sources')
        logger.info(f"  Sources: {len(self.sources_df)} rows")
        
        try:
            self.dataset_df = pd.read_excel(xlsx_path, sheet_name='Dataset')
            logger.info(f"  Dataset: {len(self.dataset_df)} rows")
        except:
            self.dataset_df = pd.DataFrame()
            logger.warning("  Dataset tab not found or empty")
    
    def _build_lookups(self) -> tuple:
        """Build source name and agency lookup dictionaries."""
        source_name_lookup = {}
        source_agency_lookup = {}
        
        if self.sources_df is not None and not self.sources_df.empty:
            for _, row in self.sources_df.iterrows():
                src_id = str(row.get('source_id', '')).strip()
                if src_id:
                    source_name_lookup[src_id] = str(row.get('source_name', src_id)).strip()
                    source_agency_lookup[src_id] = str(row.get('source_agency', '')).strip()
        
        return source_name_lookup, source_agency_lookup
    
    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate text similarity score."""
        if not text1 or not text2:
            return 0.0
        
        t1 = str(text1).lower().strip()
        t2 = str(text2).lower().strip()
        
        if not t1 or not t2:
            return 0.0
        
        if RAPIDFUZZ_AVAILABLE:
            return fuzz.token_set_ratio(t1, t2) / 100.0
        else:
            return SequenceMatcher(None, t1, t2).ratio()
    
    def generate_and_score(self) -> pd.DataFrame:
        """
        Generate candidate pairs and score them using optimized batch processing.
        """
        logger.info("=" * 60)
        logger.info("Generating and Scoring Candidate Pairs")
        logger.info("=" * 60)
        
        source_name_lookup, source_agency_lookup = self._build_lookups()
        
        # Prepare ToBeCrosswalked data
        tbc_data = []
        for idx, row in self.to_be_crosswalked_df.iterrows():
            src = str(row.get('Source', '')).strip()
            tbc_data.append({
                'tbc_idx': idx,
                'tbc_source_id': src,
                'tbc_source_name': source_name_lookup.get(src, src),
                'tbc_source_agency': source_agency_lookup.get(src, ''),
                'tbc_component': str(row.get('Component', '')).strip(),
                'tbc_description': str(row.get('Component Description', '')).strip(),
                'tbc_url': str(row.get('Component URL', '')).strip(),
                'tbc_office': str(row.get('Component Office of Primary Interest', '')).strip()
            })
        tbc_df = pd.DataFrame(tbc_data)
        
        # Prepare Components data
        comp_data = []
        for idx, row in self.components_df.iterrows():
            src = str(row.get('source_id', '')).strip()
            comp_data.append({
                'comp_idx': idx,
                'comp_source_id': src,
                'comp_source_name': source_name_lookup.get(src, src),
                'comp_source_agency': source_agency_lookup.get(src, ''),
                'comp_name': str(row.get('component_name', '')).strip(),
                'comp_description': str(row.get('component_description', '')).strip(),
                'comp_url': str(row.get('component_url', '')).strip(),
                'comp_office': str(row.get('component_ofc_of_primary_interest', '')).strip(),
                'comp_fetch_status': str(row.get('fetch_status', '')).strip()
            })
        comp_df = pd.DataFrame(comp_data)
        
        logger.info(f"Processing {len(tbc_df)} ToBeCrosswalked x {len(comp_df)} Components")
        
        # Cross join using merge with dummy key
        tbc_df['_key'] = 1
        comp_df['_key'] = 1
        
        logger.info("Creating cross-join (this may take a moment)...")
        candidates = tbc_df.merge(comp_df, on='_key').drop('_key', axis=1)
        logger.info(f"Generated {len(candidates)} candidate pairs before filtering")
        
        # Filter same-source pairs
        if self.config.get('rules', {}).get('reject_same_source', True):
            before_count = len(candidates)
            candidates = candidates[
                candidates['tbc_source_id'].str.lower() != candidates['comp_source_id'].str.lower()
            ]
            logger.info(f"Filtered same-source: {before_count} -> {len(candidates)} pairs")
        
        # Calculate similarity scores in batches
        logger.info("Calculating similarity scores...")
        scores = []
        
        batch_size = 10000
        total_batches = (len(candidates) + batch_size - 1) // batch_size
        
        for i in tqdm(range(0, len(candidates), batch_size), 
                     total=total_batches, desc="Scoring batches"):
            batch = candidates.iloc[i:i+batch_size]
            batch_scores = []
            
            for _, row in batch.iterrows():
                # Combine name and description for comparison
                tbc_text = f"{row['tbc_component']} {row['tbc_description']}"
                comp_text = f"{row['comp_name']} {row['comp_description']}"
                score = self._calculate_similarity(tbc_text, comp_text)
                batch_scores.append(score)
            
            scores.extend(batch_scores)
        
        candidates['Similarity_Score'] = scores
        
        # Filter by minimum score
        min_score = self.config.get('thresholds', {}).get('min_score', 0.3)
        before_count = len(candidates)
        candidates = candidates[candidates['Similarity_Score'] >= min_score]
        logger.info(f"Filtered by min_score ({min_score}): {before_count} -> {len(candidates)} pairs")
        
        # Assign confidence buckets
        high_thresh = self.config.get('thresholds', {}).get('high_confidence', 0.8)
        med_thresh = self.config.get('thresholds', {}).get('medium_confidence', 0.6)
        
        candidates['Confidence_Bucket'] = candidates['Similarity_Score'].apply(
            lambda x: 'High' if x >= high_thresh else ('Medium' if x >= med_thresh else 'Low')
        )
        
        # Build output for both directions
        logger.info("Building output for both directions...")
        output_rows = []
        
        for _, row in tqdm(candidates.iterrows(), total=len(candidates), desc="Building output"):
            # Direction 1: ToBeCrosswalked as Enabler
            output_rows.append({
                'Enabling Source': row['tbc_source_name'],
                'Enabling Component': row['tbc_component'],
                'Enabling Component Description': row['tbc_description'],
                'Dependent Component': row['comp_name'],
                'Dependent Component Description': row['comp_description'],
                'Dependent Source': row['comp_source_name'],
                'Enabling Component URL': row['tbc_url'],
                'Dependent Component URL': row['comp_url'],
                'Enabling Source Agency': row['tbc_source_agency'],
                'Dependent Source Agency': row['comp_source_agency'],
                'Enabling Component Office of Primary Interest': row['tbc_office'],
                'Dependent Component Office of Primary Interest': row['comp_office'],
                'Matched Enabling Index': '',
                'Matched Dependent Index': '',
                'Enabling Fetch Status': '',
                'Dependent Fetch Status': row['comp_fetch_status'],
                'Similarity_Score': row['Similarity_Score'],
                'Confidence_Bucket': row['Confidence_Bucket'],
                'Match_Direction': 'ToBeCrosswalked_as_Enabler'
            })
            
            # Direction 2: ToBeCrosswalked as Dependent
            output_rows.append({
                'Enabling Source': row['comp_source_name'],
                'Enabling Component': row['comp_name'],
                'Enabling Component Description': row['comp_description'],
                'Dependent Component': row['tbc_component'],
                'Dependent Component Description': row['tbc_description'],
                'Dependent Source': row['tbc_source_name'],
                'Enabling Component URL': row['comp_url'],
                'Dependent Component URL': row['tbc_url'],
                'Enabling Source Agency': row['comp_source_agency'],
                'Dependent Source Agency': row['tbc_source_agency'],
                'Enabling Component Office of Primary Interest': row['comp_office'],
                'Dependent Component Office of Primary Interest': row['tbc_office'],
                'Matched Enabling Index': '',
                'Matched Dependent Index': '',
                'Enabling Fetch Status': row['comp_fetch_status'],
                'Dependent Fetch Status': '',
                'Similarity_Score': row['Similarity_Score'],
                'Confidence_Bucket': row['Confidence_Bucket'],
                'Match_Direction': 'ToBeCrosswalked_as_Dependent'
            })
        
        self.output_df = pd.DataFrame(output_rows)
        logger.info(f"Generated {len(self.output_df)} output rows (both directions)")
        
        return self.output_df
    
    def lookup_indices(self) -> pd.DataFrame:
        """Lookup Matched Enabling/Dependent Index from Dataset tab."""
        if self.dataset_df is None or self.dataset_df.empty:
            logger.info("Dataset tab empty, skipping index lookups")
            return self.output_df
        
        logger.info("Looking up indices from Dataset tab...")
        
        # Build lookup from Dataset
        enabling_idx_lookup = {}
        dependent_idx_lookup = {}
        enabling_fetch_lookup = {}
        dependent_fetch_lookup = {}
        
        for _, row in self.dataset_df.iterrows():
            en_comp = str(row.get('Enabling Component', '')).strip().lower()
            dep_comp = str(row.get('Dependent Component', '')).strip().lower()
            
            if en_comp:
                enabling_idx_lookup[en_comp] = str(row.get('Matched Enabling Index', '')).strip()
                enabling_fetch_lookup[en_comp] = str(row.get('Enabling Fetch Status', '')).strip()
            if dep_comp:
                dependent_idx_lookup[dep_comp] = str(row.get('Matched Dependent Index', '')).strip()
                dependent_fetch_lookup[dep_comp] = str(row.get('Dependent Fetch Status', '')).strip()
        
        # Apply lookups
        def get_enabling_idx(comp):
            return enabling_idx_lookup.get(str(comp).lower().strip(), '')
        
        def get_dependent_idx(comp):
            return dependent_idx_lookup.get(str(comp).lower().strip(), '')
        
        def get_enabling_fetch(comp, current):
            if current:
                return current
            return enabling_fetch_lookup.get(str(comp).lower().strip(), '') or \
                   dependent_fetch_lookup.get(str(comp).lower().strip(), '')
        
        def get_dependent_fetch(comp, current):
            if current:
                return current
            return dependent_fetch_lookup.get(str(comp).lower().strip(), '') or \
                   enabling_fetch_lookup.get(str(comp).lower().strip(), '')
        
        self.output_df['Matched Enabling Index'] = self.output_df['Enabling Component'].apply(get_enabling_idx)
        self.output_df['Matched Dependent Index'] = self.output_df['Dependent Component'].apply(get_dependent_idx)
        
        self.output_df['Enabling Fetch Status'] = self.output_df.apply(
            lambda r: get_enabling_fetch(r['Enabling Component'], r['Enabling Fetch Status']), axis=1
        )
        self.output_df['Dependent Fetch Status'] = self.output_df.apply(
            lambda r: get_dependent_fetch(r['Dependent Component'], r['Dependent Fetch Status']), axis=1
        )
        
        logger.info("Index lookups complete")
        return self.output_df
    
    def save_output(self, output_path: str) -> Path:
        """Save output to CSV."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Order columns
        output_cols = DATASET_OUTPUT_COLS + ['Similarity_Score', 'Confidence_Bucket', 'Match_Direction']
        
        # Sort by score descending
        self.output_df = self.output_df.sort_values('Similarity_Score', ascending=False)
        
        # Select columns in order
        self.output_df[output_cols].to_csv(output_path, index=False)
        logger.info(f"Saved to: {output_path}")
        
        return output_path
    
    def run(self, xlsx_path: str, output_path: str = None) -> pd.DataFrame:
        """Execute the complete pipeline."""
        logger.info("=" * 60)
        logger.info("IVN OPTIMIZED CROSSWALK PIPELINE")
        logger.info("=" * 60)
        
        self.load_data(xlsx_path)
        self.generate_and_score()
        self.lookup_indices()
        
        if output_path:
            self.save_output(output_path)
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("RESULTS SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total output rows: {len(self.output_df)}")
        
        if 'Confidence_Bucket' in self.output_df.columns:
            for bucket in ['High', 'Medium', 'Low']:
                count = len(self.output_df[self.output_df['Confidence_Bucket'] == bucket])
                logger.info(f"  {bucket} confidence: {count}")
        
        return self.output_df


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="IVN Optimized Crosswalk Pipeline")
    parser.add_argument('--input', '-i', default='ivntest.xlsx', help='Input xlsx file')
    parser.add_argument('--output', '-o', default=None, help='Output CSV file')
    parser.add_argument('--min-score', type=float, default=0.3, help='Minimum similarity score')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    if args.output is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = f"output/crosswalk_optimized_{timestamp}.csv"
    
    config = {
        'thresholds': {
            'high_confidence': 0.8,
            'medium_confidence': 0.6,
            'min_score': args.min_score
        },
        'rules': {
            'reject_same_source': True
        }
    }
    
    pipeline = IVNOptimizedPipeline(config=config)
    pipeline.run(args.input, args.output)


if __name__ == '__main__':
    main()
