"""Write the pipeline file directly."""
content = '''# filepath: ivn_production_pipeline.py
"""
IVN Production Crosswalk Pipeline (OPTIMIZED)
==============================================

Specialized pipeline for processing ivntest.xlsx production data.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import logging

try:
    from rapidfuzz import fuzz
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False
    from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

TOBECROSSWALKED_COLS = {
    'source': 'Source',
    'component': 'Component',
    'description': 'Component Description',
    'url': 'Component URL',
    'office': 'Component Office of Primary Interest'
}

COMPONENTS_COLS = {
    'name': 'component_name',
    'description': 'component_description',
    'url': 'component_url',
    'office': 'component_ofc_of_primary_interest',
    'source_id': 'source_id',
    'component_id': 'component_id',
    'fetch_status': 'fetch_status'
}

SOURCES_COLS = {
    'name': 'source_name',
    'agency': 'source_agency',
    'id': 'source_id'
}

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

SCORING_COLS = ['Similarity_Score', 'Confidence_Bucket', 'Match_Direction']


def fast_similarity(text1, text2):
    """Calculate fast text similarity (0.0 to 1.0)."""
    if not text1 or not text2:
        return 0.0
    t1 = str(text1).strip().lower()
    t2 = str(text2).strip().lower()
    if not t1 or not t2:
        return 0.0
    if HAS_RAPIDFUZZ:
        return fuzz.token_sort_ratio(t1, t2) / 100.0
    return SequenceMatcher(None, t1, t2).ratio()


def combined_sim(name1, name2, desc1, desc2):
    """Combined similarity: 40% name, 60% description."""
    name_sim = fast_similarity(name1, name2)
    desc_sim = fast_similarity(desc1, desc2)
    if not desc1 or not desc2 or not str(desc1).strip() or not str(desc2).strip():
        return name_sim
    return 0.4 * name_sim + 0.6 * desc_sim


class IVNProductionPipeline:
    """Optimized pipeline for IVN crosswalk optimization."""
    
    def __init__(self, config=None):
        self.config = config if config else self._default_config()
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.to_be_crosswalked_df = pd.DataFrame()
        self.components_df = pd.DataFrame()
        self.sources_df = pd.DataFrame()
        self.dataset_df = pd.DataFrame()
        self.candidates_df = pd.DataFrame()
        self.output_df = pd.DataFrame()
        self._source_lookup = {}
        self._agency_lookup = {}
        logger.info("IVNProductionPipeline initialized")
    
    def _default_config(self):
        return {
            'thresholds': {'min_score': 0.6, 'high_confidence': 0.8, 'medium_confidence': 0.6},
            'rules': {'reject_same_source': True, 'reject_self_match': True, 'check_both_directions': True}
        }
    
    def load_ivntest(self, xlsx_path):
        logger.info("=" * 60)
        logger.info("PHASE 1: Loading ivntest.xlsx")
        logger.info("=" * 60)
        
        path = Path(xlsx_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {xlsx_path}")
        
        self.to_be_crosswalked_df = pd.read_excel(xlsx_path, sheet_name='ToBeCrosswalked')
        logger.info(f"Loaded ToBeCrosswalked: {len(self.to_be_crosswalked_df)} rows")
        
        self.components_df = pd.read_excel(xlsx_path, sheet_name='Components')
        logger.info(f"Loaded Components: {len(self.components_df)} rows")
        
        self.sources_df = pd.read_excel(xlsx_path, sheet_name='Sources')
        logger.info(f"Loaded Sources: {len(self.sources_df)} rows")
        
        try:
            self.dataset_df = pd.read_excel(xlsx_path, sheet_name='Dataset')
            logger.info(f"Loaded Dataset: {len(self.dataset_df)} rows")
        except Exception:
            self.dataset_df = pd.DataFrame()
        
        self._build_lookups()
    
    def _build_lookups(self):
        if self.sources_df.empty:
            return
        id_col = SOURCES_COLS['id']
        name_col = SOURCES_COLS['name']
        agency_col = SOURCES_COLS['agency']
        for _, row in self.sources_df.iterrows():
            src_id = self._safe_get(row, id_col)
            if src_id:
                self._source_lookup[src_id] = self._safe_get(row, name_col)
                self._agency_lookup[src_id] = self._safe_get(row, agency_col)
    
    def _safe_get(self, row, col, default=''):
        try:
            if col in row.index:
                val = row[col]
                if pd.isna(val):
                    return default
                return str(val).strip()
        except Exception:
            pass
        return default
    
    def _is_same_source(self, a, b):
        """Check if two sources are the same or closely related."""
        if not a or not b:
            return False
        a_norm = str(a).strip().lower()
        b_norm = str(b).strip().lower()
        if a_norm == b_norm:
            return True
        suffixes = ['instructions', 'directives', 'regulations', 'policies', 'guidance']
        a_base = a_norm
        b_base = b_norm
        for suffix in suffixes:
            a_base = a_base.replace(suffix, '').strip()
            b_base = b_base.replace(suffix, '').strip()
        if a_base and b_base and a_base == b_base:
            return True
        return False
    
    def _is_same_component(self, name1, name2):
        """Check if two component names are the same (self-match)."""
        if not name1 or not name2:
            return False
        return str(name1).strip().lower() == str(name2).strip().lower()
    
    def generate_and_score_candidates(self):
        logger.info("=" * 60)
        logger.info("PHASE 2: Generating & Scoring")
        logger.info("=" * 60)
        
        min_score = self.config.get('thresholds', {}).get('min_score', 0.3)
        high_thresh = self.config.get('thresholds', {}).get('high_confidence', 0.8)
        med_thresh = self.config.get('thresholds', {}).get('medium_confidence', 0.6)
        reject_same = self.config.get('rules', {}).get('reject_same_source', True)
        reject_self = self.config.get('rules', {}).get('reject_self_match', True)
        
        candidates = []
        rejected_same = 0
        rejected_self = 0
        rejected_low = 0
        
        n_tbc = len(self.to_be_crosswalked_df)
        n_comp = len(self.components_df)
        logger.info(f"Comparing {n_tbc} ToBeCrosswalked vs {n_comp} Components")
        logger.info(f"Min score threshold: {min_score}")
        
        for idx, tbc in self.to_be_crosswalked_df.iterrows():
            if idx % 100 == 0:
                logger.info(f"Processing ToBeCrosswalked row {idx}/{n_tbc}")
            
            tbc_src = self._safe_get(tbc, TOBECROSSWALKED_COLS['source'])
            tbc_name = self._safe_get(tbc, TOBECROSSWALKED_COLS['component'])
            tbc_desc = self._safe_get(tbc, TOBECROSSWALKED_COLS['description'])
            tbc_url = self._safe_get(tbc, TOBECROSSWALKED_COLS['url'])
            tbc_ofc = self._safe_get(tbc, TOBECROSSWALKED_COLS['office'])
            tbc_src_name = self._source_lookup.get(tbc_src, tbc_src)
            tbc_agency = self._agency_lookup.get(tbc_src, '')
            
            for _, comp in self.components_df.iterrows():
                comp_src_id = self._safe_get(comp, COMPONENTS_COLS['source_id'])
                
                if reject_same and self._is_same_source(tbc_src, comp_src_id):
                    rejected_same += 2
                    continue
                
                comp_name = self._safe_get(comp, COMPONENTS_COLS['name'])
                
                if reject_self and self._is_same_component(tbc_name, comp_name):
                    rejected_self += 2
                    continue
                
                comp_desc = self._safe_get(comp, COMPONENTS_COLS['description'])
                comp_url = self._safe_get(comp, COMPONENTS_COLS['url'])
                comp_ofc = self._safe_get(comp, COMPONENTS_COLS['office'])
                comp_fetch = self._safe_get(comp, COMPONENTS_COLS['fetch_status'])
                comp_src_name = self._source_lookup.get(comp_src_id, comp_src_id)
                comp_agency = self._agency_lookup.get(comp_src_id, '')
                
                score = combined_sim(tbc_name, comp_name, tbc_desc, comp_desc)
                
                if score < min_score:
                    rejected_low += 2
                    continue
                
                bucket = 'High' if score >= high_thresh else ('Medium' if score >= med_thresh else 'Low')
                
                # Direction 1: ToBeCrosswalked as Enabler
                candidates.append({
                    'Enabling Source': tbc_src_name, 'Enabling Component': tbc_name,
                    'Enabling Component Description': tbc_desc, 'Dependent Component': comp_name,
                    'Dependent Component Description': comp_desc, 'Dependent Source': comp_src_name,
                    'Enabling Component URL': tbc_url, 'Dependent Component URL': comp_url,
                    'Enabling Source Agency': tbc_agency, 'Dependent Source Agency': comp_agency,
                    'Enabling Component Office of Primary Interest': tbc_ofc,
                    'Dependent Component Office of Primary Interest': comp_ofc,
                    'Matched Enabling Index': '', 'Matched Dependent Index': '',
                    'Enabling Fetch Status': '', 'Dependent Fetch Status': comp_fetch,
                    'Similarity_Score': score, 'Confidence_Bucket': bucket,
                    'Match_Direction': 'ToBeCrosswalked_as_Enabler'
                })
                
                # Direction 2: ToBeCrosswalked as Dependent
                candidates.append({
                    'Enabling Source': comp_src_name, 'Enabling Component': comp_name,
                    'Enabling Component Description': comp_desc, 'Dependent Component': tbc_name,
                    'Dependent Component Description': tbc_desc, 'Dependent Source': tbc_src_name,
                    'Enabling Component URL': comp_url, 'Dependent Component URL': tbc_url,
                    'Enabling Source Agency': comp_agency, 'Dependent Source Agency': tbc_agency,
                    'Enabling Component Office of Primary Interest': comp_ofc,
                    'Dependent Component Office of Primary Interest': tbc_ofc,
                    'Matched Enabling Index': '', 'Matched Dependent Index': '',
                    'Enabling Fetch Status': comp_fetch, 'Dependent Fetch Status': '',
                    'Similarity_Score': score, 'Confidence_Bucket': bucket,
                    'Match_Direction': 'ToBeCrosswalked_as_Dependent'
                })
        
        self.candidates_df = pd.DataFrame(candidates)
        logger.info(f"Generated {len(self.candidates_df):,} pairs above threshold")
        logger.info(f"Rejected (same source): {rejected_same:,}")
        logger.info(f"Rejected (self-match): {rejected_self:,}")
        logger.info(f"Rejected (low score): {rejected_low:,}")
        
        if not self.candidates_df.empty:
            for b in ['High', 'Medium', 'Low']:
                cnt = len(self.candidates_df[self.candidates_df['Confidence_Bucket'] == b])
                logger.info(f"  {b}: {cnt:,}")
        
        return self.candidates_df
    
    def lookup_dataset_indices(self):
        logger.info("=" * 60)
        logger.info("PHASE 3: Looking Up Dataset Indices")
        logger.info("=" * 60)
        
        if self.candidates_df.empty or self.dataset_df.empty:
            return self.candidates_df
        
        en_idx = {}
        dep_idx = {}
        
        for _, row in self.dataset_df.iterrows():
            en_c = self._safe_get(row, 'Enabling Component').lower()
            dep_c = self._safe_get(row, 'Dependent Component').lower()
            if en_c:
                en_idx[en_c] = self._safe_get(row, 'Matched Enabling Index')
            if dep_c:
                dep_idx[dep_c] = self._safe_get(row, 'Matched Dependent Index')
        
        self.candidates_df['Matched Enabling Index'] = self.candidates_df['Enabling Component'].apply(
            lambda x: en_idx.get(str(x).lower(), ''))
        self.candidates_df['Matched Dependent Index'] = self.candidates_df['Dependent Component'].apply(
            lambda x: dep_idx.get(str(x).lower(), ''))
        
        logger.info(f"Completed lookups for {len(self.candidates_df):,} candidates")
        return self.candidates_df
    
    def build_output(self):
        logger.info("=" * 60)
        logger.info("PHASE 4: Building Output")
        logger.info("=" * 60)
        
        if self.candidates_df.empty:
            self.output_df = pd.DataFrame(columns=DATASET_OUTPUT_COLS + SCORING_COLS)
            return self.output_df
        
        cols = DATASET_OUTPUT_COLS + SCORING_COLS
        for c in cols:
            if c not in self.candidates_df.columns:
                self.candidates_df[c] = ''
        
        self.output_df = self.candidates_df[cols].copy()
        self.output_df = self.output_df.sort_values('Similarity_Score', ascending=False).reset_index(drop=True)
        logger.info(f"Built output: {len(self.output_df):,} rows")
        return self.output_df
    
    def save_output(self, output_path):
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.output_df.to_csv(path, index=False)
        logger.info(f"Saved to: {path}")
        return str(path)
    
    def run(self, xlsx_path, output_path=None):
        logger.info("=" * 60)
        logger.info("IVN PRODUCTION CROSSWALK PIPELINE")
        logger.info("=" * 60)
        logger.info(f"Started: {datetime.now()}")
        
        self.load_ivntest(xlsx_path)
        self.generate_and_score_candidates()
        self.lookup_dataset_indices()
        self.build_output()
        
        if output_path:
            self.save_output(output_path)
        
        logger.info(f"Completed: {datetime.now()}")
        return self.output_df


def main():
    import argparse
    parser = argparse.ArgumentParser(description="IVN Production Crosswalk Pipeline")
    parser.add_argument('--input', '-i', default='ivntest.xlsx')
    parser.add_argument('--output', '-o', default=None)
    parser.add_argument('--min-score', type=float, default=0.6)
    parser.add_argument('--verbose', '-v', action='store_true')
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    
    if args.output is None:
        args.output = f"output/crosswalk_candidates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    config = {
        'thresholds': {'min_score': args.min_score, 'high_confidence': 0.8, 'medium_confidence': 0.6},
        'rules': {'reject_same_source': True, 'reject_self_match': True}
    }
    
    pipeline = IVNProductionPipeline(config=config)
    result = pipeline.run(xlsx_path=args.input, output_path=args.output)
    print(f"\\nOutput: {len(result):,} pairs saved to {args.output}")


if __name__ == '__main__':
    main()
'''

with open('ivn_production_pipeline.py', 'w', encoding='utf-8') as f:
    f.write(content)

import os
print(f"Wrote {os.path.getsize('ivn_production_pipeline.py')} bytes")
