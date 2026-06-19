"""
IVN Enhanced Crosswalk - Main Pipeline
======================================

Integrated workflow for indexing and crosswalking deliverable components
in IVN governance documents. Combines:
- Master table validation
- Multi-layer candidate generation
- Semantic relationship validation
- Multi-dimensional scoring
- Complete output generation with provenance

Usage:
    python main_pipeline.py --config config.yaml --data-dir ./data
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
import pandas as pd
import numpy as np
import yaml
from tqdm import tqdm

# Local imports
from validation_module import (
    MasterTableValidator,
    SemanticRelationshipValidator,
    SameSourceFilter,
    ValidationStatus,
    ValidationResult,
    validate_components_batch
)
from scoring_module import (
    MultiDimensionalScorer,
    ScoringWeights,
    ScoringThresholds,
    AlignmentBucket,
    create_scorer_from_config
)
from output_builder import (
    OutputBuilder,
    build_review_table
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('crosswalk_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)


class CrosswalkPipeline:
    """
    Main pipeline orchestrator for IVN Enhanced Crosswalk.
    
    Executes the complete workflow:
    1. Load master tables
    2. Load extracted components
    3. Apply master validation
    4. Generate candidates (multi-layer)
    5. Filter same-source pairs
    6. Validate relationships
    7. Apply hard filters
    8. Score and bucket
    9. Build output tables
    10. Generate reports
    11. Save results
    """
    
    def __init__(self, config_path: str = None, config: Dict[str, Any] = None):
        """
        Initialize the pipeline.
        
        Args:
            config_path: Path to YAML configuration file
            config: Configuration dictionary (alternative to config_path)
        """
        if config_path:
            self.config = self._load_config(config_path)
        elif config:
            self.config = config
        else:
            self.config = self._get_default_config()
        
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Initialize components (will be set during execution)
        self.master_df: pd.DataFrame = None
        self.sources_df: pd.DataFrame = None
        self.components_df: pd.DataFrame = None
        
        # Initialize validators and scorers
        self.master_validator: MasterTableValidator = None
        self.relationship_validator: SemanticRelationshipValidator = None
        self.same_source_filter = SameSourceFilter()
        self.scorer: MultiDimensionalScorer = None
        self.output_builder: OutputBuilder = None
        
        # Results storage
        self.candidates_df: pd.DataFrame = None
        self.scored_df: pd.DataFrame = None
        self.alignments_df: pd.DataFrame = None
        self.rejected_df: pd.DataFrame = None
        self.exception_reports: Dict[str, pd.DataFrame] = {}
        
        logger.info("CrosswalkPipeline initialized")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration."""
        return {
            'thresholds': {
                'similarity': 0.7,
                'enabling_relationship': 0.8,
                'final_score': 0.75
            },
            'weights': {
                'text_similarity': 0.2,
                'embedding_similarity': 0.3,
                'enabling_score': 0.4,
                'master_confidence': 0.1
            },
            'rules': {
                'reject_same_source': True,
                'require_master_validation': True,
                'llm_validation': 'heuristic'
            },
            'columns': {
                'master_table': {
                    'component_id': 'Component_ID',
                    'component_name': 'Component_Name',
                    'valid_source_id': 'Valid_Source_ID'
                },
                'sources': {
                    'source_id': 'Source_ID',
                    'source_name': 'Source_Name',
                    'document_type': 'Document_Type',
                    'version': 'Version'
                },
                'components': {
                    'component_id': 'Component_ID',
                    'component_name': 'Component_Name',
                    'description': 'Description',
                    'source': 'Source',
                    'extracted_source_id': 'Extracted_Source_ID'
                }
            },
            'embeddings': {
                'model_name': 'all-MiniLM-L6-v2',
                'cache_embeddings': True
            },
            'output': {
                'timestamp_files': True,
                'include_reasoning': True,
                'sort_by_score': True,
                'generate_summary': True
            }
        }
    
    # =========================================================================
    # PHASE 1: SETUP AND MASTER DATA LOADING
    # =========================================================================
    
    def load_master_tables(
        self,
        master_table_path: str,
        sources_path: str = None
    ) -> None:
        """
        Step 1: Load master component table and sources table.
        
        Args:
            master_table_path: Path to master component table (CSV/Excel)
            sources_path: Path to sources table (optional)
        """
        logger.info("=" * 60)
        logger.info("PHASE 1: Loading Master Tables")
        logger.info("=" * 60)
        
        # Load master table
        master_path = Path(master_table_path)
        if master_path.suffix.lower() in ['.xlsx', '.xls']:
            self.master_df = pd.read_excel(master_path)
        else:
            self.master_df = pd.read_csv(master_path)
        
        logger.info(f"Loaded master table: {len(self.master_df)} rows from {master_path}")
        
        # Initialize master validator
        col_mapping = self.config.get('columns', {}).get('master_table', {})
        self.master_validator = MasterTableValidator(self.master_df, col_mapping)
        
        # Load sources table if provided
        if sources_path:
            sources_path = Path(sources_path)
            if sources_path.exists():
                if sources_path.suffix.lower() in ['.xlsx', '.xls']:
                    self.sources_df = pd.read_excel(sources_path)
                else:
                    self.sources_df = pd.read_csv(sources_path)
                logger.info(f"Loaded sources table: {len(self.sources_df)} rows")
            else:
                logger.warning(f"Sources file not found: {sources_path}")
                self.sources_df = pd.DataFrame()
        else:
            self.sources_df = pd.DataFrame()
    
    def load_extracted_components(self, components_path: str) -> None:
        """
        Step 2: Load components extracted from governance documents.
        
        Args:
            components_path: Path to components CSV/Excel
        """
        logger.info("Loading extracted components...")
        
        comp_path = Path(components_path)
        if comp_path.suffix.lower() in ['.xlsx', '.xls']:
            self.components_df = pd.read_excel(comp_path)
        else:
            self.components_df = pd.read_csv(comp_path)
        
        logger.info(f"Loaded {len(self.components_df)} components from {comp_path}")
        
        # Log column info
        logger.info(f"Component columns: {list(self.components_df.columns)}")
    
    def apply_master_validation(self) -> pd.DataFrame:
        """
        Step 3: Validate all components against master table.
        
        Returns:
            Enhanced components DataFrame with validation columns
        """
        logger.info("=" * 60)
        logger.info("PHASE 1b: Applying Master Table Validation")
        logger.info("=" * 60)
        
        col_mapping = self.config.get('columns', {}).get('components', {})
        id_col = col_mapping.get('component_id', 'Component_ID')
        source_col = col_mapping.get('extracted_source_id', 'Extracted_Source_ID')
        
        # Validate each component
        self.components_df = validate_components_batch(
            self.components_df,
            self.master_validator,
            id_col,
            source_col
        )
        
        # Log validation results
        if 'Validation_Status' in self.components_df.columns:
            status_counts = self.components_df['Validation_Status'].value_counts()
            logger.info("Validation Results:")
            for status, count in status_counts.items():
                logger.info(f"  {status}: {count}")
        
        return self.components_df
    
    # =========================================================================
    # PHASE 2: ENHANCED CROSSWALK PIPELINE
    # =========================================================================
    
    def generate_candidates(self) -> pd.DataFrame:
        """
        Step 4: Generate candidate pairs using multi-layer approach.
        
        Layers:
        1. All possible pairs (Cartesian product, filtered)
        2. Fuzzy matching pre-filter
        3. Embedding pre-filter (optional)
        
        Returns:
            DataFrame of candidate pairs
        """
        logger.info("=" * 60)
        logger.info("PHASE 2: Generating Candidate Pairs")
        logger.info("=" * 60)
        
        col_mapping = self.config.get('columns', {}).get('components', {})
        id_col = col_mapping.get('component_id', 'Component_ID')
        name_col = col_mapping.get('component_name', 'Component_Name')
        desc_col = col_mapping.get('description', 'Description')
        source_col = col_mapping.get('source', 'Source')
        
        # Filter to valid components only (in master table)
        if self.config.get('rules', {}).get('require_master_validation', True):
            valid_components = self.master_validator.filter_valid_components(
                self.components_df, id_col
            )
            logger.info(f"Filtered to {len(valid_components)} valid components")
        else:
            valid_components = self.components_df.copy()
        
        # Generate all possible pairs
        logger.info("Generating candidate pairs...")
        candidates = []
        
        component_list = valid_components.to_dict('records')
        n_components = len(component_list)
        
        for i in tqdm(range(n_components), desc="Generating pairs"):
            comp_a = component_list[i]
            for j in range(i + 1, n_components):
                comp_b = component_list[j]
                
                # Create pair record
                pair = {
                    'Component_A_ID': comp_a.get(id_col, ''),
                    'Component_A_Name': comp_a.get(name_col, ''),
                    'Component_A_Description': comp_a.get(desc_col, ''),
                    'Source_A': comp_a.get(source_col, ''),
                    'Component_B_ID': comp_b.get(id_col, ''),
                    'Component_B_Name': comp_b.get(name_col, ''),
                    'Component_B_Description': comp_b.get(desc_col, ''),
                    'Source_B': comp_b.get(source_col, ''),
                }
                
                # Copy additional columns with A_/B_ prefix
                for col in valid_components.columns:
                    if col not in [id_col, name_col, desc_col, source_col]:
                        pair[f'A_{col}'] = comp_a.get(col, '')
                        pair[f'B_{col}'] = comp_b.get(col, '')
                
                candidates.append(pair)
        
        self.candidates_df = pd.DataFrame(candidates)
        logger.info(f"Generated {len(self.candidates_df)} candidate pairs")
        
        return self.candidates_df
    
    def filter_same_source(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Step 5: Remove pairs from the same source document.
        
        Returns:
            Tuple of (filtered_pairs, rejected_pairs)
        """
        logger.info("=" * 60)
        logger.info("PHASE 2b: Filtering Same-Source Pairs")
        logger.info("=" * 60)
        
        if not self.config.get('rules', {}).get('reject_same_source', True):
            logger.info("Same-source filtering disabled")
            return self.candidates_df, pd.DataFrame()
        
        filtered, rejected = self.same_source_filter.filter_pairs(
            self.candidates_df,
            'Source_A',
            'Source_B'
        )
        
        self.candidates_df = filtered
        self.rejected_df = rejected
        
        logger.info(f"Kept {len(filtered)} pairs, rejected {len(rejected)} same-source pairs")
        
        return filtered, rejected
    
    def validate_relationships(self) -> pd.DataFrame:
        """
        Step 6: Validate semantic enabling relationships.
        
        Returns:
            DataFrame with enabling scores and validation results
        """
        logger.info("=" * 60)
        logger.info("PHASE 2c: Validating Semantic Relationships")
        logger.info("=" * 60)
        
        # Initialize relationship validator
        threshold_config = self.config.get('thresholds', {})
        self.relationship_validator = SemanticRelationshipValidator(threshold_config)
        
        use_llm = self.config.get('rules', {}).get('llm_validation') == 'llm'
        
        enabling_scores = []
        enabling_statuses = []
        enabling_reasons = []
        
        for idx, row in tqdm(self.candidates_df.iterrows(), 
                            total=len(self.candidates_df),
                            desc="Validating relationships"):
            
            comp_a = {
                'id': row['Component_A_ID'],
                'name': row['Component_A_Name'],
                'description': row['Component_A_Description'],
                'source': row['Source_A']
            }
            comp_b = {
                'id': row['Component_B_ID'],
                'name': row['Component_B_Name'],
                'description': row['Component_B_Description'],
                'source': row['Source_B']
            }
            
            result = self.relationship_validator.validate_relationship(
                comp_a, comp_b, use_llm=use_llm
            )
            
            enabling_scores.append(result.confidence / 100.0)
            enabling_statuses.append(result.status.value)
            enabling_reasons.append(result.reasoning)
        
        self.candidates_df['Enabling_Score'] = enabling_scores
        self.candidates_df['Enabling_Status'] = enabling_statuses
        self.candidates_df['Enabling_Reasoning'] = enabling_reasons
        
        # Log summary
        valid_count = sum(1 for s in enabling_statuses if s == 'VALID')
        logger.info(f"Valid enabling relationships: {valid_count}/{len(enabling_statuses)}")
        
        return self.candidates_df
    
    def apply_hard_filters(self) -> pd.DataFrame:
        """
        Step 7: Apply master table and business rule hard filters.
        
        Returns:
            DataFrame with validation columns
        """
        logger.info("=" * 60)
        logger.info("PHASE 2d: Applying Hard Filters")
        logger.info("=" * 60)
        
        master_validated = []
        validation_notes = []
        
        for idx, row in self.candidates_df.iterrows():
            result = self.master_validator.validate_alignment_pair(
                row['Component_A_ID'],
                row['Component_B_ID'],
                row['Source_A'],
                row['Source_B']
            )
            
            master_validated.append(result.status == ValidationStatus.VALID)
            validation_notes.append(result.reasoning)
        
        self.candidates_df['Master_Validated'] = master_validated
        self.candidates_df['Master_Validation_Notes'] = validation_notes
        self.candidates_df['Master_Confidence'] = self.candidates_df['Master_Validated'].astype(float)
        
        validated_count = sum(master_validated)
        logger.info(f"Master table validated: {validated_count}/{len(master_validated)}")
        
        return self.candidates_df
    
    def score_and_bucket(self) -> pd.DataFrame:
        """
        Step 8: Calculate multi-dimensional scores and bucket results.
        
        Returns:
            DataFrame with final scores and bucket classifications
        """
        logger.info("=" * 60)
        logger.info("PHASE 2e: Scoring and Bucketing")
        logger.info("=" * 60)
        
        # Initialize scorer from config
        self.scorer = create_scorer_from_config(self.config)
        
        # Score all candidates
        self.scored_df = self.scorer.score_candidates_batch(
            self.candidates_df,
            name_a_col='Component_A_Name',
            name_b_col='Component_B_Name',
            desc_a_col='Component_A_Description',
            desc_b_col='Component_B_Description',
            enabling_col='Enabling_Score',
            master_conf_col='Master_Confidence',
            validation_col='Master_Validated'
        )
        
        # Log bucket distribution
        if 'Alignment_Bucket' in self.scored_df.columns:
            bucket_counts = self.scored_df['Alignment_Bucket'].value_counts()
            logger.info("Bucket Distribution:")
            for bucket, count in bucket_counts.items():
                logger.info(f"  {bucket}: {count}")
        
        return self.scored_df
    
    # =========================================================================
    # PHASE 3: OUTPUT GENERATION
    # =========================================================================
    
    def build_output_tables(self, output_dir: str) -> Dict[str, pd.DataFrame]:
        """
        Step 9: Build all output tables with complete provenance.
        
        Args:
            output_dir: Directory for output files
            
        Returns:
            Dictionary of output DataFrames
        """
        logger.info("=" * 60)
        logger.info("PHASE 3: Building Output Tables")
        logger.info("=" * 60)
        
        output_config = self.config.get('output', {})
        self.output_builder = OutputBuilder(
            output_dir,
            timestamp_files=output_config.get('timestamp_files', True)
        )
        
        col_mapping = self.config.get('columns', {}).get('components', {})
        id_col = col_mapping.get('component_id', 'Component_ID')
        source_col = col_mapping.get('source', 'Source')
        
        # Build enhanced components table
        enhanced_components = self.output_builder.build_enhanced_components_table(
            self.components_df,
            self.master_validator,
            id_col,
            source_col
        )
        logger.info(f"Built enhanced components table: {len(enhanced_components)} rows")
        
        # Build alignments table
        self.alignments_df = self.output_builder.build_alignments_table(
            self.scored_df,
            'Component_A_ID',
            'Component_B_ID',
            'Source_A',
            'Source_B',
            include_reasoning=output_config.get('include_reasoning', True)
        )
        logger.info(f"Built alignments table: {len(self.alignments_df)} rows")
        
        # Build denormalized dataset table
        sources_col = self.config.get('columns', {}).get('sources', {}).get('source_id', 'Source_ID')
        dataset_df = self.output_builder.build_dataset_table(
            self.alignments_df,
            enhanced_components,
            self.sources_df if not self.sources_df.empty else None,
            component_id_col=id_col,
            source_id_col=sources_col
        )
        logger.info(f"Built dataset table: {len(dataset_df)} rows")
        
        return {
            'components': enhanced_components,
            'alignments': self.alignments_df,
            'dataset': dataset_df
        }
    
    def generate_reports(self) -> Dict[str, pd.DataFrame]:
        """
        Step 10: Generate exception and review reports.
        
        Returns:
            Dictionary of exception report DataFrames
        """
        logger.info("=" * 60)
        logger.info("PHASE 3b: Generating Reports")
        logger.info("=" * 60)
        
        # Build exception reports
        self.exception_reports = self.output_builder.build_exception_reports(
            self.components_df,
            self.alignments_df,
            self.rejected_df,
            None  # High similarity invalid - will be detected from alignments
        )
        
        # Add review table
        review_df = build_review_table(
            self.alignments_df,
            sort_by_score=self.config.get('output', {}).get('sort_by_score', True)
        )
        if not review_df.empty:
            self.exception_reports['Review_Queue'] = review_df
            logger.info(f"Built review queue: {len(review_df)} items")
        
        return self.exception_reports
    
    def save_results(self, output_dir: str) -> Dict[str, Path]:
        """
        Step 11: Save all results to files.
        
        Args:
            output_dir: Directory for output files
            
        Returns:
            Dictionary mapping output type to file path
        """
        logger.info("=" * 60)
        logger.info("PHASE 4: Saving Results")
        logger.info("=" * 60)
        
        # Build outputs if not already done
        if self.output_builder is None:
            self.build_output_tables(output_dir)
        
        col_mapping = self.config.get('columns', {}).get('components', {})
        id_col = col_mapping.get('component_id', 'Component_ID')
        source_col = col_mapping.get('source', 'Source')
        
        # Build final tables
        enhanced_components = self.output_builder.build_enhanced_components_table(
            self.components_df,
            self.master_validator,
            id_col,
            source_col
        )
        
        sources_col = self.config.get('columns', {}).get('sources', {}).get('source_id', 'Source_ID')
        dataset_df = self.output_builder.build_dataset_table(
            self.alignments_df,
            enhanced_components,
            self.sources_df if not self.sources_df.empty else None,
            component_id_col=id_col,
            source_id_col=sources_col
        )
        
        # Generate summary statistics
        summary_stats = self.output_builder.generate_summary_statistics(
            enhanced_components,
            self.alignments_df,
            self.exception_reports
        )
        
        # Save all outputs
        output_files = self.output_builder.save_all_outputs(
            enhanced_components,
            self.alignments_df,
            dataset_df,
            self.exception_reports,
            summary_stats
        )
        
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Output files saved to: {output_dir}")
        for output_type, path in output_files.items():
            logger.info(f"  {output_type}: {path}")
        
        return output_files
    
    # =========================================================================
    # MAIN EXECUTION
    # =========================================================================
    
    def run(
        self,
        master_table_path: str,
        components_path: str,
        output_dir: str,
        sources_path: str = None
    ) -> Dict[str, Path]:
        """
        Execute the complete pipeline.
        
        Args:
            master_table_path: Path to master component table
            components_path: Path to extracted components
            output_dir: Directory for output files
            sources_path: Path to sources table (optional)
            
        Returns:
            Dictionary of output file paths
        """
        logger.info("=" * 60)
        logger.info("IVN ENHANCED CROSSWALK PIPELINE")
        logger.info("=" * 60)
        logger.info(f"Started at: {datetime.now()}")
        
        try:
            # Phase 1: Setup
            self.load_master_tables(master_table_path, sources_path)
            self.load_extracted_components(components_path)
            self.apply_master_validation()
            
            # Phase 2: Crosswalk
            self.generate_candidates()
            self.filter_same_source()
            self.validate_relationships()
            self.apply_hard_filters()
            self.score_and_bucket()
            
            # Phase 3: Output
            self.build_output_tables(output_dir)
            self.generate_reports()
            output_files = self.save_results(output_dir)
            
            logger.info(f"Completed at: {datetime.now()}")
            
            return output_files
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="IVN Enhanced Crosswalk Pipeline"
    )
    parser.add_argument(
        '--config', '-c',
        default='config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--master', '-m',
        required=True,
        help='Path to master component table'
    )
    parser.add_argument(
        '--components', '-p',
        required=True,
        help='Path to extracted components file'
    )
    parser.add_argument(
        '--sources', '-s',
        default=None,
        help='Path to sources table (optional)'
    )
    parser.add_argument(
        '--output', '-o',
        default='output',
        help='Output directory'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Run pipeline
    pipeline = CrosswalkPipeline(config_path=args.config)
    pipeline.run(
        master_table_path=args.master,
        components_path=args.components,
        output_dir=args.output,
        sources_path=args.sources
    )


if __name__ == '__main__':
    main()
