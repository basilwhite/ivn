"""
IVN Enhanced Crosswalk - Output Builder Module
===============================================

Generates all output tables with complete data provenance:
- Enhanced Components Table
- Normalized Alignments Table
- Denormalized Dataset Table
- Exception Reports

Ensures all source metadata is preserved in outputs.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
from pathlib import Path
import json
import logging

from scoring_module import AlignmentBucket

logger = logging.getLogger(__name__)


class OutputBuilder:
    """
    Builds all output tables from crosswalk pipeline results.
    """
    
    def __init__(self, output_dir: str, timestamp_files: bool = True):
        """
        Initialize the output builder.
        
        Args:
            output_dir: Directory for output files
            timestamp_files: Whether to add timestamps to filenames
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.timestamp_files = timestamp_files
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        logger.info(f"OutputBuilder initialized. Output directory: {self.output_dir}")
    
    def _get_filename(self, base_name: str, extension: str = "csv") -> Path:
        """Generate filename with optional timestamp."""
        if self.timestamp_files:
            filename = f"{base_name}_{self.timestamp}.{extension}"
        else:
            filename = f"{base_name}.{extension}"
        return self.output_dir / filename
    
    def build_enhanced_components_table(
        self,
        components_df: pd.DataFrame,
        master_validator,
        id_column: str,
        source_column: str = None
    ) -> pd.DataFrame:
        """
        Build enhanced components table with validation columns.
        
        Adds:
        - Valid_Source_ID (from master table)
        - Validation_Status
        - Validation_Notes
        
        Args:
            components_df: Original components DataFrame
            master_validator: MasterTableValidator instance
            id_column: Component ID column name
            source_column: Source column name (optional)
            
        Returns:
            Enhanced DataFrame
        """
        result = components_df.copy()
        
        valid_sources = []
        validation_statuses = []
        validation_notes = []
        in_master = []
        
        for _, row in result.iterrows():
            comp_id = str(row[id_column]).strip()
            
            # Check if in master table
            is_valid = master_validator.is_valid_component(comp_id)
            in_master.append(is_valid)
            
            # Get valid source
            valid_source = master_validator.get_valid_source(comp_id)
            valid_sources.append(valid_source or "")
            
            # Validate source if available
            if source_column and source_column in row:
                ext_source = str(row[source_column]).strip()
                if ext_source and valid_source:
                    if ext_source.lower() == valid_source.lower():
                        validation_statuses.append("VALID")
                        validation_notes.append("Source matches master table")
                    else:
                        validation_statuses.append("MISMATCH")
                        validation_notes.append(f"Expected: {valid_source}, Got: {ext_source}")
                elif not valid_source:
                    validation_statuses.append("NO_MASTER")
                    validation_notes.append("Component not in master table")
                else:
                    validation_statuses.append("NO_SOURCE")
                    validation_notes.append("No extracted source to validate")
            else:
                validation_statuses.append("NOT_CHECKED")
                validation_notes.append("Source column not available")
        
        result['Valid_Source_ID'] = valid_sources
        result['In_Master_Table'] = in_master
        result['Validation_Status'] = validation_statuses
        result['Validation_Notes'] = validation_notes
        
        return result
    
    def build_alignments_table(
        self,
        scored_pairs_df: pd.DataFrame,
        component_a_id_col: str,
        component_b_id_col: str,
        source_a_col: str,
        source_b_col: str,
        include_reasoning: bool = True
    ) -> pd.DataFrame:
        """
        Build normalized alignments table.
        
        Contains:
        - Component IDs (A and B)
        - Source IDs (document sources)
        - All similarity scores
        - Validation status
        - Optional reasoning
        
        Args:
            scored_pairs_df: DataFrame with scored pairs
            component_a_id_col, component_b_id_col: ID column names
            source_a_col, source_b_col: Source column names
            include_reasoning: Whether to include reasoning column
            
        Returns:
            Alignments DataFrame
        """
        # Select and rename columns for clarity
        columns_to_keep = [
            component_a_id_col, component_b_id_col,
            source_a_col, source_b_col
        ]
        
        # Add score columns if they exist
        score_columns = ['Text_Similarity', 'Embedding_Similarity', 'Enabling_Score',
                        'Master_Confidence', 'Final_Score', 'Alignment_Bucket']
        for col in score_columns:
            if col in scored_pairs_df.columns:
                columns_to_keep.append(col)
        
        # Add validation columns
        validation_columns = ['Master_Validated', 'Validation_Status', 'Semantic_Valid']
        for col in validation_columns:
            if col in scored_pairs_df.columns:
                columns_to_keep.append(col)
        
        # Add reasoning if requested
        if include_reasoning:
            reasoning_columns = ['Reasoning', 'LLM_Reasoning', 'Heuristic_Reasoning', 'Validation_Notes']
            for col in reasoning_columns:
                if col in scored_pairs_df.columns:
                    columns_to_keep.append(col)
        
        # Filter to existing columns
        existing_columns = [c for c in columns_to_keep if c in scored_pairs_df.columns]
        result = scored_pairs_df[existing_columns].copy()
        
        # Rename to standard names
        rename_map = {
            component_a_id_col: 'Component_A_ID',
            component_b_id_col: 'Component_B_ID',
            source_a_col: 'Source_A_ID',
            source_b_col: 'Source_B_ID'
        }
        result = result.rename(columns=rename_map)
        
        # Add master table validated flag if not present
        if 'Master_Validated' not in result.columns:
            if 'Validation_Status' in result.columns:
                result['Master_Validated'] = result['Validation_Status'].apply(
                    lambda x: str(x).upper() == 'VALID'
                )
            else:
                result['Master_Validated'] = True
        
        # Sort by final score descending
        if 'Final_Score' in result.columns:
            result = result.sort_values('Final_Score', ascending=False)
        
        return result
    
    def build_dataset_table(
        self,
        alignments_df: pd.DataFrame,
        components_df: pd.DataFrame,
        sources_df: pd.DataFrame = None,
        component_id_col: str = 'Component_ID',
        source_id_col: str = 'Source_ID'
    ) -> pd.DataFrame:
        """
        Build denormalized dataset table with complete provenance.
        
        Joins ALL metadata from components and sources for each alignment.
        
        Args:
            alignments_df: Alignments table
            components_df: Components table (with enhanced columns)
            sources_df: Sources table (optional)
            component_id_col: Component ID column in components_df
            source_id_col: Source ID column in sources_df
            
        Returns:
            Denormalized dataset DataFrame
        """
        result = alignments_df.copy()
        
        # Prepare component data for joining
        comp_cols = components_df.columns.tolist()
        
        # Join Component A data
        comp_a = components_df.copy()
        comp_a.columns = [f'A_{col}' if col != component_id_col else 'Component_A_ID' 
                         for col in comp_a.columns]
        
        result = result.merge(
            comp_a,
            on='Component_A_ID',
            how='left',
            suffixes=('', '_comp_a')
        )
        
        # Join Component B data
        comp_b = components_df.copy()
        comp_b.columns = [f'B_{col}' if col != component_id_col else 'Component_B_ID' 
                         for col in comp_b.columns]
        
        result = result.merge(
            comp_b,
            on='Component_B_ID',
            how='left',
            suffixes=('', '_comp_b')
        )
        
        # Join source data if available
        if sources_df is not None and not sources_df.empty:
            # Source A
            src_a = sources_df.copy()
            src_a.columns = [f'Source_A_{col}' if col != source_id_col else 'Source_A_ID'
                           for col in src_a.columns]
            
            if 'Source_A_ID' in result.columns:
                result = result.merge(
                    src_a,
                    on='Source_A_ID',
                    how='left',
                    suffixes=('', '_src_a')
                )
            
            # Source B
            src_b = sources_df.copy()
            src_b.columns = [f'Source_B_{col}' if col != source_id_col else 'Source_B_ID'
                           for col in src_b.columns]
            
            if 'Source_B_ID' in result.columns:
                result = result.merge(
                    src_b,
                    on='Source_B_ID',
                    how='left',
                    suffixes=('', '_src_b')
                )
        
        # Create JSON column with all scores
        score_cols = ['Text_Similarity', 'Embedding_Similarity', 'Enabling_Score',
                     'Master_Confidence', 'Final_Score']
        existing_score_cols = [c for c in score_cols if c in result.columns]
        
        if existing_score_cols:
            result['All_Scores_JSON'] = result[existing_score_cols].apply(
                lambda row: json.dumps({col: round(row[col], 4) if pd.notna(row[col]) else None 
                                       for col in existing_score_cols}),
                axis=1
            )
        
        # Add alignment type based on bucket
        if 'Alignment_Bucket' in result.columns:
            result['Alignment_Type'] = result['Alignment_Bucket']
        else:
            # Infer from final score
            if 'Final_Score' in result.columns:
                result['Alignment_Type'] = result['Final_Score'].apply(
                    lambda x: 'Confirmed' if x >= 0.8 else ('Review' if x >= 0.6 else 'Rejected')
                )
            else:
                result['Alignment_Type'] = 'Unknown'
        
        return result
    
    def build_exception_reports(
        self,
        components_df: pd.DataFrame,
        alignments_df: pd.DataFrame,
        rejected_pairs_df: pd.DataFrame = None,
        high_sim_invalid_df: pd.DataFrame = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Build exception reports for manual review.
        
        Reports:
        1. Mismatched_Components: Source doesn't match master table
        2. Unaligned_Edge_Cases: High similarity but invalid
        3. Same_Source_Rejections: Rejected due to same source
        4. Orphaned_Components: No alignment found
        
        Args:
            components_df: Enhanced components table
            alignments_df: Alignments table
            rejected_pairs_df: DataFrame of rejected pairs (optional)
            high_sim_invalid_df: High similarity but invalid pairs (optional)
            
        Returns:
            Dictionary of exception report DataFrames
        """
        reports = {}
        
        # 1. Mismatched Components
        if 'Validation_Status' in components_df.columns:
            mismatched = components_df[
                components_df['Validation_Status'].isin(['MISMATCH', 'NO_MASTER'])
            ].copy()
            if not mismatched.empty:
                mismatched['Exception_Type'] = 'Source_Mismatch'
                reports['Mismatched_Components'] = mismatched
                logger.info(f"Found {len(mismatched)} mismatched components")
        
        # 2. Same Source Rejections
        if rejected_pairs_df is not None and not rejected_pairs_df.empty:
            same_source = rejected_pairs_df[
                rejected_pairs_df.get('Rejection_Reason', '').str.contains('same source', case=False, na=False)
            ].copy() if 'Rejection_Reason' in rejected_pairs_df.columns else rejected_pairs_df.copy()
            
            if not same_source.empty:
                same_source['Exception_Type'] = 'Same_Source_Rejection'
                reports['Same_Source_Rejections'] = same_source
                logger.info(f"Found {len(same_source)} same-source rejections")
        
        # 3. High Similarity but Invalid
        if high_sim_invalid_df is not None and not high_sim_invalid_df.empty:
            reports['High_Similarity_Invalid'] = high_sim_invalid_df.copy()
            logger.info(f"Found {len(high_sim_invalid_df)} high-similarity invalid pairs")
        elif 'Final_Score' in alignments_df.columns and 'Alignment_Bucket' in alignments_df.columns:
            # Find cases with high score but rejected
            high_sim_rejected = alignments_df[
                (alignments_df['Final_Score'] >= 0.7) & 
                (alignments_df['Alignment_Bucket'].isin(['Rejected', 'Review_Needed']))
            ].copy()
            if not high_sim_rejected.empty:
                high_sim_rejected['Exception_Type'] = 'High_Score_Not_Confirmed'
                reports['High_Similarity_Edge_Cases'] = high_sim_rejected
                logger.info(f"Found {len(high_sim_rejected)} high-similarity edge cases")
        
        # 4. Orphaned Components (no alignment)
        if 'Component_A_ID' in alignments_df.columns and 'Component_B_ID' in alignments_df.columns:
            aligned_ids = set(alignments_df['Component_A_ID'].tolist() + 
                            alignments_df['Component_B_ID'].tolist())
            
            id_col = None
            for col in ['Component_ID', 'ID', 'ComponentID']:
                if col in components_df.columns:
                    id_col = col
                    break
            
            if id_col:
                all_ids = set(components_df[id_col].astype(str).tolist())
                orphan_ids = all_ids - aligned_ids
                
                if orphan_ids:
                    orphans = components_df[
                        components_df[id_col].astype(str).isin(orphan_ids)
                    ].copy()
                    orphans['Exception_Type'] = 'Orphaned_No_Alignment'
                    reports['Orphaned_Components'] = orphans
                    logger.info(f"Found {len(orphans)} orphaned components")
        
        return reports
    
    def generate_summary_statistics(
        self,
        components_df: pd.DataFrame,
        alignments_df: pd.DataFrame,
        exception_reports: Dict[str, pd.DataFrame]
    ) -> Dict[str, Any]:
        """
        Generate summary statistics for the crosswalk results.
        
        Args:
            components_df: Enhanced components table
            alignments_df: Alignments table
            exception_reports: Dictionary of exception reports
            
        Returns:
            Dictionary of summary statistics
        """
        stats = {
            'timestamp': self.timestamp,
            'total_components': len(components_df),
            'total_alignments': len(alignments_df),
        }
        
        # Validation statistics
        if 'Validation_Status' in components_df.columns:
            status_counts = components_df['Validation_Status'].value_counts().to_dict()
            stats['validation_status_counts'] = status_counts
            stats['valid_components'] = status_counts.get('VALID', 0)
            stats['mismatched_components'] = status_counts.get('MISMATCH', 0)
        
        # Alignment bucket distribution
        if 'Alignment_Bucket' in alignments_df.columns:
            bucket_counts = alignments_df['Alignment_Bucket'].value_counts().to_dict()
            stats['alignment_bucket_counts'] = bucket_counts
            stats['confirmed_alignments'] = bucket_counts.get('Confirmed', 0)
            stats['review_needed'] = bucket_counts.get('Review_Needed', 0)
            stats['rejected_alignments'] = bucket_counts.get('Rejected', 0)
        
        # Score statistics
        if 'Final_Score' in alignments_df.columns:
            stats['score_statistics'] = {
                'mean': alignments_df['Final_Score'].mean(),
                'median': alignments_df['Final_Score'].median(),
                'std': alignments_df['Final_Score'].std(),
                'min': alignments_df['Final_Score'].min(),
                'max': alignments_df['Final_Score'].max()
            }
        
        # Exception counts
        stats['exception_counts'] = {
            name: len(df) for name, df in exception_reports.items()
        }
        
        # Calculate effectiveness metrics
        if stats.get('total_components', 0) > 0:
            aligned_ids = set()
            if 'Component_A_ID' in alignments_df.columns:
                aligned_ids.update(alignments_df['Component_A_ID'].tolist())
            if 'Component_B_ID' in alignments_df.columns:
                aligned_ids.update(alignments_df['Component_B_ID'].tolist())
            
            stats['components_with_alignments'] = len(aligned_ids)
            stats['alignment_coverage'] = len(aligned_ids) / stats['total_components']
        
        return stats
    
    def save_all_outputs(
        self,
        components_df: pd.DataFrame,
        alignments_df: pd.DataFrame,
        dataset_df: pd.DataFrame,
        exception_reports: Dict[str, pd.DataFrame],
        summary_stats: Dict[str, Any]
    ) -> Dict[str, Path]:
        """
        Save all output files.
        
        Args:
            components_df: Enhanced components table
            alignments_df: Alignments table
            dataset_df: Denormalized dataset table
            exception_reports: Dictionary of exception reports
            summary_stats: Summary statistics dictionary
            
        Returns:
            Dictionary mapping output type to file path
        """
        output_files = {}
        
        # Save components table
        comp_path = self._get_filename("enhanced_components")
        components_df.to_csv(comp_path, index=False)
        output_files['components'] = comp_path
        logger.info(f"Saved components table: {comp_path}")
        
        # Save alignments table
        align_path = self._get_filename("alignments")
        alignments_df.to_csv(align_path, index=False)
        output_files['alignments'] = align_path
        logger.info(f"Saved alignments table: {align_path}")
        
        # Save dataset table
        dataset_path = self._get_filename("dataset_denormalized")
        dataset_df.to_csv(dataset_path, index=False)
        output_files['dataset'] = dataset_path
        logger.info(f"Saved dataset table: {dataset_path}")
        
        # Save exception reports
        for report_name, report_df in exception_reports.items():
            if not report_df.empty:
                report_path = self._get_filename(f"exception_{report_name.lower()}")
                report_df.to_csv(report_path, index=False)
                output_files[f'exception_{report_name}'] = report_path
                logger.info(f"Saved exception report: {report_path}")
        
        # Save summary statistics as JSON
        stats_path = self._get_filename("summary_statistics", "json")
        with open(stats_path, 'w') as f:
            json.dump(summary_stats, f, indent=2, default=str)
        output_files['summary'] = stats_path
        logger.info(f"Saved summary statistics: {stats_path}")
        
        # Save summary as readable text
        summary_txt_path = self._get_filename("summary_report", "txt")
        self._write_summary_text(summary_stats, summary_txt_path)
        output_files['summary_text'] = summary_txt_path
        
        return output_files
    
    def _write_summary_text(self, stats: Dict[str, Any], path: Path):
        """Write human-readable summary report."""
        with open(path, 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("IVN ENHANCED CROSSWALK - SUMMARY REPORT\n")
            f.write("=" * 60 + "\n\n")
            
            f.write(f"Generated: {stats.get('timestamp', 'Unknown')}\n\n")
            
            f.write("-" * 40 + "\n")
            f.write("OVERVIEW\n")
            f.write("-" * 40 + "\n")
            f.write(f"Total Components: {stats.get('total_components', 0)}\n")
            f.write(f"Total Alignments: {stats.get('total_alignments', 0)}\n")
            f.write(f"Components with Alignments: {stats.get('components_with_alignments', 0)}\n")
            coverage = stats.get('alignment_coverage', 0) * 100
            f.write(f"Alignment Coverage: {coverage:.1f}%\n\n")
            
            if 'alignment_bucket_counts' in stats:
                f.write("-" * 40 + "\n")
                f.write("ALIGNMENT CLASSIFICATION\n")
                f.write("-" * 40 + "\n")
                for bucket, count in stats['alignment_bucket_counts'].items():
                    f.write(f"  {bucket}: {count}\n")
                f.write("\n")
            
            if 'validation_status_counts' in stats:
                f.write("-" * 40 + "\n")
                f.write("VALIDATION STATUS\n")
                f.write("-" * 40 + "\n")
                for status, count in stats['validation_status_counts'].items():
                    f.write(f"  {status}: {count}\n")
                f.write("\n")
            
            if 'score_statistics' in stats:
                f.write("-" * 40 + "\n")
                f.write("SCORE STATISTICS\n")
                f.write("-" * 40 + "\n")
                score_stats = stats['score_statistics']
                f.write(f"  Mean: {score_stats.get('mean', 0):.4f}\n")
                f.write(f"  Median: {score_stats.get('median', 0):.4f}\n")
                f.write(f"  Std Dev: {score_stats.get('std', 0):.4f}\n")
                f.write(f"  Min: {score_stats.get('min', 0):.4f}\n")
                f.write(f"  Max: {score_stats.get('max', 0):.4f}\n\n")
            
            if 'exception_counts' in stats:
                f.write("-" * 40 + "\n")
                f.write("EXCEPTIONS FLAGGED\n")
                f.write("-" * 40 + "\n")
                for exc_type, count in stats['exception_counts'].items():
                    f.write(f"  {exc_type}: {count}\n")
                f.write("\n")
            
            f.write("=" * 60 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 60 + "\n")
        
        logger.info(f"Saved summary text report: {path}")


def build_review_table(
    alignments_df: pd.DataFrame,
    sort_by_score: bool = True
) -> pd.DataFrame:
    """
    Build a table specifically formatted for manual review.
    
    Filters to Review_Needed bucket and sorts by priority.
    
    Args:
        alignments_df: Full alignments table
        sort_by_score: Whether to sort by final score
        
    Returns:
        Review-focused DataFrame
    """
    if 'Alignment_Bucket' in alignments_df.columns:
        review_df = alignments_df[
            alignments_df['Alignment_Bucket'] == 'Review_Needed'
        ].copy()
    else:
        # Fallback to score-based filtering
        if 'Final_Score' in alignments_df.columns:
            review_df = alignments_df[
                (alignments_df['Final_Score'] >= 0.6) & 
                (alignments_df['Final_Score'] < 0.8)
            ].copy()
        else:
            return pd.DataFrame()
    
    if sort_by_score and 'Final_Score' in review_df.columns:
        review_df = review_df.sort_values('Final_Score', ascending=False)
    
    # Add priority indicator
    if 'Final_Score' in review_df.columns:
        review_df['Review_Priority'] = review_df['Final_Score'].apply(
            lambda x: 'HIGH' if x >= 0.75 else ('MEDIUM' if x >= 0.65 else 'LOW')
        )
    
    return review_df
