"""
IVN Crosswalk Optimizer - Main Entry Point
==========================================

Main execution script for the IVN Crosswalk Pipeline.
Processes ivntest.xlsx to find enabling relationships between
ToBeCrosswalked components and existing Components.

Usage:
    python ivn_crosswalk_optimizer.py
    python ivn_crosswalk_optimizer.py --input ivntest.xlsx --output results.csv
    python ivn_crosswalk_optimizer.py --min-score 0.6 --verbose
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
import logging

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from ivn_production_pipeline import IVNProductionPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def estimate_threshold_for_max_rows(result_df, max_rows=100000):
    """
    Estimate the minimum similarity score needed to keep output under max_rows.
    Uses the first 1000 rows to estimate the distribution.
    """
    if 'Similarity_Score' not in result_df.columns:
        return None
    scores = result_df['Similarity_Score'].sort_values(ascending=False)
    avg_first_1000 = scores.head(1000).mean()
    # Estimate threshold by finding the score at the max_rows-th position
    if len(scores) > max_rows:
        est_threshold = scores.iloc[max_rows-1]
    else:
        est_threshold = scores.min()
    return avg_first_1000, est_threshold


def recommend_min_score(result_df, desired_matches=1000):
    """
    Compute the first 100 similarity scores, extrapolate what minimum score would generate
    `desired_matches` if that score applied to all possible pairs, and recommend that score.
    """
    if 'Similarity_Score' not in result_df.columns or len(result_df) == 0:
        return None, None
    scores = result_df['Similarity_Score'].sort_values(ascending=False)
    first_100 = scores.head(100)
    avg_score = first_100.mean()
    total_pairs = len(result_df)
    # Extrapolate: what score would yield desired_matches if that score was the cutoff
    if total_pairs <= desired_matches:
        recommended_score = scores.min()
    else:
        # Find the score at the desired_matches-th position
        recommended_score = scores.iloc[desired_matches-1]
    return avg_score, recommended_score


def prompt_for_min_score(default_score):
    """
    Prompt the user to set the minimum score, with the recommended score as the default.
    """
    try:
        user_input = input(f"Enter minimum similarity score to use for filtering [{default_score:.3f}]: ").strip()
        if user_input == '':
            return default_score
        val = float(user_input)
        if 0.0 <= val <= 1.0:
            return val
        else:
            print("Please enter a value between 0.0 and 1.0.")
            return default_score
    except Exception:
        print("Invalid input. Using default.")
        return default_score


def run_crosswalk(
    input_path: str = None,
    output_path: str = None,
    min_score: float = 0.0,
    confidence_filter: str = None,
    verbose: bool = False,
    top_n: int = None,
    interactive: bool = False
):
    """
    Run the IVN Crosswalk Optimizer on production data.
    
    Args:
        input_path: Path to ivntest.xlsx (default: ivntest.xlsx in script directory)
        output_path: Path for output CSV (default: auto-generated in output/)
        min_score: Minimum similarity score to include in output (default: 0.0)
        confidence_filter: Filter by confidence bucket ('High', 'Medium', 'Low')
        verbose: Enable verbose logging
        top_n: Only output the top N rows by Similarity_Score
        interactive: Prompt for minimum similarity score based on recommendations
    """
    # Set log level
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Determine paths
    script_dir = Path(__file__).parent
    
    if input_path is None:
        input_path = script_dir / 'ivntest.xlsx'
    else:
        input_path = Path(input_path)
    
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = script_dir / 'output'
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f'crosswalk_candidates_{timestamp}.csv'
    else:
        output_path = Path(output_path)
    
    # Validate input
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    logger.info("=" * 60)
    logger.info("IVN CROSSWALK OPTIMIZER")
    logger.info("=" * 60)
    logger.info(f"Input:  {input_path}")
    logger.info(f"Output: {output_path}")
    if min_score > 0:
        logger.info(f"Min Score Filter: {min_score}")
    if confidence_filter:
        logger.info(f"Confidence Filter: {confidence_filter}")
    logger.info("=" * 60)
      # Configure pipeline
    config = {
        'thresholds': {
            'min_score': min_score,  # Use CLI or interactive value, not hardcoded
            'similarity': 0.5,
            'high_confidence': 0.8,
            'medium_confidence': 0.6
        },
        'weights': {
            'text_similarity': 0.3,
            'embedding_similarity': 0.5,
            'enabling_score': 0.2
        },
        'rules': {
            'reject_same_source': True,
            'check_both_directions': True
        }
    }
    
    # Create and run pipeline
    pipeline = IVNProductionPipeline(config=config)
    result_df = pipeline.run(
        xlsx_path=str(input_path),
        output_path=str(output_path)
    )
    
    # Recommend min score based on first 100 scores
    avg_first_100, recommended_score = recommend_min_score(result_df, desired_matches=1000)
    if avg_first_100 is not None and recommended_score is not None:
        logger.info(f"Average Similarity_Score of first 100: {avg_first_100:.4f}")
        logger.info(f"Recommended minimum score for ~1000 matches: {recommended_score:.4f}")
    else:
        logger.info("No similarity scores available to recommend a minimum score (no matches found).")

    # Prompt user if interactive mode is enabled
    if interactive and recommended_score is not None:
        min_score = prompt_for_min_score(recommended_score)
        logger.info(f"Using minimum score: {min_score:.4f}")
    elif interactive:
        logger.info("Interactive mode enabled, but no recommended score available.")

    # Predict if output will exceed Excel's row limit (1,048,576 rows)
    predicted_rows = len(result_df)
    if predicted_rows > 1_000_000:
        # Estimate what min score would yield ~900,000 rows
        scores = result_df['Similarity_Score'].sort_values(ascending=False)
        if len(scores) > 900_000:
            est_score_900k = scores.iloc[900_000-1]
        else:
            est_score_900k = scores.min()
        logger.warning(f"Predicted output: {predicted_rows} rows (Excel limit is 1,048,576).")
        logger.warning(f"Estimated minimum score for ~900,000 rows: {est_score_900k:.4f}")
        # Prompt user to use this threshold
        try:
            user_input = input(f"Would you like to use {est_score_900k:.4f} as the minimum similarity score to reduce output to ~900,000 rows? [Y/n]: ").strip().lower()
            if user_input in ('', 'y', 'yes'):
                min_score = est_score_900k
                original_count = len(result_df)
                result_df = result_df[result_df['Similarity_Score'] >= min_score]
                logger.info(f"Filtered by min_score ({min_score}): {original_count} -> {len(result_df)} rows")
        except Exception:
            logger.warning("Could not prompt for minimum score. Proceeding with original filter.")

    # Apply filters if specified
    if min_score > 0:
        original_count = len(result_df)
        result_df = result_df[result_df['Similarity_Score'] >= min_score]
        logger.info(f"Filtered by min_score ({min_score}): {original_count} -> {len(result_df)} rows")
    if confidence_filter:
        original_count = len(result_df)
        result_df = result_df[result_df['Confidence_Bucket'] == confidence_filter]
        logger.info(f"Filtered by confidence ({confidence_filter}): {original_count} -> {len(result_df)} rows")
    if top_n is not None:
        original_count = len(result_df)
        result_df = result_df.sort_values('Similarity_Score', ascending=False).head(top_n)
        logger.info(f"Filtered to top {top_n} rows by Similarity_Score: {original_count} -> {len(result_df)} rows")

    # Ensure URLs are plain text (not Excel hyperlinks)
    url_columns = [
        'Enabling Component URL',
        'Dependent Component URL'
    ]
    for col in url_columns:
        if col in result_df.columns:
            # Convert to string, remove Excel hyperlink formulas if present
            result_df[col] = result_df[col].astype(str).str.replace(r'^=HYPERLINK\([^)]+\)$', '', regex=True)

    # Standardize and reorder columns before writing output
    expected_columns = [
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
        'Dependent Fetch Status',
        'Similarity_Score',
        'Confidence_Bucket',
        'Match_Direction'
    ]
    # Rename columns to match expected output if needed
    col_map = {c: c.replace('_', ' ') for c in result_df.columns if c.replace('_', ' ') in expected_columns}
    result_df = result_df.rename(columns=col_map)
    # Reorder columns, fill missing with blank
    for col in expected_columns:
        if col not in result_df.columns:
            result_df[col] = ''
    result_df = result_df[expected_columns]

    # Only write output if there are rows
    if len(result_df) > 0:
        # Write to Excel instead of CSV
        result_df.to_excel(output_path.with_suffix('.xlsx'), index=False)
        logger.info(f"Output saved to: {output_path.with_suffix('.xlsx')}")
    else:
        logger.warning(f"No results to write after filtering. Output file not created: {output_path.with_suffix('.xlsx')}")

    # Estimate threshold for max 100,000 rows
    avg_first_1000, est_threshold = estimate_threshold_for_max_rows(result_df, max_rows=100000)
    logger.info(f"Average Similarity_Score of first 1000: {avg_first_1000:.4f}")
    logger.info(f"Estimated threshold for <=100,000 rows: {est_threshold:.4f}")

    # Display summary
    logger.info("\n" + "=" * 60)
    logger.info("RESULTS SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total candidate pairs: {len(result_df)}")
    
    if 'Confidence_Bucket' in result_df.columns:
        logger.info("\nBy Confidence Bucket:")
        for bucket in ['High', 'Medium', 'Low']:
            count = len(result_df[result_df['Confidence_Bucket'] == bucket])
            logger.info(f"  {bucket}: {count}")
    
    if 'Match_Direction' in result_df.columns:
        logger.info("\nBy Match Direction:")
        for direction in result_df['Match_Direction'].unique():
            count = len(result_df[result_df['Match_Direction'] == direction])
            logger.info(f"  {direction}: {count}")
    
    logger.info(f"\nOutput saved to: {output_path}")
    logger.info("=" * 60)
    
    return result_df


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="IVN Crosswalk Optimizer - Find enabling relationships between components"
    )
    parser.add_argument(
        '--input', '-i',
        default=None,
        help='Path to ivntest.xlsx input file (default: ivntest.xlsx in script directory)'
    )
    parser.add_argument(
        '--output', '-o',
        default=None,
        help='Path for output CSV file (default: auto-generated in output/)'
    )
    parser.add_argument(
        '--min-score',
        type=float,
        default=0.0,
        help='Minimum similarity score to include (0.0-1.0, default: 0.0)'
    )
    parser.add_argument(
        '--confidence',
        choices=['High', 'Medium', 'Low'],
        default=None,
        help='Filter by confidence bucket'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    parser.add_argument(
        '--top-n',
        type=int,
        default=None,
        help='Only output the top N rows by Similarity_Score'
    )
    parser.add_argument(
        '--interactive',
        action='store_true',
        help='Prompt for minimum similarity score based on recommendations'
    )
    
    args = parser.parse_args()
    
    try:
        run_crosswalk(
            input_path=args.input,
            output_path=args.output,
            min_score=args.min_score,
            confidence_filter=args.confidence,
            verbose=args.verbose,
            top_n=args.top_n,
            interactive=args.interactive
        )
    except Exception as e:
        logger.error(f"Crosswalk failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
