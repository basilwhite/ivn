"""Simple runner script for the IVN production pipeline."""
import logging
import sys
import traceback
from datetime import datetime

# Set up logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline_run.log', mode='w')
    ]
)

print("=" * 60)
print("IVN Pipeline Runner")
print("=" * 60)

try:
    from ivn_production_pipeline import IVNProductionPipeline
    print("Import successful")
except Exception as e:
    print(f"Import error: {e}")
    with open('import_error.log', 'w') as f:
        traceback.print_exc(file=f)
    traceback.print_exc()
    sys.exit(1)

# Create pipeline with config
config = {
    'thresholds': {
        'min_score': 0.6,
        'high_confidence': 0.8,
        'medium_confidence': 0.6
    },
    'rules': {
        'reject_same_source': True,
        'reject_self_match': True,
        'check_both_directions': True
    }
}

print("Creating pipeline...")
pipeline = IVNProductionPipeline(config=config)

print("Running pipeline...")
output_file = f"output/crosswalk_fixed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
result = pipeline.run('ivntest.xlsx', output_file)

print("=" * 60)
print("RESULTS")
print("=" * 60)
print(f"Total output rows: {len(result)}")
print(f"Output saved to: {output_file}")

# Show bucket distribution
if 'Confidence_Bucket' in result.columns:
    print("\nConfidence Bucket Distribution:")
    for bucket in ['High', 'Medium', 'Low']:
        count = len(result[result['Confidence_Bucket'] == bucket])
        print(f"  {bucket}: {count}")

# Check for any self-matches that might have slipped through
if not result.empty:
    self_matches = result[
        result['Enabling Component'].str.lower() == result['Dependent Component'].str.lower()
    ]
    print(f"\nSelf-matches in output: {len(self_matches)}")
    
    # Check same-source matches
    same_source = result[
        result['Enabling Source'].str.lower() == result['Dependent Source'].str.lower()
    ]
    print(f"Same-source matches in output: {len(same_source)}")

print("\nDone!")
