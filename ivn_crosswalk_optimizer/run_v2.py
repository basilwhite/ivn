"""Run the pipeline using the v2 file."""
import sys
import logging

# Setup logging to file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_v2_run.log', mode='w'),
        logging.StreamHandler()
    ]
)

print("Starting pipeline run...")

try:
    from ivn_pipeline_v2 import IVNProductionPipeline
    print("Import successful!")
    
    config = {
        'thresholds': {'min_score': 0.6, 'high_confidence': 0.8, 'medium_confidence': 0.6},
        'rules': {'reject_same_source': True, 'reject_self_match': True}
    }
    
    pipeline = IVNProductionPipeline(config=config)
    print("Pipeline created!")
    
    from datetime import datetime
    output_file = f"output/crosswalk_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    result = pipeline.run('ivntest.xlsx', output_file)
    
    print(f"\n{'='*60}")
    print(f"RESULTS")
    print(f"{'='*60}")
    print(f"Total output rows: {len(result)}")
    print(f"Output saved to: {output_file}")
    
    if 'Confidence_Bucket' in result.columns:
        print("\nConfidence Bucket Distribution:")
        for bucket in ['High', 'Medium', 'Low']:
            count = len(result[result['Confidence_Bucket'] == bucket])
            print(f"  {bucket}: {count}")
    
    # Check for self-matches
    if not result.empty:
        self_matches = result[
            result['Enabling Component'].str.lower() == result['Dependent Component'].str.lower()
        ]
        print(f"\nSelf-matches in output: {len(self_matches)}")
        
        same_source = result[
            result['Enabling Source'].str.lower() == result['Dependent Source'].str.lower()
        ]
        print(f"Same-source matches in output: {len(same_source)}")
    
    print("\nDone!")
    
except Exception as e:
    import traceback
    print(f"ERROR: {e}")
    traceback.print_exc()

# Write summary to file
with open('run_summary.txt', 'w') as f:
    f.write("Pipeline run completed - check pipeline_v2_run.log for details\n")
