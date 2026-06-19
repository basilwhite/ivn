"""Test import and run."""
import sys
import traceback

# Try to import
try:
    import ivn_production_pipeline
    print("Module imported OK")
    print(f"Has class: {hasattr(ivn_production_pipeline, 'IVNProductionPipeline')}")
    
    # List what's in the module
    items = [x for x in dir(ivn_production_pipeline) if not x.startswith('_')]
    print(f"Public items: {items}")
    
except Exception as e:
    print(f"ERROR: {e}")
    traceback.print_exc()

# Write results to file
with open('test_result.txt', 'w') as f:
    f.write("Test completed - check console output\n")
