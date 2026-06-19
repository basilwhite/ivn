import os
import sys

# Check file size
filepath = 'ivn_production_pipeline.py'
try:
    size = os.path.getsize(filepath)
    with open(filepath, 'r') as f:
        content = f.read()
    
    result = f"""File: {filepath}
Size: {size} bytes
Lines: {content.count(chr(10))}
Has 'class IVNProductionPipeline': {'class IVNProductionPipeline' in content}
First 200 chars: {content[:200]}
"""
except Exception as e:
    result = f"Error: {e}"

# Write to file
with open('file_check.txt', 'w') as f:
    f.write(result)

print("Check file_check.txt")
