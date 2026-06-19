import joblib
import json

# Load algorithm
extractor = joblib.load('component_extraction_algorithm.joblib')

# Load rules
with open('component_extraction_rules.json', 'r', encoding='utf-8') as f:
    rules = json.load(f)

# Sample text simulating a governance requirement
sample_text = "The CIO must verify A, B, and C for annual compliance; Agencies must report progress quarterly."

print("Sample text:")
print(sample_text)
print("\nExtracted atomic components:")
components = extractor.extract(sample_text)
for i, comp in enumerate(components, 1):
    print(f"Component {i}: {comp}")
