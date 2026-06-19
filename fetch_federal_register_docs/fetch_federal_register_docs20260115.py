import requests
import pandas as pd
from datetime import datetime
import os

url = "https://www.federalregister.gov/api/v1/documents.json"
params = {
    # Add your query parameters here
    "per_page": 100,
    "order": "newest"
}

response = requests.get(url, params=params)
data = response.json()

records = []
for doc in data.get("results", []):
    records.append({
        "Component": doc.get("title", ""),
        "Source ": doc.get("title", ""),
        "Component Description": doc.get("abstract", ""),
        "URL": doc.get("html_url", "")
    })

df = pd.DataFrame(records)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"federal_register_docs_{timestamp}.csv"
output_path = os.path.join(os.getcwd(), filename)
df.to_csv(output_path, index=False, encoding="utf-8")
print(f"📁 Saved {len(df)} records to {output_path}")