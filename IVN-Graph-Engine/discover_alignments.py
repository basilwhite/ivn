import json
import pandas as pd
import re
from collections import Counter

# Using a predefined list of common English stop words.
STOP_WORDS = set([
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could",
    "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for",
    "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's",
    "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm",
    "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't",
    "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours",
    "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't",
    "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there",
    "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too",
    "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't",
    "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's",
    "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself",
    "yourselves", "federal", "government", "agencies", "agency"
])

def get_keywords(text):
    """Extracts and cleans keywords from a text string."""
    if not isinstance(text, str):
        return []
    text = re.sub(r'[^\w\s]', '', text.lower())
    words = text.split()
    return [word for word in words if word not in STOP_WORDS and len(word) > 3]

def find_alignments(m_25_22_path, ivn_data_path):
    """
    Finds and scores alignments between M-25-22 components and the IVN database.
    """
    with open(m_25_22_path, 'r') as f:
        m_25_22_components = json.load(f)

    ivn_df = pd.read_csv(ivn_data_path)
    alignments = []

    for category, components in m_25_22_components.items():
        for component in components:
            comp_text = component.get("component", "") + " " + component.get("description", "")
            comp_keywords = get_keywords(comp_text)
            
            if not comp_keywords:
                continue

            for _, row in ivn_df.iterrows():
                ivn_name = str(row.get("Component Name") or "")
                ivn_desc = str(row.get("Component Description") or "")
                ivn_text = ivn_name + " " + ivn_desc
                ivn_keywords = get_keywords(ivn_text)

                if not ivn_keywords:
                    continue

                # Calculate alignment score based on common keywords
                common_keywords = Counter(comp_keywords) & Counter(ivn_keywords)
                score = sum(common_keywords.values())

                if score > 1:  # Require at least 2 keyword matches
                    alignments.append({
                        "m_25_22_component": component,
                        "ivn_component": ivn_name,
                        "ivn_description": ivn_desc,
                        "alignment_score": score,
                        "common_keywords": list(common_keywords.keys())
                    })

    # Sort alignments by score in descending order
    alignments.sort(key=lambda x: x['alignment_score'], reverse=True)
    return alignments

def main():
    print("Starting alignment discovery...")
    m_25_22_json_path = "m_25_22_components.json"
    ivn_csv_path = "sample_ivn_data.csv" # Using the CSV I created previously
    
    alignments = find_alignments(m_25_22_json_path, ivn_csv_path)
    print(f"Found {len(alignments)} potential alignments.")

    with open("alignments.json", "w") as f:
        json.dump(alignments, f, indent=2)

    print(f"Successfully saved {len(alignments)} potential alignments to alignments.json")

if __name__ == "__main__":
    main()
