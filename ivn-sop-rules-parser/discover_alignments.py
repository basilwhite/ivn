import json
import re
from pdf_parser import get_excel_data

# Stop words to filter out common, non-descriptive words
STOP_WORDS = set([
    "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
    "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers",
    "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves",
    "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are",
    "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does",
    "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until",
    "while", "of", "at", "by", "for", "with", "about", "against", "between", "into",
    "through", "during", "before", "after", "above", "below", "to", "from", "up", "down",
    "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here",
    "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more",
    "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so",
    "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"
])

def get_keywords(text):
    """Extracts and filters keywords from a text string."""
    if not isinstance(text, str):
        return []
    text = re.sub(r'[^\w\s]', '', text.lower())
    return [word for word in text.split() if word not in STOP_WORDS]

def find_alignments(m_25_22_components, ivn_data, threshold=2):
    """
    Finds alignments between M-25-22 components and the IVN database
    using a keyword scoring system.
    """
    alignments = []
    ivn_header = ivn_data[0]
    ivn_rows = ivn_data[1:]

    for category, components in m_25_22_components.items():
        for component in components:
            m2522_keywords = set(get_keywords(component.get("component", "")) + get_keywords(component.get("description", "")))
            
            for row in ivn_rows:
                ivn_component_data = dict(zip(ivn_header, row))
                
                ivn_name = str(ivn_component_data.get("component_name") or "")
                ivn_desc = str(ivn_component_data.get("component_description") or "")
                ivn_source_name_keywords = get_keywords(ivn_name + " " + ivn_desc)
                
                score = len(m2522_keywords.intersection(ivn_source_name_keywords))

                if score >= threshold:
                    alignments.append({
                        "m_25_22_component": component,
                        "ivn_component": ivn_name,
                        "ivn_description": ivn_desc,
                        "alignment_score": score
                    })
    return alignments

def main():
    print("Starting alignment discovery with scoring...")
    with open("m_25_22_components.json", "r") as f:
        m_25_22_components = json.load(f)
    print("Loaded M-25-22 components.")

    ivn_db_path = "C:\\Users\\Basil.White\\OneDrive - USDA\\OCIO-STRATUS Governance Document Working Group - Documents\\USDA-IVN-dataset.xlsx"
    ivn_data = get_excel_data(ivn_db_path)
    print("Loaded IVN data.")

    if isinstance(ivn_data, str) and (ivn_data.startswith("Error:") or ivn_data.startswith("An error occurred:")):
        print(ivn_data)
        return

    alignments = find_alignments(m_25_22_components, ivn_data)
    
    # Sort alignments by score in descending order
    alignments.sort(key=lambda x: x['alignment_score'], reverse=True)
    
    print(f"Found {len(alignments)} potential alignments with a score >= 2.")

    with open("alignments.json", "w") as f:
        json.dump(alignments, f, indent=4)

    print(f"Successfully saved {len(alignments)} potential alignments to alignments.json")

if __name__ == "__main__":
    main()
