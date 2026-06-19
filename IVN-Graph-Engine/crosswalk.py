import pandas as pd
from collections import Counter
import re

# A list of common English stop words
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
    # Remove punctuation and convert to lowercase
    text = re.sub(r'[^\w\s]', '', text.lower())
    # Split into words and filter out stop words
    return [word for word in text.split() if word not in STOP_WORDS]

def crosswalk_datasets(m2522_path, ivn_path, alignments_path, threshold=2):
    """
    Performs a crosswalk between the M-25-22 components and the IVN database
    using a keyword scoring system.

    Args:
        m2522_path (str): File path of the M-25-22 components CSV.
        ivn_path (str): File path of the IVN database CSV.
        alignments_path (str): File path to save the alignments CSV.
        threshold (int): The minimum score to consider an alignment valid.
    """
    try:
        m2522_df = pd.read_csv(m2522_path)
        ivn_df = pd.read_csv(ivn_path)

        alignments = []

        for _, m2522_row in m2522_df.iterrows():
            m2522_component_keywords = get_keywords(m2522_row['Component'])
            m2522_title_keywords = get_keywords(m2522_row['Title'])
            m2522_keywords = set(m2522_component_keywords + m2522_title_keywords)

            for _, ivn_row in ivn_df.iterrows():
                ivn_source_name_keywords = get_keywords(ivn_row['source_name'])
                
                # Calculate score based on number of matching keywords
                score = len(m2522_keywords.intersection(ivn_source_name_keywords))

                if score >= threshold:
                    alignments.append({
                        'M-25-22 Section': m2522_row['Section'],
                        'M-25-22 Title': m2522_row['Title'],
                        'M-25-22 Component': m2522_row['Component'],
                        'IVN Source Name': ivn_row['source_name'],
                        'IVN Source Agency': ivn_row['source_agency'],
                        'Alignment Score': score
                    })

        alignments_df = pd.DataFrame(alignments)
        # Sort by score in descending order
        alignments_df = alignments_df.sort_values(by='Alignment Score', ascending=False)
        alignments_df.to_csv(alignments_path, index=False)
        print(f"Successfully created alignments file at {alignments_path}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    m2522_file = "c:\\Users\\Basil.White\\OneDrive - USDA\\OCIO-STRATUS Governance Document Working Group - Documents\\Python\\scripts\\ivn\\IVN-Graph-Engine\\M-25-22-components.csv"
    ivn_file = "c:\\Users\\Basil.White\\OneDrive - USDA\\OCIO-STRATUS Governance Document Working Group - Documents\\Python\\scripts\\ivn\\IVN-Graph-Engine\\USDA-IVN-dataset.csv"
    alignments_file = "c:\\Users\\Basil.White\\OneDrive - USDA\\OCIO-STRATUS Governance Document Working Group - Documents\\Python\\scripts\\ivn\\IVN-Graph-Engine\\alignments.csv"
    crosswalk_datasets(m2522_file, ivn_file, alignments_file)
