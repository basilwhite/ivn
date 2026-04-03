import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# --- Helper function to infer IVN alignment ---
def infer_alignment(enabling, dependent):
    """
    Infers if enabling causally supports dependent, per IVN logic.
    Returns (justification, score) or (None, None) if not aligned.
    """
    # Lowercase for easier matching
    e, d = enabling.lower(), dependent.lower()
    # Example heuristics (expand as needed)
    # 1. Look for causal verbs in enabling
    causal_verbs = ['enable', 'support', 'provide', 'supply', 'reduce', 'improve', 'accelerate', 'facilitate', 'ensure', 'deliver']
    if any(verb in e for verb in causal_verbs):
        justification = f"{enabling} provides a critical input or reduces risk for {dependent}"
        score = 85
    # 2. Look for shared outcome/priority/law keywords
    elif any(word in e and word in d for word in ['compliance', 'risk', 'speed', 'law', 'performance', 'priority', 'outcome']):
        justification = f"Both components serve a shared outcome or legal requirement"
        score = 75
    # 3. If enabling missing, would dependent be degraded? (simple heuristic: enabling is a prerequisite)
    elif any(phrase in e for phrase in ['guidance', 'policy', 'training', 'infrastructure', 'funding']):
        justification = f"{enabling} is a prerequisite for effective delivery of {dependent}"
        score = 70
    else:
        # No clear causal/transactive link
        return None, None
    return justification, score

# --- Load data ---
file_path = 'ivntest.xlsx'
df = pd.read_excel(file_path)
df = df.fillna('')

# Extract unique pairs
unique_pairs = df[['Enabling Component Description', 'Dependent Component Description']].drop_duplicates()

# Vectorize for initial similarity filtering
vectorizer = TfidfVectorizer()
enabling_vecs = vectorizer.fit_transform(unique_pairs['Enabling Component Description'])
dependent_vecs = vectorizer.transform(unique_pairs['Dependent Component Description'])
similarity_matrix = cosine_similarity(enabling_vecs, dependent_vecs)

# Prepare output rows
output_rows = []
for i, enabling in enumerate(unique_pairs['Enabling Component Description']):
    for j, dependent in enumerate(unique_pairs['Dependent Component Description']):
        # Skip self-pairings
        if enabling == dependent:
            continue
        # Use similarity as a pre-filter (adjust threshold as needed)
        sim_score = similarity_matrix[i, j]
        if sim_score < 0.05:
            continue
        # Apply IVN logic
        justification, causal_score = infer_alignment(enabling, dependent)
        if justification:
            # Combine similarity and causal score for final confidence (weighted)
            final_score = int(0.3 * (sim_score * 100) + 0.7 * causal_score)
            output_rows.append({
                'Enabling Component': enabling,
                'Dependent Component': dependent,
                'Justification': justification,
                'Similarity Score': final_score
            })

# Output DataFrame
output_df = pd.DataFrame(output_rows)
output_df.to_csv('ivn_inferred_alignments.csv', index=False)
print("Done!")
print("IVN inferred alignments saved to ivn_inferred_alignments.csv")