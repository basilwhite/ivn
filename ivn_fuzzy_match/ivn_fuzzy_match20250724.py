import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# --- RAG Pipeline Integration ---
# Assumes you have a function `retrieve_similar_examples` that queries a vector DB or RAG index.
# For demonstration, this is a stub. Replace with actual CoreAI RAG Pipeline integration.
def retrieve_similar_examples(enabling, dependent, top_k=3):
    """
    Retrieve top_k similar historical alignments, definitions, or policy precedents
    from a RAG index to inform and validate the heuristic.
    Returns a list of dicts: [{'enabling': ..., 'dependent': ..., 'justification': ..., 'score': ...}, ...]
    """
    # TODO: Replace this stub with actual RAG pipeline retrieval logic.
    # Example return format:
    return [
        {
            'enabling': 'Develop guidance on cross-agency cybersecurity protocols',
            'dependent': 'Implement secure shared data environments',
            'justification': 'The guidance reduces risk and accelerates compliance for multi-agency data platforms',
            'score': 91
        },
        {
            'enabling': 'Provide funding for cloud migration',
            'dependent': 'Modernize legacy IT systems',
            'justification': 'Funding is a prerequisite for modernization efforts across agencies',
            'score': 88
        }
    ][:top_k]

# --- Helper function to infer IVN alignment with RAG grounding ---
def infer_alignment(enabling, dependent):
    """
    Infers if enabling causally supports dependent, per IVN logic, with RAG grounding.
    Returns (justification, score, rag_examples) or (None, None, None) if not aligned.
    """
    e, d = enabling.lower(), dependent.lower()
    causal_verbs = [
        'enable', 'support', 'provide', 'supply', 'reduce', 'improve', 'accelerate',
        'facilitate', 'ensure', 'deliver', 'authorize', 'fund', 'mandate', 'require'
    ]
    shared_keywords = [
        'compliance', 'risk', 'speed', 'law', 'performance', 'priority', 'outcome',
        'security', 'interoperability', 'transparency', 'efficiency', 'oversight'
    ]
    prerequisite_phrases = [
        'guidance', 'policy', 'training', 'infrastructure', 'funding', 'authorization', 'approval'
    ]

    # Retrieve similar historical alignments for grounding
    rag_examples = retrieve_similar_examples(enabling, dependent, top_k=3)

    # 1. Causal verbs in enabling
    if any(verb in e for verb in causal_verbs):
        justification = (
            f"{enabling} provides a critical input or reduces risk for {dependent}. "
            f"Similar precedent: \"{rag_examples[0]['justification']}\""
            if rag_examples else
            f"{enabling} provides a critical input or reduces risk for {dependent}."
        )
        score = 90 if rag_examples else 85
    # 2. Shared outcome/priority/law keywords
    elif any(word in e and word in d for word in shared_keywords):
        justification = (
            f"Both components serve a shared outcome or legal requirement. "
            f"Example: \"{rag_examples[0]['justification']}\""
            if rag_examples else
            "Both components serve a shared outcome or legal requirement."
        )
        score = 80 if rag_examples else 75
    # 3. Enabling is a prerequisite
    elif any(phrase in e for phrase in prerequisite_phrases):
        justification = (
            f"{enabling} is a prerequisite for effective delivery of {dependent}. "
            f"Historical case: \"{rag_examples[0]['justification']}\""
            if rag_examples else
            f"{enabling} is a prerequisite for effective delivery of {dependent}."
        )
        score = 78 if rag_examples else 70
    else:
        return None, None, None
    return justification, score, rag_examples

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
        if enabling == dependent:
            continue
        sim_score = similarity_matrix[i, j]
        if sim_score < 0.05:
            continue
        justification, causal_score, rag_examples = infer_alignment(enabling, dependent)
        if justification:
            # Combine similarity and causal score for final confidence (weighted)
            final_score = int(0.3 * (sim_score * 100) + 0.7 * causal_score)
            # Add RAG provenance for explainability
            provenance = "; ".join(
                [f"Enabling: {ex['enabling']} | Dependent: {ex['dependent']} | Justification: {ex['justification']} | Score: {ex['score']}
                 for ex in rag_examples]
            ) if rag_examples else ""
            output_rows.append({
                'Enabling Component': enabling,
                'Dependent Component': dependent,
                'Justification': justification,
                'Similarity Score': final_score,
                'RAG Provenance': provenance
            })

# Output DataFrame
output_df = pd.DataFrame(output_rows)
output_df.to_csv('ivn_inferred_alignments.csv', index=False)
print("Done!")
print("IVN inferred alignments saved to ivn_inferred_alignments.csv")