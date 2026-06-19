import pandas as pd
from sentence_transformers import SentenceTransformer, util
import torch

def extract_m2522_components(file_path):
    """
    Extracts components from the M-25-22 text file.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    components = []
    sections = content.split('\n## ')[1:] 
    for section in sections:
        lines = section.strip().split('\n')
        items = [line.strip().lstrip('- ') for line in lines[1:] if line.strip().startswith('- ')]
        components.extend(items)
    return components

def extract_ivn_components(file_path):
    """
    Extracts components from the IVN database Excel file.
    """
    df = pd.read_excel(file_path, sheet_name='Components')
    # Drop rows where component_description is NaN
    df.dropna(subset=['component_description'], inplace=True)
    return df['component_description'].tolist()

def find_semantic_alignments(m2522_components, ivn_components, model):
    """
    Finds semantic alignments between M-25-22 and IVN components.
    """
    # Encode the components to get their embeddings
    m2522_embeddings = model.encode(m2522_components, convert_to_tensor=True)
    ivn_embeddings = model.encode(ivn_components, convert_to_tensor=True)

    # Compute cosine similarity between all pairs
    cosine_scores = util.cos_sim(m2522_embeddings, ivn_embeddings)

    alignments = []
    # For each M-25-22 component, find the top 3 most similar IVN components
    for i in range(len(m2522_components)):
        top_results = torch.topk(cosine_scores[i], k=3)
        
        for score, idx in zip(top_results[0], top_results[1]):
            alignments.append({
                "m2522_component": m2522_components[i],
                "ivn_component": ivn_components[idx],
                "similarity_score": score.item()
            })
    return alignments

def write_alignment_report(alignments, output_file):
    """
    Writes the semantic alignment report to a file.
    """
    # Sort alignments by similarity score in descending order
    sorted_alignments = sorted(alignments, key=lambda x: x['similarity_score'], reverse=True)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("M-25-22 and IVN Database Semantic Alignment Report\n")
        f.write("================================================\n\n")
        
        # Group by M-25-22 component
        from itertools import groupby
        
        for key, group in groupby(sorted_alignments, lambda x: x['m2522_component']):
            f.write(f"### M-25-22 Component: {key}\n\n")
            for alignment in group:
                f.write(f"- **IVN Component:** {alignment['ivn_component']}\n")
                f.write(f"- **Similarity Score:** {alignment['similarity_score']:.4f}\n")
                f.write(f"- **Justification:** This IVN component is semantically similar to the M-25-22 component, suggesting it can enable or support its implementation.\n\n")

if __name__ == "__main__":
    m2522_components_file = "M-25-22_components.txt"
    ivn_database_file = r"C:\Users\Basil.White\OneDrive - USDA\OCIO-STRATUS Governance Document Working Group - Documents\USDA-IVN-dataset.xlsx"
    alignment_report_file = "semantic_alignment_report.txt"

    # Load a pre-trained sentence transformer model
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Extract components
    m2522_components = extract_m2522_components(m2522_components_file)
    ivn_components = extract_ivn_components(ivn_database_file)

    # Find alignments
    alignments = find_semantic_alignments(m2522_components, ivn_components, model)

    # Write the report
    write_alignment_report(alignments, alignment_report_file)

    print(f"Semantic alignment report generated: {alignment_report_file}")
