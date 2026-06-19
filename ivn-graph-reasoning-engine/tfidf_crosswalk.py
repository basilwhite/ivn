import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

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
    Extracts component names and descriptions from the IVN database Excel file.
    Returns a dictionary mapping component names to their descriptions.
    """
    df = pd.read_excel(file_path, sheet_name='Components')
    # Drop rows where component_name or component_description is NaN
    df.dropna(subset=['component_name', 'component_description'], inplace=True)
    return pd.Series(df.component_description.values,index=df.component_name).to_dict()

def find_tfidf_alignments(m2522_components, ivn_components_dict):
    """
    Finds alignments using TF-IDF and cosine similarity.
    """
    ivn_component_names = list(ivn_components_dict.keys())
    ivn_component_descriptions = list(ivn_components_dict.values())

    # Combine all components to build the TF-IDF vocabulary
    all_components = m2522_components + ivn_component_names
    
    vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix = vectorizer.fit_transform(all_components)
    
    # Split the matrix back into M-25-22 and IVN components
    m2522_tfidf = tfidf_matrix[:len(m2522_components)]
    ivn_tfidf = tfidf_matrix[len(m2522_components):]
    
    # Compute cosine similarity
    cosine_similarities = cosine_similarity(m2522_tfidf, ivn_tfidf)
    
    alignments = []
    # For each M-25-22 component, find the top 3 most similar IVN components
    for i in range(len(m2522_components)):
        # Get the indices of the top 3 scores
        top_indices = cosine_similarities[i].argsort()[-3:][::-1]
        
        for idx in top_indices:
            alignments.append({
                "m2522_component": m2522_components[i],
                "ivn_component_name": ivn_component_names[idx],
                "ivn_component_description": ivn_component_descriptions[idx],
                "similarity_score": cosine_similarities[i][idx]
            })
    return alignments

def write_alignment_report(alignments, output_file):
    """
    Writes the TF-IDF alignment report to a file.
    """
    # Sort alignments by M-25-22 component and then by similarity score
    sorted_alignments = sorted(alignments, key=lambda x: (x['m2522_component'], x['similarity_score']), reverse=True)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("M-25-22 and IVN Database TF-IDF Alignment Report\n")
        f.write("================================================\n\n")
        
        from itertools import groupby
        
        # Group by M-25-22 component
        for key, group in groupby(sorted_alignments, lambda x: x['m2522_component']):
            f.write(f"### M-25-22 Component: **{key}**\n\n")
            # Sort the group by similarity score
            sorted_group = sorted(list(group), key=lambda x: x['similarity_score'], reverse=True)
            for alignment in sorted_group:
                if alignment['similarity_score'] > 0.1: # Threshold to show only relevant results
                    f.write(f"- **IVN Component Name:** {alignment['ivn_component_name']}\n")
                    f.write(f"- **IVN Component Description:** {alignment['ivn_component_description']}\n")
                    f.write(f"- **Similarity Score:** {alignment['similarity_score']:.4f}\n")
                    f.write(f"- **Justification:** This IVN component's name has a high semantic similarity with the M-25-22 component, suggesting it can enable or support its implementation.\n\n")

if __name__ == "__main__":
    m2522_components_file = "M-25-22_components.txt"
    ivn_database_file = r"C:\Users\Basil.White\OneDrive - USDA\OCIO-STRATUS Governance Document Working Group - Documents\USDA-IVN-dataset.xlsx"
    alignment_report_file = "tfidf_alignment_report_v2.txt"

    # Extract components
    m2522_components = extract_m2522_components(m2522_components_file)
    ivn_components_dict = extract_ivn_components(ivn_database_file)

    # Find alignments
    alignments = find_tfidf_alignments(m2522_components, ivn_components_dict)

    # Write the report
    write_alignment_report(alignments, alignment_report_file)

    print(f"TF-IDF alignment report generated: {alignment_report_file}")
