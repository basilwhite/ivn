import json
import csv
from pdf_parser import get_excel_data
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import datetime
import os

def find_alignments(eo_components_path, ivn_data_path, output_path, similarity_threshold=0.2):
    """
    Finds and scores alignments between Executive Order components and IVN data
    using a self-contained TF-IDF model for semantic analysis. This approach
    works entirely offline after initial library installation.
    """
    print("Starting offline semantic alignment discovery for EO 14240...")

    # Load Executive Order components
    with open(eo_components_path, 'r', encoding='utf-8') as f:
        eo_components = json.load(f)
    print("Loaded Executive Order components.")
    eo_texts = [comp.get('component', '') for comp in eo_components]

    # Load IVN data from the "Components" sheet
    ivn_data = get_excel_data(ivn_data_path, sheet_name="Components")
    if isinstance(ivn_data, str) and ivn_data.startswith("Error"):
        print(ivn_data)
        return
    
    ivn_headers = [h.strip() for h in ivn_data[0]] if ivn_data else []
    ivn_rows = ivn_data[1:]
    print(f"Loaded IVN data with headers: {ivn_headers}")

    try:
        name_idx = ivn_headers.index("component_name")
        desc_idx = ivn_headers.index("component_description")
        id_idx = ivn_headers.index("component_id")
        url_idx = ivn_headers.index("component_url")
        source_id_idx = ivn_headers.index("source_id")
    except ValueError as e:
        print(f"Error: Missing required column in IVN data - {e}")
        print(f"Headers found: {ivn_headers}")
        return

    ivn_texts = [f"{row[name_idx] if len(row) > name_idx else ''}: {row[desc_idx] if len(row) > desc_idx else ''}" for row in ivn_rows]

    # --- TF-IDF Vectorization ---
    print("Building TF-IDF model from corpus...")
    # Initialize the vectorizer. `stop_words='english'` removes common English words.
    vectorizer = TfidfVectorizer(stop_words='english', norm='l2')

    # Combine all texts to build a comprehensive vocabulary
    all_texts = eo_texts + ivn_texts
    
    # Fit the model on the entire corpus and transform the texts into vectors
    tfidf_matrix = vectorizer.fit_transform(all_texts)

    # Split the matrix back into EO and IVN vectors
    eo_embeddings = tfidf_matrix[:len(eo_texts)]
    ivn_embeddings = tfidf_matrix[len(eo_texts):]
    
    print("TF-IDF model built and text vectorized successfully.")

    # Calculate cosine similarity between all EO and IVN components
    print("Calculating cosine similarity matrix...")
    similarity_matrix = cosine_similarity(eo_embeddings, ivn_embeddings)

    all_alignments = []
    print(f"Processing alignments with a similarity threshold of {similarity_threshold}...")
    for i, eo_comp in enumerate(eo_components):
        for j, ivn_row in enumerate(ivn_rows):
            score = similarity_matrix[i][j]

            if score >= similarity_threshold:
                ivn_component_id = ivn_row[id_idx] if len(ivn_row) > id_idx else ""
                alignment = {
                    "eo_14240_component": eo_comp['component'],
                    "section_number": eo_comp['section_number'],
                    "section_title": eo_comp['section_title'],
                    "component": eo_comp['component'],
                    "ivn_component_name": ivn_row[name_idx] if len(ivn_row) > name_idx else "",
                    "ivn_component_description": ivn_row[desc_idx] if len(ivn_row) > desc_idx else "",
                    "ivn_component_source": ivn_row[source_id_idx] if len(ivn_row) > source_id_idx else "",
                    "eo_component_url": "https://www.whitehouse.gov/briefing-room/presidential-actions/2025/03/20/executive-order-on-eliminating-waste-and-saving-taxpayer-dollars-by-consolidating-procurement/",
                    "ivn_component_url": ivn_row[url_idx] if len(ivn_row) > url_idx else "",
                    "alignment_score": float(score) # Ensure score is a standard float
                }
                all_alignments.append(alignment)

    # Sort alignments by score in descending order
    sorted_alignments = sorted(all_alignments, key=lambda x: x['alignment_score'], reverse=True)
    
    print(f"Found {len(sorted_alignments)} potential alignments with a score >= {similarity_threshold}.")

    # Generate timestamp prefix
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")
    
    # Prepend timestamp to the output filenames
    base, ext = os.path.splitext(output_path)
    
    # Save to CSV
    output_csv_path = f"{timestamp}_{base}.csv"
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as f:
        if sorted_alignments:
            writer = csv.DictWriter(f, fieldnames=sorted_alignments[0].keys())
            writer.writeheader()
            writer.writerows(sorted_alignments)

    print(f"Successfully saved TF-IDF semantic alignments to {output_csv_path}")


if __name__ == "__main__":
    ivn_path = "C:\\Users\\Basil.White\\OneDrive - USDA\\OCIO-STRATUS Governance Document Working Group - Documents\\USDA-IVN-dataset.xlsx"
    find_alignments(
        'eo_14240_components.json',
        ivn_path,
        'eo_14240_semantic_alignments.json'
    )
