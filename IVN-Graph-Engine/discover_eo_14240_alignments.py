import json
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# Load a pre-trained model for generating sentence embeddings
model = SentenceTransformer('all-MiniLM-L6-v2')

def find_eo_alignments_semantic(eo_components_path, ivn_excel_path, similarity_threshold=0.5):
    """
    Finds and scores alignments between Executive Order 14240 components and the IVN database
    using semantic similarity.
    """
    print("Loading and processing data for semantic analysis...")
    with open(eo_components_path, 'r') as f:
        eo_components_data = json.load(f)

    try:
        ivn_df = pd.read_excel(ivn_excel_path)
    except FileNotFoundError:
        print(f"Error: The file was not found at {ivn_excel_path}")
        return []
    except Exception as e:
        print(f"An error occurred while reading the Excel file: {e}")
        return []

    # --- Prepare EO 14240 Data ---
    eo_components = []
    eo_texts = []
    for section, components in eo_components_data.items():
        for component in components:
            text = component.get("description", "")
            if text:
                eo_components.append({"section": section, "component": component})
                eo_texts.append(text)

    # --- Prepare IVN Data ---
    ivn_df.columns = [col.strip().lower().replace(' ', '_') for col in ivn_df.columns]
    if 'component_name' not in ivn_df.columns or 'component_description' not in ivn_df.columns:
        # Let's try to find suitable columns if the expected ones aren't there.
        # This is a common issue. Let's look for the first two columns as a fallback.
        if len(ivn_df.columns) >= 2:
            ivn_df.rename(columns={ivn_df.columns[0]: 'component_name', ivn_df.columns[1]: 'component_description'}, inplace=True)
            print(f"Warning: Could not find 'component_name' or 'component_description'. Using columns '{ivn_df.columns[0]}' and '{ivn_df.columns[1]}' as fallback.")
        else:
            print("Error: Cannot find suitable columns for component name and description in the IVN Excel file.")
            return []

    ivn_df['full_text'] = ivn_df['component_name'].fillna('') + ' ' + ivn_df['component_description'].fillna('')
    ivn_texts = ivn_df['full_text'].tolist()

    if not eo_texts or not ivn_texts:
        print("No text data found to analyze.")
        return []

    print("Generating semantic embeddings for EO components...")
    eo_embeddings = model.encode(eo_texts, convert_to_tensor=True)
    
    print("Generating semantic embeddings for IVN components...")
    ivn_embeddings = model.encode(ivn_texts, convert_to_tensor=True)

    print("Calculating cosine similarity between all components...")
    # Calculate cosine similarity between all EO and IVN components
    similarity_matrix = cosine_similarity(eo_embeddings.cpu(), ivn_embeddings.cpu())

    alignments = []
    for i, eo_comp in enumerate(eo_components):
        for j, ivn_row in ivn_df.iterrows():
            score = similarity_matrix[i][j]
            if score >= similarity_threshold:
                alignments.append({
                    "eo_14240_section": eo_comp["section"],
                    "eo_14240_component": eo_comp["component"],
                    "ivn_component": ivn_row["component_name"],
                    "ivn_description": ivn_row["component_description"],
                    "alignment_score": float(score),
                })

    alignments.sort(key=lambda x: x['alignment_score'], reverse=True)
    return alignments

def main():
    print("Starting SEMANTIC alignment discovery for Executive Order 14240...")
    eo_json_path = "eo_14240_components.json"
    ivn_excel_path = "C:\\Users\\Basil.White\\OneDrive - USDA\\OCIO-STRATUS Governance Document Working Group - Documents\\USDA-IVN-dataset.xlsx"
    
    # Using a slightly lower threshold to catch more potential semantic links
    alignments = find_eo_alignments_semantic(eo_json_path, ivn_excel_path, similarity_threshold=0.4)
    
    print(f"Found {len(alignments)} potential alignments.")

    with open("eo_14240_alignments.json", "w") as f:
        json.dump(alignments, f, indent=2)

    print(f"Successfully saved {len(alignments)} potential alignments to eo_14240_alignments.json")

if __name__ == "__main__":
    main()
