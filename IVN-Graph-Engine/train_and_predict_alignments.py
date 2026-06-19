import pandas as pd
import json
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
import numpy as np

def train_alignment_model(training_file_path):
    """
    Trains a machine learning model to classify if a pair of components is aligned,
    based on the provided data dictionary.
    """
    print("Loading training data...")
    try:
        # Load positive examples (aligned)
        aligned_df = pd.read_excel(training_file_path, sheet_name='Alignments')
        aligned_df['label'] = 1
        
        # Load negative examples (not aligned)
        nonaligned_df = pd.read_excel(training_file_path, sheet_name='Nonaligned-Edge-Cases')
        nonaligned_df['label'] = 0
        
        training_df = pd.concat([aligned_df, nonaligned_df], ignore_index=True)
    except FileNotFoundError:
        print(f"Error: Training file not found at {training_file_path}")
        return None, None
    except Exception as e:
        print(f"Error reading training data from Excel file: {e}")
        return None, None

    # Normalize column names based on the data dictionary
    training_df.columns = [col.strip().lower().replace(' ', '_') for col in training_df.columns]
    
    # Use the correct column names from the data dictionary
    enabling_col = 'enabling_component_description'
    dependent_col = 'dependent_component_description'
    
    if enabling_col not in training_df.columns or dependent_col not in training_df.columns:
        print(f"Error: Expected columns '{enabling_col}' and '{dependent_col}' not found in training sheets.")
        print(f"Available columns: {training_df.columns.tolist()}")
        return None, None

    # Combine the text from both components into a single string for vectorization
    training_df['combined_text'] = training_df[enabling_col].fillna('') + ' ' + training_df[dependent_col].fillna('')
    
    print("Training TF-IDF Vectorizer and Logistic Regression model...")
    vectorizer = TfidfVectorizer(stop_words='english', max_features=10000, ngram_range=(1, 2))
    X_train = vectorizer.fit_transform(training_df['combined_text'])
    y_train = training_df['label']
    
    model = LogisticRegression(class_weight='balanced', C=1.0, solver='liblinear') # Fine-tuned model
    model.fit(X_train, y_train)
    
    print("Model training complete.")
    return model, vectorizer

def predict_alignments(model, vectorizer, eo_components_path, ivn_excel_path):
    """
    Uses the trained model to predict alignments between EO 14240 and the IVN database.
    """
    print("Loading data for prediction...")
    with open(eo_components_path, 'r') as f:
        eo_data = json.load(f)
        
    try:
        # Per data dictionary, the components to be crosswalked are in the 'Components' tab
        ivn_df = pd.read_excel(ivn_excel_path, sheet_name='Components') 
    except Exception as e:
        print(f"Error reading IVN 'Components' sheet for prediction: {e}")
        return []

    # Prepare EO data
    eo_components = []
    for section, comps in eo_data.items():
        for comp in comps:
            eo_components.append({
                "section": section,
                "component_obj": comp,
                "text": comp.get('description', '')
            })

    # Prepare IVN data from 'Components' tab
    ivn_df.columns = [col.strip().lower().replace(' ', '_') for col in ivn_df.columns]
    if 'component_name' not in ivn_df.columns or 'component_description' not in ivn_df.columns:
        print("Error: 'component_name' or 'component_description' not found in the 'Components' tab.")
        return []

    ivn_df['full_text'] = ivn_df['component_name'].fillna('') + ' ' + ivn_df['component_description'].fillna('')

    print(f"Predicting alignments for {len(eo_components)} EO components against {len(ivn_df)} IVN components...")
    alignments = []
    for eo_comp in eo_components:
        if not eo_comp['text']: continue # Skip EO components with no description
        for _, ivn_row in ivn_df.iterrows():
            combined_text = [eo_comp['text'] + ' ' + ivn_row['full_text']]
            X_pred = vectorizer.transform(combined_text)
            
            probability = model.predict_proba(X_pred)[0][1]
            
            if probability > 0.7: # Using a higher confidence threshold for better precision
                alignments.append({
                    "eo_14240_section": eo_comp["section"],
                    "eo_14240_component": eo_comp["component_obj"],
                    "ivn_component": ivn_row.get('component_name'),
                    "ivn_description": ivn_row.get('component_description'),
                    "alignment_score": float(probability)
                })

    alignments.sort(key=lambda x: x['alignment_score'], reverse=True)
    return alignments

def main():
    training_file = "C:\\Users\\Basil.White\\OneDrive - USDA\\OCIO-STRATUS Governance Document Working Group - Documents\\USDA-IVN-dataset.xlsx"
    eo_json_path = "eo_14240_components.json"
    
    model, vectorizer = train_alignment_model(training_file)
    
    if model and vectorizer:
        predicted_alignments = predict_alignments(model, vectorizer, eo_json_path, training_file)
        print(f"Found {len(predicted_alignments)} potential alignments using the ML model.")
        
        output_filename = "eo_14240_alignments_ml.json"
        with open(output_filename, "w") as f:
            json.dump(predicted_alignments, f, indent=2)
            
        print(f"Successfully saved ML-based alignments to {output_filename}")
        print("\nNew output files generated:")
        print(f"- {output_filename}")


if __name__ == "__main__":
    main()
