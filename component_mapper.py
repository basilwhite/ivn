# IMPORTANT: Keep this script in bidirectional sync with the external prompt file `component_mapper_prompt.txt`.
# After changing code, immediately update `component_mapper_prompt.txt` to reflect the new behavior.
# When `component_mapper_prompt.txt` changes, treat it as the authoritative spec and update this script to match.

# Create a Python script that performs fuzzy matching between components in two Excel sheets. The script should:
# 1. Load data from 'Dataset' and 'ToBeCrosswalked' sheets in 'ivntest.xlsx'
# 2. Validate file existence and sheet availability with appropriate error messages
# 3. Preprocess text by lowercasing, removing punctuation/stopwords, and normalizing whitespace
# 4. Normalize the Dataset sheet into enabling and dependent components with standardized column names
# 5. Use fuzzy string matching (fuzzywuzzy) to find the best matches between unmapped and existing components
# 6. Handle the correct return format from fuzzywuzzy.process.extract which returns (match, score) tuples
# 7. Calculate both name and description similarity scores with weighted composite scoring (60% name, 40% description)
# 8. For each unmapped component, find the top 3 matches with their suggested relationship type (Enabling/Dependent)
# 9. Apply minimum similarity thresholds (50) to filter out poor matches
# 10. Save results to a new 'Match_Suggestions' sheet while preserving the original workbook structure
# 11. Use only local libraries: pandas, fuzzywuzzy, re, os, openpyxl, time, collections (no API keys or online services)
# 12. Include comprehensive error handling for file operations and data validation
# 13. Optimize performance using efficient algorithms and add progress tracking for large datasets
# 14. Provide clear console feedback about progress and results with time estimates and percentage completion
# 15. Ensure the script handles edge cases like missing data, preprocessing errors, and empty results
# 16. Maintain code readability with clear comments and organized structure
# 17. Add performance optimizations for large datasets (10K+ rows) using index mapping dictionaries
# 18. Include timing metrics to track processing duration
# 19. Add progress indicators that update in real-time without newlines
# 20. Create efficient lookup structures (defaultdict) for faster text-to-index mapping
# 21. # - Results are always saved to a new file named 'component_mapper_suggestions-yyyymmddhhmmss.xlsx' (timestamped).
# 22. REMEMBER: Update this comprehensive prompt header whenever modifying the script below - 

import pandas as pd
from fuzzywuzzy import fuzz, process
import re
import os
import warnings
from openpyxl import load_workbook
from collections import defaultdict
import time

# Suppress fuzzywuzzy warnings
warnings.filterwarnings("ignore", message="Using slow pure-python SequenceMatcher.*")

# Define stop words for text preprocessing
STOP_WORDS = {'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were', 'this', 'that'}

def preprocess_text(text):
    """
    Clean and standardize text for optimal comparison.
    - Convert to lowercase
    - Remove punctuation
    - Remove extra whitespace
    - Remove common stop words
    """
    if pd.isna(text):
        return ""
    
    # Convert to lowercase and remove punctuation
    text = str(text).lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Remove stop words
    words = text.split()
    words = [word for word in words if word not in STOP_WORDS]
    
    return ' '.join(words)

def main():
    # File path
    file_path = 'ivntest.xlsx'
    
    # Validate file existence
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        return
    
    try:
        # Get all available sheet names
        excel_file = pd.ExcelFile(file_path)
        sheet_names = excel_file.sheet_names
        print(f"Available sheets in '{file_path}': {sheet_names}")
        
        # Check if required sheets exist
        required_sheets = ['Dataset', 'ToBeCrosswalked']
        missing_sheets = [sheet for sheet in required_sheets if sheet not in sheet_names]
        
        if missing_sheets:
            print(f"Error: The following required sheets are missing: {missing_sheets}")
            return
        
        # Load datasets
        print("Loading datasets...")
        df_dataset = pd.read_excel(file_path, sheet_name='Dataset', engine='openpyxl')
        df_tbc = pd.read_excel(file_path, sheet_name='ToBeCrosswalked', engine='openpyxl')
        
        print("Sheets loaded successfully.")
        print(f"Dataset shape: {df_dataset.shape}")
        print(f"ToBeCrosswalked shape: {df_tbc.shape}")
        
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        return
    
    # Data Preparation: Normalize the Dataset sheet
    print("Preprocessing data...")
    # Create enabling components dataframe
    df_enabling = df_dataset[['Enabling Component', 'Enabling Source', 'Enabling Component Description']].copy()
    df_enabling.columns = ['Component', 'Source', 'Description']
    df_enabling['Match_Type'] = 'Enabling Component'
    
    # Create dependent components dataframe
    df_dependent = df_dataset[['Dependent Component', 'Dependent Source', 'Dependent Component Description']].copy()
    df_dependent.columns = ['Component', 'Source', 'Description']
    df_dependent['Match_Type'] = 'Dependent Component'
    
    # Combine and remove duplicates
    df_mapped = pd.concat([df_enabling, df_dependent], ignore_index=True)
    df_mapped = df_mapped.drop_duplicates(subset=['Component', 'Source', 'Description']).reset_index(drop=True)
    
    print(f"Unique mapped components: {len(df_mapped)}")
    
    # Text Preprocessing
    df_mapped['processed_comp'] = df_mapped['Component'].apply(preprocess_text)
    df_mapped['processed_desc'] = df_mapped['Description'].apply(preprocess_text)
    
    df_tbc['processed_comp'] = df_tbc['Component'].apply(preprocess_text)
    df_tbc['processed_desc'] = df_tbc['Component Description'].apply(preprocess_text)
    
    # Prepare results list
    results = []
    
    # Create lookup dictionaries for faster processing
    mapped_components = list(df_mapped['processed_comp'])
    mapped_descriptions = list(df_mapped['processed_desc'])
    mapped_data = df_mapped.to_dict('records')
    
    # Create index mapping for faster lookup
    comp_to_indices = defaultdict(list)
    for idx, comp in enumerate(mapped_components):
        comp_to_indices[comp].append(idx)
    
    desc_to_indices = defaultdict(list)
    for idx, desc in enumerate(mapped_descriptions):
        desc_to_indices[desc].append(idx)
    
    # For each component in ToBeCrosswalked, find best matches in mapped components
    total_tbc = len(df_tbc)
    print(f"Starting fuzzy matching for {total_tbc} components...")
    print("This may take several minutes for large datasets.")
    print("Progress: 0%", end="", flush=True)
    
    start_time = time.time()
    
    for i, (_, tbc_row) in enumerate(df_tbc.iterrows()):
        tbc_comp = tbc_row['processed_comp']
        tbc_desc = tbc_row['processed_desc']
        
        # Show progress
        if i % max(1, total_tbc // 20) == 0:  # Update progress every ~5%
            progress = (i / total_tbc) * 100
            print(f"\rProgress: {progress:.1f}% ({i}/{total_tbc})", end="", flush=True)
        
        # Use process.extract for faster matching with fuzzywuzzy
        comp_matches = process.extract(tbc_comp, mapped_components, scorer=fuzz.token_set_ratio, limit=15)
        desc_matches = process.extract(tbc_desc, mapped_descriptions, scorer=fuzz.token_set_ratio, limit=15)
        
        # Create a dictionary to store the best scores for each mapped index
        match_scores = defaultdict(lambda: {'name_score': 0, 'desc_score': 0})
        
        # Process component matches
        for match_text, score in comp_matches:
            if match_text in comp_to_indices:
                for idx in comp_to_indices[match_text]:
                    if score > match_scores[idx]['name_score']:
                        match_scores[idx]['name_score'] = score
        
        # Process description matches
        for match_text, score in desc_matches:
            if match_text in desc_to_indices:
                for idx in desc_to_indices[match_text]:
                    if score > match_scores[idx]['desc_score']:
                        match_scores[idx]['desc_score'] = score
        
        # Calculate composite scores
        scored_matches = []
        for idx, scores in match_scores.items():
            if scores['name_score'] >= 50 or scores['desc_score'] >= 50:  # Minimum threshold
                composite_score = (scores['name_score'] * 0.6) + (scores['desc_score'] * 0.4)
                scored_matches.append({
                    'mapped_index': idx,
                    'name_score': scores['name_score'],
                    'desc_score': scores['desc_score'],
                    'composite_score': composite_score,
                    'match_type': mapped_data[idx]['Match_Type']
                })
        
        # Sort by composite score and get top 3 matches
        scored_matches.sort(key=lambda x: x['composite_score'], reverse=True)
        top_matches = scored_matches[:3]
        
        # Add top matches to results
        for match in top_matches:
            mapped_idx = match['mapped_index']
            mapped_data_row = mapped_data[mapped_idx]
            
            results.append({
                # ToBeCrosswalked data
                'TBC_Component': tbc_row['Component'],
                'TBC_Source': tbc_row['Source'],
                'TBC_Description': tbc_row['Component Description'],
                'TBC_URL': tbc_row['Component URL'],
                
                # Matched data
                'Matched_Component': mapped_data_row['Component'],
                'Matched_Source': mapped_data_row['Source'],
                'Matched_Description': mapped_data_row['Description'],
                
                # Match metadata
                'Match_Type': match['match_type'],
                'Composite_Score': match['composite_score'],
                'Name_Score': match['name_score'],
                'Description_Score': match['desc_score']
            })
    
    elapsed_time = time.time() - start_time
    print(f"\rProgress: 100% ({total_tbc}/{total_tbc}) - Completed in {elapsed_time:.1f} seconds")
    
    # Create results DataFrame
    df_results = pd.DataFrame(results)
    
    if len(df_results) == 0:
        print("No matches found meeting the similarity threshold.")
        return
    
    print(f"Generated {len(df_results)} match suggestions.")
    
    # Always save to a new file with timestamp
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_path = f'component_mapper_suggestions-{timestamp}.xlsx'
    try:
        print(f"Saving results to '{output_path}'...")
        df_results.to_excel(output_path, index=False)
        print(f"Results successfully saved to '{output_path}'.")
    except Exception as e:
        print(f"Failed to save results: {e}")

if __name__ == "__main__":
    main()
