import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# This script compares all components against each other and outputs a large CSV. The script runs fast, so we can use it for the entire database.
# Load the IVN data
file_path = 'ivntest.xlsx'  # Update this to your file path if necessary
df = pd.read_excel(file_path)

# Fill NaN values with an empty string to avoid errors in vectorization
df = df.fillna('')

# Extract unique pairs of Enabling and Dependent Component Descriptions
unique_pairs = df[['Enabling Component Description', 'Dependent Component Description']].drop_duplicates()

# Vectorize the unique descriptions
vectorizer = TfidfVectorizer()
enabling_vectors = vectorizer.fit_transform(unique_pairs['Enabling Component Description'])
dependent_vectors = vectorizer.transform(unique_pairs['Dependent Component Description'])

# Calculate similarity scores between Enabling and Dependent descriptions
similarity_matrix = cosine_similarity(enabling_vectors, dependent_vectors)

# Convert the similarity matrix to a DataFrame for easy merging
similarity_df = pd.DataFrame(similarity_matrix, 
                             index=unique_pairs['Enabling Component Description'], 
                             columns=unique_pairs['Dependent Component Description']).reset_index()

# Reshape the similarity scores DataFrame for merging
similarity_df = similarity_df.melt(id_vars='Enabling Component Description', 
                                   var_name='Dependent Component Description', 
                                   value_name='Similarity Score')

# Filter pairs with similarity scores above a threshold value
similarity_df = similarity_df[similarity_df['Similarity Score'] > 0.00]

# Merge similarity scores back with the original DataFrame to include all 15 columns
output_df = df.merge(similarity_df, 
                     on=['Enabling Component Description', 'Dependent Component Description'], 
                     how='inner')

# Save the output to CSV
output_df.to_csv('ivn_similarity_scores_complete_above_threshold.csv', index=False)
print("Done!")
print("Alignments with similarity scores over threshold saved to ivn_similarity_scores_complete_above_threshold.csv")