import pandas as pd
import networkx as nx
from collections import defaultdict

def find_first_cousins(excel_file, min_common_targets=3):
    """
    Find first cousin relationships in IVN dataset
    
    Args:
        excel_file (str): Path to Excel file with Alignments and Components sheets
        min_common_targets (int): Minimum number of common targets to consider
    
    Returns:
        DataFrame: Results of first cousin analysis
    """
    # Step 1: Load data from Excel file
    print("Loading data from Excel file...")
    alignments_df = pd.read_excel(excel_file, sheet_name='Alignments')
    components_df = pd.read_excel(excel_file, sheet_name='Components')
    print("Alignments columns:", alignments_df.columns.tolist())  # Add this line
    
    # Step 2: Create a directed graph to represent component alignments
    print("Creating alignment graph...")
    G = nx.DiGraph()
    
    # Add edges to the graph (use correct column names)
    for _, row in alignments_df.iterrows():
        G.add_edge(row['Enabling Component'], row['Dependent Component'])
    
    # Step 3: Create a mapping of each component to its alignment targets
    print("Mapping components to their alignment targets...")
    targets_map = {}
    for component in G.nodes():
        targets_map[component] = set(G.successors(component))
    
    # Step 4: Identify candidate pairs with no direct alignment but common targets
    print("Finding candidate pairs with common alignment targets...")
    candidate_pairs = []
    common_targets_dict = {}
    
    # Get list of all components
    components = list(G.nodes())
    
    # Compare all pairs of components
    for i, comp1 in enumerate(components):
        for j, comp2 in enumerate(components[i+1:], i+1):
            # Skip if direct alignment exists in either direction
            if G.has_edge(comp1, comp2) or G.has_edge(comp2, comp1):
                continue
                
            # Find common targets (components aligned to both comp1 and comp2)
            common_targets = targets_map[comp1] & targets_map[comp2]
            
            # Check if meets the minimum common targets threshold
            if len(common_targets) >= min_common_targets:
                candidate_pairs.append((comp1, comp2))
                common_targets_dict[(comp1, comp2)] = common_targets
    
    # Step 5: Analyze each candidate pair in detail
    print("Analyzing candidate pairs...")
    results = []
    for comp1, comp2 in candidate_pairs:
        # Identify targets exclusive to each component
        exclusive_to_comp1 = targets_map[comp1] - targets_map[comp2]
        exclusive_to_comp2 = targets_map[comp2] - targets_map[comp1]
        
        # Count common targets
        common_targets_count = len(common_targets_dict[(comp1, comp2)])
        
        # Compile results for this pair
        results.append({
            'Component_A': comp1,
            'Component_B': comp2,
            'Common_Targets_Count': common_targets_count,
            'Common_Targets': common_targets_dict[(comp1, comp2)],
            'Exclusive_to_A_Count': len(exclusive_to_comp1),
            'Exclusive_to_A': exclusive_to_comp1,
            'Exclusive_to_B_Count': len(exclusive_to_comp2),
            'Exclusive_to_B': exclusive_to_comp2
        })
    
    return pd.DataFrame(results)

# Main execution
if __name__ == "__main__":
    # Configuration parameters
    excel_file = 'ivntest.xlsx'  # Path to the Excel file
    min_common_targets = 3       # Minimum number of common targets to consider
    
    print("Starting first cousin analysis...")
    print(f"Using minimum common targets threshold: {min_common_targets}")
    
    # Step 6: Execute the analysis
    results_df = find_first_cousins(excel_file, min_common_targets)
    
    # Step 7: Save results to CSV file
    print("Saving results to CSV file...")
    results_df.to_csv('first_cousins_analysis.csv', index=False)
    
    # Step 8: Print summary of findings
    print("\nAnalysis Complete!")
    print(f"Found {len(results_df)} first cousin pairs with {min_common_targets}+ common targets")
    
    if len(results_df) > 0:
        print("\nTop pairs by number of common targets:")
        top_pairs = results_df.nlargest(5, 'Common_Targets_Count')
        for _, row in top_pairs.iterrows():
            print(f"  {row['Component_A']} & {row['Component_B']}: {row['Common_Targets_Count']} common targets")
    else:
        print("No first cousin relationships found with the current threshold.")
    
    print("\nDetailed results saved to 'first_cousins_analysis.csv'")

