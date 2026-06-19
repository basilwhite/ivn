"""Loads USDA directive data into the graph."""
import pandas as pd
import os
from gov_graph.config import GRAPH_BACKEND

def load(backend):
    """
    Loads USDA directives and categories from CSV files.
    """
    data_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data')
    
    if GRAPH_BACKEND == 'neo4j':
        cypher_path = os.path.join(os.path.dirname(__file__), '..', 'cypher', 'usda_directives.cypher')
        backend.load_cypher_file(cypher_path)
    elif GRAPH_BACKEND == 'kuzu':
        # Load categories
        categories_df = pd.read_csv(os.path.join(data_path, 'directive_categories.csv'))
        backend.upsert_nodes(categories_df.to_dict('records'), 'DirectiveCategory')

        # Load directives
        directives_df = pd.read_csv(os.path.join(data_path, 'usda_directives.csv'))
        backend.upsert_nodes(directives_df.to_dict('records'), 'Document')

        # Create relationships
        # ISSUES
        edges = [{'from_id': 'INST_USDA', 'to_id': row['id']} for index, row in directives_df.iterrows()]
        backend.upsert_edges(edges, 'ISSUES', 'Institution', 'Document')

        # IN_CATEGORY
        edges = [{'from_id': row['id'], 'to_id': row['categoryCode']} for index, row in directives_df.iterrows() if pd.notna(row['categoryCode'])]
        backend.upsert_edges(edges, 'IN_CATEGORY', 'Document', 'DirectiveCategory')
