"""Loads OMB M-26-10 alignment data into the graph."""
import pandas as pd
import os
from gov_graph.config import GRAPH_BACKEND

def load(backend):
    """
    Loads M-26-10 alignment data from a CSV file.
    """
    if GRAPH_BACKEND == 'neo4j':
        cypher_path = os.path.join(os.path.dirname(__file__), '..', 'cypher', 'm2610_alignment.cypher')
        backend.load_cypher_file(cypher_path)
    elif GRAPH_BACKEND == 'kuzu':
        data_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'm2610_alignment.csv')
        alignment_df = pd.read_csv(data_path)

        # IMPLEMENTED_BY relationships
        edges = [{'from_id': row['memo_id'], 'to_id': row['directive_id']} for index, row in alignment_df.iterrows()]
        backend.upsert_edges(edges, 'IMPLEMENTED_BY_Doc', 'Document', 'Document')

        # CIO bindings
        cio_directives = ['DR3130-010', 'DR3145-001', 'DR3105-001']
        edges = [{'from_id': 'INST_USDA_CIO', 'to_id': did} for did in cio_directives]
        backend.upsert_edges(edges, 'BOUND_BY_Doc', 'Institution', 'Document')
