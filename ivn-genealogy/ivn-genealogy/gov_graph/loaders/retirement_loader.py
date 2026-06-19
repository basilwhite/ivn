"""Loads retirement-related data into the graph."""
import pandas as pd
import os
from gov_graph.config import GRAPH_BACKEND

def load(backend):
    """
    Loads retirement data from Cypher script or CSV.
    """
    if GRAPH_BACKEND == 'neo4j':
        cypher_path = os.path.join(os.path.dirname(__file__), '..', 'cypher', 'retirement.cypher')
        backend.load_cypher_file(cypher_path)
    elif GRAPH_BACKEND == 'kuzu':
        data_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'sample_retirement.csv')
        retirement_df = pd.read_csv(data_path)
        
        nodes = retirement_df.to_dict('records')
        backend.upsert_nodes(nodes, 'Provision')

        # Manually add relationships from retirement.cypher
        edges = [
            {'from_id': 'USC_T5', 'to_id': 'USC_T5_CH83'},
            {'from_id': 'USC_T5', 'to_id': 'USC_T5_CH84'},
            {'from_id': 'USC_T5_CH83', 'to_id': 'USC_8336'},
            {'from_id': 'USC_T5_CH84', 'to_id': 'USC_8412'},
            {'from_id': 'USC_T5_CH84', 'to_id': 'USC_8414'},
            {'from_id': 'USC_T5_CH84', 'to_id': 'USC_8461'},
        ]
        backend.upsert_edges(edges, 'PARENT_OF_Provision', 'Provision', 'Provision')

        edges = [
            {'from_id': 'USC_8336', 'to_id': 'CFR_5_831'},
            {'from_id': 'USC_8412', 'to_id': 'CFR_5_842'},
            {'from_id': 'USC_8414', 'to_id': 'CFR_5_842'},
            {'from_id': 'USC_8461', 'to_id': 'CFR_5_841'},
            {'from_id': 'USC_8461', 'to_id': 'CFR_5_846'},
        ]
        backend.upsert_edges(edges, 'IMPLEMENTED_BY_Provision', 'Provision', 'Provision')
        
        # Bind institutions
        provisions_to_bind = ['USC_8336','USC_8412','USC_8414','USC_8461','CFR_5_831','CFR_5_841','CFR_5_842','CFR_5_846']
        opm_edges = [{'from_id': 'INST_OPM', 'to_id': pid} for pid in provisions_to_bind]
        usda_edges = [{'from_id': 'INST_USDA', 'to_id': pid} for pid in provisions_to_bind]
        backend.upsert_edges(opm_edges, 'BOUND_BY', 'Institution', 'Provision')
        backend.upsert_edges(usda_edges, 'BOUND_BY', 'Institution', 'Provision')

        # Constitutional ancestry
        ancestry_edges = [
            {'from_id': 'USC_8336', 'to_id': 'CONST_ART1_S8_CL1'},
            {'from_id': 'USC_8412', 'to_id': 'CONST_ART1_S8_CL1'},
            {'from_id': 'USC_8414', 'to_id': 'CONST_ART1_S8_CL1'},
            {'from_id': 'USC_8461', 'to_id': 'CONST_ART1_S8_CL1'},
            {'from_id': 'USC_8412', 'to_id': 'CONST_ART1_S8_CL3'},
            {'from_id': 'USC_8414', 'to_id': 'CONST_ART1_S8_CL3'},
        ]
        backend.upsert_edges(ancestry_edges, 'DERIVES_FROM_Provision', 'Provision', 'Provision')

        # Case law
        backend.upsert_nodes([{'id':'CASE_MUNN', 'name':'Munn v. Illinois, 94 U.S. 113 (1877)', 'type':'Case'}], 'Document')
        backend.upsert_edges([{'from_id': 'CONST_ART1_S8_CL3', 'to_id': 'CASE_MUNN'}], 'INTERPRETED_BY', 'Provision', 'Document')
