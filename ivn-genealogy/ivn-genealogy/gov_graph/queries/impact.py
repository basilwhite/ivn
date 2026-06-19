"""Queries for analyzing the impact of regulations."""
from gov_graph.config import get_backend

def cio_m2610_impact_paths():
    """
    Traces the full path from constitutional basis to directives binding the CIO,
    related to OMB M-26-10.
    """
    backend = get_backend()
    query = """
    MATCH path = (c:Provision)<-[:DERIVES_FROM*]-(m:Document {id:'OMB_M-26-10'})-[:IMPLEMENTED_BY_Doc]->(d:Document)<-[:BOUND_BY_Doc]-(cio:Institution {id:'INST_USDA_CIO'})
    WHERE c.id STARTS WITH 'CONST_'
    WITH nodes(path) as path_nodes
    UNWIND range(0, size(path_nodes)-2) as i
    WITH path_nodes[i] as source, path_nodes[i+1] as target
    RETURN DISTINCT source.id as source_id, labels(source)[0] as source_label, source.name as source_name,
           target.id as target_id, labels(target)[0] as target_label, target.name as target_name
    """
    # This is a complex query and might need adjustments based on graph structure.
    # A simpler path query:
    query_simple = """
        MATCH path = (c:Provision)<-[*]-(m:Document {id:'OMB_M-26-10'})-[*]->(cio:Institution {id:'INST_USDA_CIO'})
        WHERE c.id STARTS WITH 'CONST_'
        RETURN path
    """
    return backend.run_cypher(query_simple)


def institutions_sharing_obligations(inst_a_id: str, inst_b_id: str):
    """
    Finds provisions that bind both specified institutions.
    """
    backend = get_backend()
    query = """
    MATCH (a:Institution {id: $inst_a_id})-[:BOUND_BY]->(p:Provision)<-[:BOUND_BY]-(b:Institution {id: $inst_b_id})
    RETURN p.citation AS shared_obligation, p.heading AS heading
    """
    return backend.run_cypher(query, {'inst_a_id': inst_a_id, 'inst_b_id': inst_b_id})
