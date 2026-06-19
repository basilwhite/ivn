"""Queries for tracing ancestry and lineage."""
from gov_graph.config import get_backend

def trace_derivation_from_uscode(provision_id: str):
    """
    Traces the derivation of a provision from a US Code section.
    """
    backend = get_backend()
    query = """
    MATCH (p:Provision {id: $provision_id})<-[:PARENT_OF_Provision*]-(usc:Provision)
    WHERE usc.id STARTS WITH 'USC_'
    RETURN p.citation AS provision, usc.citation AS derived_from_usc
    """
    return backend.run_cypher(query, {'provision_id': provision_id})

def constitutional_ancestors(provision_id: str):
    """
    Finds the constitutional ancestors of a given provision.
    """
    backend = get_backend()
    query = """
    MATCH (p:Provision {id: $provision_id})-[:DERIVES_FROM_Provision|PARENT_OF_Provision*]->(ancestor:Provision)
    WHERE ancestor.id STARTS WITH 'CONST_'
    RETURN p.citation AS provision, ancestor.citation AS ancestor
    """
    return backend.run_cypher(query, {'provision_id': provision_id})

def full_lineage_memo_to_usda():
    """
    Shows the full lineage from Constitution to USDA directives via OMB M-26-10.
    """
    backend = get_backend()
    query = """
    MATCH path = (const:Provision)<-[:DERIVES_FROM_Provision*]-(statute:Provision)<-[:DERIVES_FROM]-(memo:Document {id: 'OMB_M-26-10'})-[:IMPLEMENTED_BY_Doc]->(directive:Document)
    WHERE const.id STARTS WITH 'CONST_'
    RETURN const.citation AS constitutional_basis,
           statute.citation AS statute,
           memo.name AS memo,
           directive.name AS usda_directive
    UNION
    MATCH path = (eo:Document)<-[:DERIVES_FROM_Doc]-(memo:Document {id: 'OMB_M-26-10'})-[:IMPLEMENTED_BY_Doc]->(directive:Document)
    RETURN eo.name AS constitutional_basis,
           'Executive Order' AS statute,
           memo.name AS memo,
           directive.name AS usda_directive
    """
    return backend.run_cypher(query)
