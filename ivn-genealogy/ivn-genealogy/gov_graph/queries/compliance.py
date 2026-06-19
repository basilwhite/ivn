"""Queries for assessing compliance surfaces."""
from gov_graph.config import get_backend

def usda_bound_provisions():
    """
    Lists all USC and CFR provisions that bind USDA.
    """
    backend = get_backend()
    query = """
    MATCH (usda:Institution {id: 'INST_USDA'})-[:BOUND_BY]->(p:Provision)
    WHERE p.id STARTS WITH 'USC_' OR p.id STARTS WITH 'CFR_'
    RETURN p.citation AS provision, p.heading AS heading
    """
    return backend.run_cypher(query)

def usda_directives():
    """
    Lists all directives issued by USDA.
    """
    backend = get_backend()
    query = """
    MATCH (usda:Institution {id: 'INST_USDA'})-[:ISSUES]->(d:Document)
    WHERE d.type = 'USDA_Directive'
    RETURN d.number AS directive_number, d.name AS name, d.date AS date
    ORDER BY d.date DESC
    """
    return backend.run_cypher(query)

def usda_directives_implementing_memo():
    """
    Finds USDA directives that implement OMB M-26-10.
    """
    backend = get_backend()
    query = """
    MATCH (memo:Document {id: 'OMB_M-26-10'})-[:IMPLEMENTED_BY_Doc]->(d:Document)
    WHERE d.type = 'USDA_Directive'
    RETURN d.number AS directive_number, d.name AS name
    """
    return backend.run_cypher(query)

def cio_compliance_surface():
    """
    Lists all directives that the USDA CIO is bound by.
    """
    backend = get_backend()
    query = """
    MATCH (cio:Institution {id: 'INST_USDA_CIO'})-[:BOUND_BY_Doc]->(d:Document)
    RETURN d.number AS directive_number, d.name AS name
    """
    return backend.run_cypher(query)
