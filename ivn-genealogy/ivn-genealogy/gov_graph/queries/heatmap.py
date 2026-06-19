"""Queries for generating a heatmap of influence."""
from gov_graph.config import get_backend

def get_m2610_influence_heatmap():
    """
    Calculates an influence score for USDA directives related to OMB M-26-10.
    - Score +2 if directly implementing the memo.
    - Score +1 if in a related category.
    """
    backend = get_backend()
    
    # This query is complex and demonstrates the logic.
    # It might be slow on large graphs and could be optimized.
    query = """
    // Find all USDA directives
    MATCH (d:Document)
    WHERE d.type = 'USDA_Directive'

    // Calculate influence score
    WITH d,
         // Score for direct implementation
         CASE WHEN (d)<-[:IMPLEMENTED_BY_Doc]-(:Document {id: 'OMB_M-26-10'}) THEN 2 ELSE 0 END AS direct_score,
         // Score for category relevance
         CASE WHEN (d)-[:IN_CATEGORY]->(:DirectiveCategory) WHERE d.categoryCode IN ['3100', '3130', '3000-3900'] THEN 1 ELSE 0 END AS category_score

    // Combine scores
    WITH d, (direct_score + category_score) AS influence_score
    WHERE influence_score > 0

    RETURN d.number AS directive,
           d.name AS name,
           influence_score,
           CASE WHEN direct_score > 0 THEN 'Direct' ELSE 'Category' END AS influence_type
    ORDER BY influence_score DESC, directive
    """
    return backend.run_cypher(query)
