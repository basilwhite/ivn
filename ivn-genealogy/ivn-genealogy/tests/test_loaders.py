"""Tests for the data loaders."""

def test_load_all(test_backend):
    """
    Tests that the load_all function completes and loads some data.
    """
    # The fixture already loads the data, so we just need to assert counts.
    
    # Check for USDA directives
    directives_count = test_backend.run_cypher("MATCH (d:Document) WHERE d.type = 'USDA_Directive' RETURN count(d) as count")[0]['count']
    assert directives_count >= 7

    # Check for M-26-10 memo
    memo_count = test_backend.run_cypher("MATCH (m:Document {id: 'OMB_M-26-10'}) RETURN count(m) as count")[0]['count']
    assert memo_count == 1

    # Check for memo to directive relationships
    memo_edges = test_backend.run_cypher("MATCH (:Document {id: 'OMB_M-26-10'})-[:IMPLEMENTED_BY_Doc]->(:Document) RETURN count(*) as count")[0]['count']
    assert memo_edges >= 5
