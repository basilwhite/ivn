"""Tests for the query packs."""
from gov_graph.queries import ancestry, compliance, impact, heatmap

def test_lineage_m2610(test_backend):
    """
    Verify that the M-26-10 lineage query returns the expected directives.
    """
    results = ancestry.full_lineage_memo_to_usda()
    directives = {r['usda_directive'] for r in results}
    
    expected_directives = {
        'United States Department of Agriculture Enterprise Information Technology Governance',
        'Oversight and Management of the Federal Information Technology Acquisition Reform Act (FITARA)',
        'USDA Chief Information Officers Council',
        'Electronic-Government Program',
        'Cloud Computing'
    }
    assert directives.issuperset(expected_directives)

def test_cio_compliance_surface(test_backend):
    """
    Verify that the CIO compliance surface includes the correct directives.
    """
    results = compliance.cio_compliance_surface()
    directive_numbers = {r['directive_number'] for r in results}

    expected_directives = {'DR3130-010', 'DR3145-001', 'DR3105-001'}
    assert directive_numbers == expected_directives

def test_heatmap_ranking(test_backend):
    """
    Verify that the heatmap ranks directly implementing directives higher.
    """
    results = heatmap.get_m2610_influence_heatmap()
    
    direct_implementers = {r['directive'] for r in results if r['influence_type'] == 'Direct'}
    
    assert 'DR3130-010' in direct_implementers
    assert 'DR3145-001' in direct_implementers
    
    scores = {r['directive']: r['influence_score'] for r in results}
    assert scores['DR3130-010'] > scores.get('DR3111-001', 0) # Example of a non-direct
