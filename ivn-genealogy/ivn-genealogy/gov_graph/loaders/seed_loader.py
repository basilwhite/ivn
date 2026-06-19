"""Orchestrates the loading of all data into the graph."""
from gov_graph.config import get_backend, GRAPH_BACKEND
from . import usda_loader, m2610_loader, retirement_loader
import os

def load_all(backend_override=None):
    """
    Loads all data sources into the configured graph backend.
    """
    backend = get_backend()
    
    # Base data
    print("Loading base graph...")
    base_cypher_path = os.path.join(os.path.dirname(__file__), '..', 'cypher', 'base.cypher')
    backend.load_cypher_file(base_cypher_path)

    # Retirement data
    print("Loading retirement data...")
    retirement_loader.load(backend)

    # USDA Directives
    print("Loading USDA directives...")
    usda_loader.load(backend)

    # M-26-10 Alignment
    print("Loading M-26-10 alignment...")
    m2610_loader.load(backend)
    
    print("All data loaded successfully.")

if __name__ == '__main__':
    load_all()
