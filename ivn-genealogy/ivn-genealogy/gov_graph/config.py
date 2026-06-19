"""Configuration module for the application."""
import os
from dotenv import load_dotenv
from typing import Literal, cast

# Load environment variables from .env file
load_dotenv()

# --- Environment Variables ---
GRAPH_BACKEND = cast(Literal['neo4j', 'kuzu'], os.getenv('GRAPH_BACKEND', 'neo4j'))
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'password')
KUZU_DB_PATH = os.getenv('KUZU_DB_PATH', './kuzu_db')

# --- Backend Factory ---
_backend_instance = None

def get_backend():
    """
    Factory function to get the appropriate graph backend instance.
    Returns a singleton instance of the configured backend.
    """
    global _backend_instance
    if _backend_instance is None:
        if GRAPH_BACKEND == 'neo4j':
            from gov_graph.backends.neo4j_backend import Neo4jBackend
            _backend_instance = Neo4jBackend(
                uri=NEO4J_URI,
                user=NEO4J_USER,
                password=NEO4J_PASSWORD
            )
        elif GRAPH_BACKEND == 'kuzu':
            from gov_graph.backends.kuzu_backend import KuzuBackend
            _backend_instance = KuzuBackend(db_path=KUZU_DB_PATH)
        else:
            raise ValueError(f"Unsupported backend: {GRAPH_BACKEND}")
    return _backend_instance
