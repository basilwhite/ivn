"""Pytest configuration and fixtures."""
import pytest
import os
import shutil
from gov_graph.config import get_backend
from gov_graph.loaders import seed_loader

@pytest.fixture(scope="session")
def test_backend():
    """
    Provides a test backend instance (Kùzu for local testing).
    This fixture will set up a temporary Kùzu database, load it with data,
    yield the backend instance for tests, and then clean up.
    """
    # Use Kùzu for testing to avoid Docker dependency
    os.environ['GRAPH_BACKEND'] = 'kuzu'
    test_db_path = './test_kuzu_db'
    os.environ['KUZU_DB_PATH'] = test_db_path

    # Clean up previous test runs
    if os.path.exists(test_db_path):
        shutil.rmtree(test_db_path)

    # Get backend and load data
    backend = get_backend()
    seed_loader.load_all()
    
    yield backend

    # Teardown: clean up the database directory
    backend.close()
    if os.path.exists(test_db_path):
        shutil.rmtree(test_db_path)
