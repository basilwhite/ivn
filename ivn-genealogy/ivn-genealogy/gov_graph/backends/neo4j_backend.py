"""Neo4j backend implementation."""
from neo4j import GraphDatabase
from typing import Any, List, Dict

class Neo4jBackend:
    """A wrapper for Neo4j database interactions."""

    def __init__(self, uri, user, password):
        """Initialize the Neo4j backend."""
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """Close the database connection."""
        self._driver.close()

    def run_cypher(self, query: str, params: dict = None) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query and return the results.
        """
        with self._driver.session() as session:
            result = session.run(query, params)
            return [record.data() for record in result]

    def load_cypher_file(self, path: str) -> None:
        """
        Load and execute a Cypher script from a file.
        The script is split by semicolons and executed as separate transactions.
        """
        with open(path, 'r') as f:
            queries = f.read().split(';')
            for query in queries:
                if query.strip():
                    self.run_cypher(query)

    def upsert_nodes(self, nodes: List[dict], label: str) -> None:
        """
        Upsert nodes into the graph. This is a pass-through for Kùzu compatibility.
        For Neo4j, data is loaded via Cypher scripts.
        """
        # This method is primarily for Kùzu.
        # For Neo4j, we use Cypher files for loading.
        # However, a basic implementation could look like this:
        query = f"""
        UNWIND $nodes AS node_data
        MERGE (n:{label} {{id: node_data.id}})
        SET n += node_data
        """
        self.run_cypher(query, params={'nodes': nodes})


    def upsert_edges(self, edges: List[dict], rel_type: str, from_label: str, to_label: str) -> None:
        """
        Upsert edges into the graph. This is a pass-through for Kùzu compatibility.
        For Neo4j, data is loaded via Cypher scripts.
        """
        # This method is primarily for Kùzu.
        query = f"""
        UNWIND $edges AS edge_data
        MATCH (from_node:{from_label} {{id: edge_data.from_id}})
        MATCH (to_node:{to_label} {{id: edge_data.to_id}})
        MERGE (from_node)-[r:{rel_type}]->(to_node)
        """
        self.run_cypher(query, params={'edges': edges})
