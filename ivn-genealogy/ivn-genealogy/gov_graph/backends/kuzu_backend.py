"""Kùzu backend implementation."""
import kuzu
import os
from typing import Any, List, Dict

class KuzuBackend:
    """A wrapper for Kùzu database interactions."""

    def __init__(self, db_path: str):
        """Initialize the Kùzu backend."""
        if not os.path.exists(db_path):
            os.makedirs(db_path)
        self.db = kuzu.Database(db_path)
        self.conn = kuzu.Connection(self.db)
        self._initialize_schema()

    def _initialize_schema(self):
        """Create tables for the graph schema if they don't exist."""
        # Node tables
        self.conn.execute("CREATE TABLE IF NOT EXISTS Document (id STRING, name STRING, type STRING, number STRING, categoryCode STRING, date DATE, PRIMARY KEY (id))")
        self.conn.execute("CREATE TABLE IF NOT EXISTS Provision (id STRING, citation STRING, level STRING, heading STRING, text STRING, PRIMARY KEY (id))")
        self.conn.execute("CREATE TABLE IF NOT EXISTS Institution (id STRING, name STRING, kind STRING, PRIMARY KEY (id))")
        self.conn.execute("CREATE TABLE IF NOT EXISTS DirectiveCategory (code STRING, name STRING, PRIMARY KEY (code))")

        # Edge tables (Relationships)
        self.conn.execute("CREATE TABLE IF NOT EXISTS PARENT_OF (FROM Document TO Document)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS PARENT_OF_Provision (FROM Provision TO Provision)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS ANCESTOR_OF (FROM Document TO Document)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS SIBLING_OF (FROM Document TO Document)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS AUTHORIZES (FROM Provision TO Institution)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS ENACTS (FROM Institution TO Provision)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS DERIVES_FROM (FROM Document TO Provision)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS DERIVES_FROM_Provision (FROM Provision TO Provision)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS IMPLEMENTED_BY (FROM Provision TO Document)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS IMPLEMENTED_BY_Doc (FROM Document TO Document)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS INTERPRETED_BY (FROM Provision TO Document)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS LIMITED_BY (FROM Provision TO Document)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS BOUND_BY (FROM Institution TO Provision)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS APPLIES_TO (FROM Document TO Institution)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS DIRECTS (FROM Document TO Institution)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS ISSUES (FROM Institution TO Document)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS IN_CATEGORY (FROM Document TO DirectiveCategory)")


    def close(self):
        """Kùzu doesn't require an explicit close for connections."""
        pass

    def run_cypher(self, query: str, params: dict = None) -> List[Dict[str, Any]]:
        """Execute a Cypher query and return the results."""
        prepared_statement = self.conn.prepare(query, params)
        result = self.conn.execute(prepared_statement)
        
        column_names = result.get_column_names()
        return [dict(zip(column_names, row)) for row in result.get_as_arrow(result.get_num_tuples()).to_pylist()]

    def load_cypher_file(self, path: str) -> None:
        """Load and execute a Cypher script from a file."""
        with open(path, 'r') as f:
            queries = f.read().split(';')
            for query in queries:
                if query.strip():
                    self.run_cypher(query)

    def upsert_nodes(self, nodes: List[dict], label: str) -> None:
        """Upsert nodes into the graph using direct insertion."""
        # Kùzu doesn't have a direct MERGE. We'll emulate it.
        # This is a simplified example. A robust solution would handle existing nodes.
        for node in nodes:
            # Check if node exists
            check_query = f"MATCH (n:{label} {{id: $id}}) RETURN n.id"
            result = self.run_cypher(check_query, params={ 'id': node['id']})
            
            if not result:
                # Insert if not exists
                properties = ", ".join([f"{k}: ${k}" for k in node.keys()])
                insert_query = f"CREATE (n:{label} {{{properties}}})"
                self.run_cypher(insert_query, params=node)

    def upsert_edges(self, edges: List[dict], rel_type: str, from_label: str, to_label: str) -> None:
        """Upsert edges into the graph."""
        for edge in edges:
            query = f"""
            MATCH (a:{from_label} {{id: $from_id}}), (b:{to_label} {{id: $to_id}})
            CREATE (a)-[:{rel_type}]->(b)
            """
            self.run_cypher(query, params=edge)
