MATCH (m2610:Document {id:'OMB_M-26-10'})
MATCH (dr3130_010:Document {id:'DR3130-010'})
MATCH (dr3145_001:Document {id:'DR3145-001'})
MATCH (dr3105_001:Document {id:'DR3105-001'})
MATCH (dr3600_002:Document {id:'DR3600-002'})
MATCH (dr3650_001:Document {id:'DR3650-001'})
CREATE (m2610)-[:IMPLEMENTED_BY_Doc]->(dr3130_010),
       (m2610)-[:IMPLEMENTED_BY_Doc]->(dr3145_001),
       (m2610)-[:IMPLEMENTED_BY_Doc]->(dr3105_001),
       (m2610)-[:IMPLEMENTED_BY_Doc]->(dr3600_002),
       (m2610)-[:IMPLEMENTED_BY_Doc]->(dr3650_001);

MATCH (usdaCio:Institution {id:'INST_USDA_CIO'})
WITH usdaCio
UNWIND ['DR3130-010','DR3145-001','DR3105-001'] AS did
MATCH (d:Document {id:did})
CREATE (usdaCio)-[:BOUND_BY_Doc]->(d);
