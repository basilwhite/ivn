-- Directive categories
CREATE (catIRM:DirectiveCategory {code:'3000-3900', name:'Information Resources Management'});
CREATE (cat3100:DirectiveCategory {code:'3100', name:'Management of Information Resources'});
CREATE (cat3130:DirectiveCategory {code:'3130', name:'Information Technology Investments and Governance'});

-- USDA directives
CREATE (dr3105_001:Document {id:'DR3105-001', name:'USDA Chief Information Officers Council', type:'USDA_Directive', number:'DR3105-001', categoryCode:'3100', date:date('2024-10-23')});
CREATE (dr3107_001:Document {id:'DR3107-001', name:'Management of USDA IT Enterprise Initiatives', type:'USDA_Directive', number:'DR3107-001', categoryCode:'3100', date:date('2016-05-12')});
CREATE (dr3111_001:Document {id:'DR3111-001', name:'USDA Information Technology Strategic Plan Process', type:'USDA_Directive', number:'DR3111-001', categoryCode:'3100', date:date('2021-06-30')});
CREATE (dr3130_010:Document {id:'DR3130-010', name:'United States Department of Agriculture Enterprise Information Technology Governance', type:'USDA_Directive', number:'DR3130-010', categoryCode:'3130', date:date('2021-04-20')});
CREATE (dr3145_001:Document {id:'DR3145-001', name:'Oversight and Management of the Federal Information Technology Acquisition Reform Act (FITARA)', type:'USDA_Directive', number:'DR3145-001', categoryCode:'3130', date:date('2021-05-07')});
CREATE (dr3600_002:Document {id:'DR3600-002', name:'Electronic-Government Program', type:'USDA_Directive', number:'DR3600-002', categoryCode:'3600', date:date('2020-11-24')});
CREATE (dr3650_001:Document {id:'DR3650-001', name:'Cloud Computing', type:'USDA_Directive', number:'DR3650-001', categoryCode:'3650', date:date('2025-01-22')});

-- Issuance and categorization
MATCH (usda:Institution {id:'INST_USDA'})
WITH usda
UNWIND ['DR3105-001','DR3107-001','DR3111-001','DR3130-010','DR3145-001','DR3600-002','DR3650-001'] AS did
MATCH (d:Document {id:did})
CREATE (usda)-[:ISSUES]->(d);

MATCH (dr3105_001:Document {id:'DR3105-001'}), (cat3100:DirectiveCategory {code:'3100'}) CREATE (dr3105_001)-[:IN_CATEGORY]->(cat3100);
MATCH (dr3107_001:Document {id:'DR3107-001'}), (cat3100:DirectiveCategory {code:'3100'}) CREATE (dr3107_001)-[:IN_CATEGORY]->(cat3100);
MATCH (dr3111_001:Document {id:'DR3111-001'}), (cat3100:DirectiveCategory {code:'3100'}) CREATE (dr3111_001)-[:IN_CATEGORY]->(cat3100);
MATCH (dr3130_010:Document {id:'DR3130-010'}), (cat3130:DirectiveCategory {code:'3130'}) CREATE (dr3130_010)-[:IN_CATEGORY]->(cat3130);
MATCH (dr3145_001:Document {id:'DR3145-001'}), (cat3130:DirectiveCategory {code:'3130'}) CREATE (dr3145_001)-[:IN_CATEGORY]->(cat3130);
MATCH (dr3600_002:Document {id:'DR3600-002'}), (catIRM:DirectiveCategory {code:'3000-3900'}) CREATE (dr3600_002)-[:IN_CATEGORY]->(catIRM);
MATCH (dr3650_001:Document {id:'DR3650-001'}), (catIRM:DirectiveCategory {code:'3000-3900'}) CREATE (dr3650_001)-[:IN_CATEGORY]->(catIRM);
