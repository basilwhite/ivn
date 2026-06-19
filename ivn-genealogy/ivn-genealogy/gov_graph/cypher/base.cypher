-- RESET
MATCH (n) DETACH DELETE n;

-- === Constitution slice ===
CREATE (taxClause:Provision {id:'CONST_ART1_S8_CL1', citation:'Art. I, §8, cl.1', level:'Clause', heading:'Taxing and Spending'});
CREATE (commerceClause:Provision {id:'CONST_ART1_S8_CL3', citation:'Art. I, §8, cl.3', level:'Clause', heading:'Commerce Clause'});

-- === Statutes referenced by M-26-10 ===
CREATE (cfoAct:Provision {id:'31_USC_901B', citation:'31 U.S.C. §901(b)', level:'USC_Section', heading:'CFO Act – covered agencies'});
CREATE (paperworkDef:Provision {id:'44_USC_3502_1', citation:'44 U.S.C. §3502(1)', level:'USC_Section', heading:'Definition of agency'});
CREATE (nssDef:Provision {id:'40_USC_11103_A', citation:'40 U.S.C. §11103(a)', level:'USC_Section', heading:'National security systems'});

-- Statute → Clause (analytic)
CREATE (cfoAct)-[:DERIVES_FROM_Provision]->(taxClause);
CREATE (paperworkDef)-[:DERIVES_FROM_Provision]->(taxClause);
CREATE (nssDef)-[:DERIVES_FROM_Provision]->(commerceClause);

-- === EOs ===
CREATE (eo13833:Document {id:'EO_13833', name:'Enhancing the Effectiveness of Agency Chief Information Officers', type:'ExecutiveOrder'});
CREATE (eo14240:Document {id:'EO_14240', name:'Eliminating Waste and Saving Taxpayer Dollars by Consolidating Procurement', type:'ExecutiveOrder'});
CREATE (eo14243:Document {id:'EO_14243', name:'Stopping Waste, Fraud, and Abuse by Eliminating Information Silos', type:'ExecutiveOrder'});
CREATE (eo14271:Document {id:'EO_14271', name:'Ensuring Commercial, Cost-Effective Solutions in Federal Contracts', type:'ExecutiveOrder'});

-- === OMB M-26-10 ===
CREATE (m2610:Document {id:'OMB_M-26-10', name:'Reinforcing Transparency, Accountability, and Oversight of Federal Technology', type:'OMB_Memo', date:date('2026-03-31')});

-- Memo lineage
CREATE (m2610)-[:DERIVES_FROM]->(cfoAct);
CREATE (m2610)-[:DERIVES_FROM]->(paperworkDef);
CREATE (m2610)-[:DERIVES_FROM]->(nssDef);
CREATE (m2610)-[:DERIVES_FROM_Doc]->(eo13833);
CREATE (m2610)-[:DERIVES_FROM_Doc]->(eo14240);
CREATE (m2610)-[:DERIVES_FROM_Doc]->(eo14243);
CREATE (m2610)-[:DERIVES_FROM_Doc]->(eo14271);

-- === Institutions ===
CREATE (usda:Institution {id:'INST_USDA', name:'U.S. Department of Agriculture', kind:'ExecutiveAgency'});
CREATE (usdaCio:Institution {id:'INST_USDA_CIO', name:'USDA Chief Information Officer', kind:'CIO'});
CREATE (opm:Institution {id:'INST_OPM', name:'Office of Personnel Management', kind:'Agency'});
CREATE (congress:Institution {id:'INST_CONGRESS', name:'Congress of the United States', kind:'Legislature'});
CREATE (scotus:Institution {id:'INST_SCOTUS', name:'Supreme Court of the United States', kind:'Court'});

-- USDA hierarchy and memo application
CREATE (usda)-[:PARENT_OF_Inst]->(usdaCio);
CREATE (m2610)-[:APPLIES_TO]->(usda);
CREATE (m2610)-[:DIRECTS]->(usdaCio);
