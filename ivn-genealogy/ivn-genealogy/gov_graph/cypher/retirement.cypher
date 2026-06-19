-- Title 5 retirement slice
CREATE (t5:Provision {id:'USC_T5', citation:'5 U.S.C.', level:'Title', heading:'Government Organization and Employees'});
CREATE (usc_ch83:Provision {id:'USC_T5_CH83', citation:'5 U.S.C. ch. 83', level:'Chapter', heading:'Retirement (CSRS)'});
CREATE (usc_8336:Provision {id:'USC_8336', citation:'5 U.S.C. §8336', level:'USC_Section', heading:'Immediate retirement (CSRS)'});
CREATE (usc_ch84:Provision {id:'USC_T5_CH84', citation:'5 U.S.C. ch. 84', level:'Chapter', heading:'Federal Employees’ Retirement System (FERS)'});
CREATE (usc_8412:Provision {id:'USC_8412', citation:'5 U.S.C. §8412', level:'USC_Section', heading:'Immediate retirement (FERS)'});
CREATE (usc_8414:Provision {id:'USC_8414', citation:'5 U.S.C. §8414', level:'USC_Section', heading:'Early retirement (FERS)'});
CREATE (usc_8461:Provision {id:'USC_8461', citation:'5 U.S.C. §8461', level:'USC_Section', heading:'Authority of the Office of Personnel Management'});

-- Genealogy
CREATE (t5)-[:PARENT_OF_Provision]->(usc_ch83);
CREATE (t5)-[:PARENT_OF_Provision]->(usc_ch84);
CREATE (usc_ch83)-[:PARENT_OF_Provision]->(usc_8336);
CREATE (usc_ch84)-[:PARENT_OF_Provision]->(usc_8412);
CREATE (usc_ch84)-[:PARENT_OF_Provision]->(usc_8414);
CREATE (usc_ch84)-[:PARENT_OF_Provision]->(usc_8461);

-- CFR
CREATE (cfr_831:Provision {id:'CFR_5_831', citation:'5 C.F.R. pt. 831', level:'CFR_Section', heading:'Civil Service Retirement System'});
CREATE (cfr_841:Provision {id:'CFR_5_841', citation:'5 C.F.R. pt. 841', level:'CFR_Section', heading:'FERS—General Administration'});
CREATE (cfr_842:Provision {id:'CFR_5_842', citation:'5 C.F.R. pt. 842', level:'CFR_Section', heading:'FERS—Basic Annuity Rights'});
CREATE (cfr_846:Provision {id:'CFR_5_846', citation:'5 C.F.R. pt. 846', level:'CFR_Section', heading:'FERS—Elections of Coverage'});

CREATE (usc_8336)-[:IMPLEMENTED_BY_Provision]->(cfr_831);
CREATE (usc_8412)-[:IMPLEMENTED_BY_Provision]->(cfr_842);
CREATE (usc_8414)-[:IMPLEMENTED_BY_Provision]->(cfr_842);
CREATE (usc_8461)-[:IMPLEMENTED_BY_Provision]->(cfr_841);
CREATE (usc_8461)-[:IMPLEMENTED_BY_Provision]->(cfr_846);

-- Bind institutions
MATCH (opm:Institution {id:'INST_OPM'})
MATCH (usda:Institution {id:'INST_USDA'})
WITH opm, usda
UNWIND ['USC_8336','USC_8412','USC_8414','USC_8461','CFR_5_831','CFR_5_841','CFR_5_842','CFR_5_846'] AS pid
MATCH (p:Provision {id:pid})
CREATE (opm)-[:BOUND_BY]->(p);
CREATE (usda)-[:BOUND_BY]->(p);

-- Constitutional ancestry
MATCH (taxClause:Provision {id:'CONST_ART1_S8_CL1'})
MATCH (commerceClause:Provision {id:'CONST_ART1_S8_CL3'})
WITH taxClause, commerceClause
MATCH (usc_8336:Provision {id:'USC_8336'})
MATCH (usc_8412:Provision {id:'USC_8412'})
MATCH (usc_8414:Provision {id:'USC_8414'})
MATCH (usc_8461:Provision {id:'USC_8461'})
CREATE (usc_8336)-[:DERIVES_FROM_Provision]->(taxClause);
CREATE (usc_8412)-[:DERIVES_FROM_Provision]->(taxClause);
CREATE (usc_8414)-[:DERIVES_FROM_Provision]->(taxClause);
CREATE (usc_8461)-[:DERIVES_FROM_Provision]->(taxClause);
CREATE (usc_8412)-[:DERIVES_FROM_Provision]->(commerceClause);
CREATE (usc_8414)-[:DERIVES_FROM_Provision]->(commerceClause);

-- Cases
CREATE (case_munn:Document {id:'CASE_MUNN', name:'Munn v. Illinois, 94 U.S. 113 (1877)', type:'Case'});
MATCH (commerceClause:Provision {id:'CONST_ART1_S8_CL3'})
CREATE (commerceClause)-[:INTERPRETED_BY]->(case_munn);
