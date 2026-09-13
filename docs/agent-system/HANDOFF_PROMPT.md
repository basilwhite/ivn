# IVN Agent Recruitment System — Full Project Handoff Prompt

Paste this entire document to any LLM to resume this project with full
context. This is the authoritative knowledge transfer document.

---

## PROJECT OWNER

Basil White — retired federal IT governance professional (USDA, GS-2210,
32 years, separated June 26, 2026). Based in New Braunfels, TX.
Creator of the Integrated Value Network (IVN). GitHub: basilwhite.
Co-developer: Beth A. Martin (bamartin@gmail.com).

Communication style: short declarative sentences, active verbs, no
hedging, no adjectives, precise USC/CFR citations, systems framing.
Lead with the answer. Direct engagement with corrections.

---

## WHAT THE IVN IS

The Integrated Value Network models US federal governance as a directed
semantic network of policy components and dependency relationships.
GitHub repo: https://github.com/basilwhite/ivn

Three normalized tables:

SOURCES: source_name (PK), source_agency, source_id

COMPONENTS (8 fields):
component_name, component_description, component_url, component_agency,
component_ofc_of_primary_interest, source_id,
component_id (format: "source_name::component_name"), fetch_status

ALIGNMENTS (17 fields):
enabling_component_id, Enabling Component, Enabling Source,
dependent_component_id, Dependent Component, Dependent Source,
linkage_mandate, notes_and_keywords, keywords_tab_items_found,
edits, valid, similarity, confidence, transitive_support,
matched_enabling_index, matched_dependent_index, alignment_rationale

CRITICAL DIRECTIONAL RULE — NON-NEGOTIABLE:
Flow runs FROM Enabling Component TO Dependent Component.
Enabling = upstream/prerequisite. Dependent = downstream/beneficiary.
When an Enabling Component is delivered, it progresses the Dependent
Component closer to its delivery state.

Scale: ~10,522 policy components, ~4,906 cross-agency dependencies.

Key repo files:
- IVN_Normalize_Dataset_prompt.txt — authoritative schema spec
- IVN-dataset.xlsb — primary dataset (binary Excel)
- Public-IVN-dataset.xlsx — public-facing dataset
- MANIFEST.json — completeness model (in repo root)

---

## WHAT THE AGENT RECRUITMENT SYSTEM IS

A 24/7 OpenClaw orchestrator that detects gaps in the IVN dataset
and repo, spawns sub-agents to fill them, stages output for Basil's
review, and escalates failures via GitHub issues.

Platform: OpenClaw (cloud VPS, ~$10/mo)
State layer: basilwhite.com/ivn/ (DEPLOYED AND VERIFIED)
Staging: GitHub branch agent-staging (separate from main)
Nothing enters main automatically. Basil promotes manually.

---

## THREE-TIER MODEL ARCHITECTURE

### TIER A — Sonnet 4.6: $5.00/day
Model: claude-sonnet-4-6 ($3/$15 per 1M in/out)
Tasks: Null-field gap scanning, citation format validation,
       doc generation, structural checks, companion prompts
Volume: ~150-200 tasks/day
Gap coverage: ~33 days for all 4,906 known dependencies

### TIER B — Fable 5.1 Batch: $10.00/day
Model: claude-fable-5-1 Batch API ($5/$25 per 1M in/out)
Effort: medium
Tasks: Cross-agency EC-DC mapping, alignment validation proposals
Volume: ~30-60 tasks/day at $0.25-0.35/task
Turnaround: ~24 hours (asynchronous)
Enable: After Tier A stable for 14 days

### TIER C — Fable 5.1 Standard + Caching: $15.00/day
Model: claude-fable-5-1 standard ($10/$50 per 1M in/out)
Cache reads: $0.25/M (40x cheaper than fresh input)
Effort: low (planning turn) → high (execution turn), per-message
Tasks: Transitive chain discovery, crosswalk analysis,
       root-cause gap attribution, novel alignment proposal
Cache warm: midnight UTC, Components table (~$9.00 once/day)
Per run after cache: ~$1.24 total
Enable: After first client revenue engagement is contracted

TOTAL DAILY BUDGET: $30.00/day
PHASING: Tier A only first. Add Tier B at day 14. Add Tier C after revenue.

---

## WHAT FABLE 5.1 ENABLES THAT SONNET CANNOT

1. Full ~1M-token Components table in single context — no chunking,
   no broken cross-batch transitive chains.
2. Root-cause gap attribution — distinguishes WHY a gap exists
   (missing citation vs. superseded authority vs. ID format error
   vs. directional inversion). Sonnet confused these ~30% of time.
3. Novel alignment discovery — proposes candidate EC-DC pairs not
   yet in Alignments table.
4. Transitive gap clusters — identifies that gaps A/B/C share a
   common upstream cause. Fixing source resolves entire cluster.

Per-message effort switching: effort level changes per message without
invalidating cache. Use low effort (plan turn, ~$0.35) then high
effort (execution turn, ~$1.00) in same conversation. Cached context
pays $0.25/M on re-reads vs $10/M fresh.

---

## GAP DETECTION — TWO SOURCES

SOURCE A: Schema gaps (null fields in normalized Excel tables)
| Null field | Gap type | Weight | Tier |
| linkage_mandate | missing_alignment_citation | 5 | B |
| alignment_rationale | missing_alignment_rationale | 4 | B |
| valid/similarity/confidence | missing_alignment_validation | 4 | B |
| component_description | missing_component_description | 3 | A |
| component_url/fetch_status | missing_component_url | 2 | A |
| zero alignment rows | orphan_component | 5 | C |

SOURCE B: File gaps (MANIFEST.json diff vs live repo)
Confirmed missing files (first-sweep targets):
- ivn_neo4j_schema.cypher → coder agent (Tier A)
- ivn_neo4j_load.cypher → coder agent (Tier A)
- ivn_neo4j_queries.cypher → coder agent (Tier A)
- ivn_schema.md → doc agent (Tier A)
- ivn_agent_onboarding.md → doc agent (Tier A)

---

## QUALITY GATE

Everything stages. Nothing auto-rejected. Basil validates at promotion.
Structural check (format only) tags pass/warn — never blocks.
Requeue limit: 2. After 2 rejections → GitHub escalation issue.
Basil closes issue to retry. Labels closed-no-requeue to retire.

Rejection taxonomy: wrong_component_ids, citation_not_valid,
incomplete_output, schema_mismatch, out_of_scope, low_quality

---

## STATE LAYER: basilwhite.com/ivn/ — DEPLOYED AND VERIFIED

Files (all confirmed working as of 2026-05-08):
- briefing.json — co-owner session briefing (three-tier spend, alerts)
- state.json — daily spend per tier, last sweep, cache status
- manifest.json — staging manifest mirror
- trigger.php — GET polls pending trigger, POST+token queues sweep
- trigger_config.php — TRIGGER_TOKEN (never in repo or chat)
- .htaccess — protects config files

briefing.json now includes agent_system_docs URLs pointing to GitHub:
- architecture: /docs/agent-system/IVN_Agent_Recruitment_System_v2.md
- cost_model: /docs/agent-system/IVN_Fable51_Cost_Model.md
- quality_gate: /docs/agent-system/IVN_Quality_Gate_Design.md
- alignment_validation: /docs/agent-system/IVN_Alignment_Validation_Design.md
- handoff_prompt: /docs/agent-system/HANDOFF_PROMPT.md
- manifest: /MANIFEST.json

---

## CO-OWNER SESSION PROTOCOL

At session start:
1. Fetch https://basilwhite.com/ivn/briefing.json
2. Fetch https://basilwhite.com/ivn/state.json
3. Fetch https://basilwhite.com/ivn/manifest.json
4. Parse into briefing: spend per tier, staged files, decisions,
   escalations, cache status, alerts
5. Present decisions one at a time
6. Manual sweep trigger: POST to trigger.php with TRIGGER_TOKEN

TRIGGER_TOKEN: Never in chat. Never in repo. Lives in
trigger_config.php on basilwhite.com server and in OpenClaw
environment config only.

---

## IVN REVENUE DEVELOPMENT PLAN

Google Doc ID (v4): 1nLV50VncZFap5Hvoy2uxBs2U-7Gf4OtfxKaXfMGRJzk
URL: https://docs.google.com/document/d/1nLV50VncZFap5Hvoy2uxBs2U-7Gf4OtfxKaXfMGRJzk/edit

Key decisions made:
- Pricing validated via USASpending.gov (NAICS 541611)
- Product 1 (Dependency Trace Report): $15K-$40K/engagement
- Product 2 (Conflict and Duplication Audit): $25K-$75K
- Product 3 (Policy Change Impact Assessment): $10K-$30K
- Floor rule: never discount below $10K for any full engagement
- Priority target: Nexight Group LLC (UEI: LBDVN6QLYPY4) —
  active USDA AMS 541611 incumbent at $400K/year
- Monetization agent: 7 modules covering full pipeline from
  opportunity detection through invoice drafting
- Budget: $2.00/day for revenue monitor agent (separate from IVN $30/day)
- Human gate: agent drafts and stages — Basil decides, sends, signs,
  delivers, banks. Agent handles 80% of labor.

---

## BUILD SEQUENCE STATUS

COMPLETED:
- Completeness model (MANIFEST.json)
- Quality gate design
- Cost ceiling design (updated to three-tier)
- Alignment validation design
- Fable 5.1 cost model and selective routing
- basilwhite.com/ivn/ state layer (deployed and verified)
- Revenue development plan (Google Doc, v4)
- Revenue monitor agent design (7 modules)

NEXT — OpenClaw orchestrator config:
The runnable config tying together triggers, gap detection, agent
registry, three-tier cost ceiling, staging, state writing, and
trigger polling. Not yet built.

THEN: Five sub-agent system prompts (mapper, validator, coder, doc,
discovery), agent-staging branch setup, GitHub fine-grained PAT,
first end-to-end manual trigger test, enable nightly sweep.

---

## HARD RULES — NEVER VIOLATE

1. Directional rule: Enabling → Dependent. Non-negotiable.
2. component_id always "source_name::component_name" verbatim.
3. Never write "nan" or "None" strings — use proper nulls.
4. main branch never auto-written. Basil promotes manually.
5. Requeue limit 2, then GitHub escalation issue.
6. Tier A $5/day, Tier B $10/day, Tier C $15/day. Hard stops.
7. TRIGGER_TOKEN never in chat, never in any repo.
8. Tier C disabled until first revenue engagement contracted.
9. Agent never sends outreach, submits proposals, or delivers
   analysis. Agent drafts and stages only.
10. Revenue agent never submits anything. Basil approves and sends.
 
---

## KEY FILES IN REPO /docs/agent-system/

IVN_Agent_Recruitment_System_v2.md — full architecture (this session)
IVN_Fable51_Cost_Model.md — three-tier cost model with Fable 5.1
IVN_Quality_Gate_Design.md — staging, escalation, requeue logic
IVN_Alignment_Validation_Design.md — validator agent proposal logic
IVN_Cost_Ceiling_Design.md — original Sonnet-only cost model
HANDOFF_PROMPT.md — this file

In repo root:
MANIFEST.json — completeness model for IVN repo

On basilwhite.com/ivn/:
briefing.json, state.json, manifest.json, trigger.php,
trigger_config.php (TRIGGER_TOKEN set, never share), .htaccess
