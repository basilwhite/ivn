# IVN Agent Recruitment System — Architecture v2
# Updated: 2026-09-13 — Fable 5.1 three-tier model integrated

---

## What This System Does

A continuously running OpenClaw orchestrator detects gaps in the IVN
GitHub repo and dataset, routes each gap to the appropriate sub-agent
and model tier, stages output for Basil's review, and escalates
failures via GitHub issues.

---

## Repository

https://github.com/basilwhite/ivn
State layer: https://basilwhite.com/ivn/
Agent-staging branch: agent-staging (separate from main)

---

## Three-Tier Model Architecture

### TIER A — Sonnet 4.6: $5.00/day
Model: claude-sonnet-4-6
Tasks: Null-field gap scanning, citation format validation,
       doc generation, structural checks, companion prompts
Volume: ~150-200 tasks/day
Full gap coverage: ~33 days at 150 tasks/day

### TIER B — Fable 5.1 Batch API: $10.00/day
Model: claude-fable-5-1 (Batch API, $5/$25 per 1M tokens)
Effort: medium
Tasks: Cross-agency EC-DC pair mapping, alignment validation proposals
Volume: ~30-60 tasks/day at $0.25-0.35/task
Turnaround: ~24 hours (asynchronous)
Enable: After Tier A is stable and running

### TIER C — Fable 5.1 Standard + Caching: $15.00/day
Model: claude-fable-5-1 (standard API, $10/$50 per 1M tokens)
Cache reads: $0.25/M (40x cheaper than fresh input)
Effort: low (planning turn) → high (execution turn), per-message
Tasks: Transitive chain discovery, crosswalk analysis, root-cause
       gap attribution, novel alignment proposal
Volume: 1-3 discovery runs/day
Cache setup: Warm at midnight UTC (Components table ~$9.00 once/day)
Per run after cache: ~$1.24 (cache read + output at high effort)
Enable: After first client engagement is contracted

TOTAL: $30.00/day at full operation
PHASED: Start Tier A only ($5/day). Add Tier B after 14 days.
        Add Tier C after first revenue.

---

## What Fable 5.1 Enables That Sonnet Cannot

1. FULL-DATASET CONTEXT: Components table (~1M tokens) in single context.
   Sonnet required chunking, breaking cross-batch transitive chains.
   Fable sees the entire graph simultaneously.

2. ROOT-CAUSE GAP ATTRIBUTION: Distinguishes WHY a gap exists —
   missing citation vs. superseded authority vs. component_id format
   error vs. directional inversion. Each requires different sub-agent.
   Sonnet confused these categories ~30% of the time.

3. NOVEL ALIGNMENT DISCOVERY: Proposes candidate EC-DC pairs not yet
   in Alignments table, based on shared policy domain and authority.
   Active graph expansion, not just gap-filling.

4. TRANSITIVE GAP CLUSTERS: Identifies that gaps A, B, C, D share
   a common upstream cause. Fixing source resolves entire cluster.
   Dramatically higher ROI per API call.

---

## Gap Detection — Two Sources

### Source A: Schema gaps (normalized Excel tables via GitHub API)
Scans null fields in Components and Alignments tables.

| Null field | Gap type | Weight | Tier |
|---|---|---|---|
| linkage_mandate | missing_alignment_citation | 5 | B |
| alignment_rationale | missing_alignment_rationale | 4 | B |
| valid/similarity/confidence | missing_alignment_validation | 4 | B |
| component_description | missing_component_description | 3 | A |
| component_url/fetch_status | missing_component_url | 2 | A |
| zero alignments rows | orphan_component | 5 | C |

### Source B: File gaps (MANIFEST.json diff against live repo)
Confirmed missing files (first-sweep targets):
- ivn_neo4j_schema.cypher → coder agent (Tier A)
- ivn_neo4j_load.cypher → coder agent (Tier A)
- ivn_neo4j_queries.cypher → coder agent (Tier A)
- ivn_schema.md → doc agent (Tier A)
- ivn_agent_onboarding.md → doc agent (Tier A)

---

## Sub-Agent Registry

| Gap type | Agent | Tier | Effort |
|---|---|---|---|
| missing_alignment_citation | mapper | B | medium |
| missing_alignment_rationale | mapper | B | medium |
| missing_alignment_validation | validator | B | medium |
| missing_component_description | validator | A | — |
| missing_component_url | validator | A | — |
| missing_required_file (cypher) | coder | A | — |
| missing_required_file (doc) | doc | A | — |
| transitive discovery | discovery | C | low→high |
| novel alignment proposal | discovery | C | high |
| orphan component | mapper+discovery | C | high |

---

## Quality Gate

Everything stages. Basil validates at promotion time.
No output is auto-rejected. No output blocks the pipeline.

Structural check (format only, NOT semantic) tags pass/warn:
- EC-DC pairs: required fields non-null
- .cypher files: size > 100 bytes
- .py files: size > 200 bytes, no SyntaxError
- .md files: size > 500 bytes

Manifest entry per staged file:
{
  filename, staged_at, gap_id, gap_type, sub_agent_type,
  api_model, effort_level, tier, structural_check,
  promotion_status, rejection_reason, requeue_count
}

Rejection taxonomy: wrong_component_ids, citation_not_valid,
incomplete_output, schema_mismatch, out_of_scope, low_quality

Requeue limit: 2. After 2 rejections → GitHub escalation issue.
Basil closes issue to retry or labels closed-no-requeue to retire.

---

## Cost Ceiling Logic

Per-tier daily budgets enforced independently.
Check runs BEFORE each API call on the tier's budget.
On ceiling hit: stop that tier, stage partial report, create
GitHub issue [IVN-BUDGET-TIER-X], auto-close at midnight UTC reset.

At 80% of any tier's ceiling: reorder remaining gap queue by
severity descending so high-value gaps complete before ceiling hits.

Tier C caveat: Cache warm cost ($9.00) is paid once at midnight
before sweep begins. If Tier C ceiling is hit during sweep, the
cache write is sunk. Tier C ceiling ($15.00) accounts for this.

---

## Per-Message Effort Switching (Tier C)

Fable 5.1 allows effort change per message without cache invalidation.
Discovery run architecture:

Turn 1 — Planning (effort: low, ~$0.35):
  "Given [IVN schema context from cache], identify which component
   clusters have correlated gaps that share a common upstream cause."

Turn 2 — Execution (effort: high, ~$1.00, same cached context):
  "Execute the plan above. Trace all transitive dependencies to depth 3.
   Report root causes. Propose fixes. Assign sub-agent type per gap."

Total per discovery run: ~$1.24 (excluding cache write amortization)

API implementation:
  model="claude-fable-5-1"
  effort="low" for Turn 1, effort="high" for Turn 2
  same messages array — cache persists across turns

---

## State Layer: basilwhite.com/ivn/

DEPLOYED AND VERIFIED. Files:
- briefing.json — co-owner session briefing
- state.json — daily spend per tier, last sweep timestamp
- manifest.json — staging manifest mirror
- trigger.php — manual sweep trigger (GET polls, POST+token queues)
- trigger_config.php — TRIGGER_TOKEN (never in repo or chat)

State.json schema updated for three tiers:
{
  "budget_date": "YYYY-MM-DD",
  "tier_a": { "daily_spend_usd": 0.00, "budget_halted": false },
  "tier_b": { "daily_spend_usd": 0.00, "budget_halted": false },
  "tier_c": { "daily_spend_usd": 0.00, "budget_halted": false,
               "cache_warmed": false, "cache_warm_cost_usd": 0.00 },
  "last_sweep_timestamp": null,
  "last_sweep_trigger": null
}

---

## Build Sequence

PHASE 0 — PREREQUISITES
P0-1: agent-staging branch created in basilwhite/ivn repo
P0-2: MANIFEST.json committed to repo root
P0-3: Fine-grained GitHub PAT (write to agent-staging only)
P0-4: OpenClaw host provisioned (cloud VPS, ~$10/mo)
P0-5: Anthropic API key in OpenClaw environment config

PHASE 1 — TIER A (Sonnet gap scanning)
P1-1: Write OpenClaw config for null-field scan (GitHub API read)
P1-2: Write Sonnet sub-agent prompts: mapper, validator, coder, doc
P1-3: Write staging write logic (GitHub API to agent-staging branch)
P1-4: Write state/briefing FTP write to basilwhite.com/ivn/
P1-5: Manual trigger test: one gap → one staged file end-to-end
P1-6: Enable nightly Tier A sweep

PHASE 2 — TIER B (Fable 5.1 Batch)
P2-1: Write Batch API submission script for alignment validation
P2-2: Write Fable alignment validator system prompt
P2-3: Write Batch result polling and staging logic
P2-4: Test with 10 alignment validation tasks
P2-5: Compare output quality vs. Sonnet baseline
P2-6: Enable Tier B if quality threshold met

PHASE 3 — TIER C (Fable 5.1 Transitive Discovery)
P3-1: Write cache-warming script (midnight UTC, Components table)
P3-2: Write two-turn discovery run (low effort plan + high effort exec)
P3-3: Write discovery result parser and staging logic
P3-4: Test with one real gap cluster
P3-5: Enable Tier C after first revenue engagement

---

## Co-Owner Session Protocol

Session opener:
1. Fetch basilwhite.com/ivn/briefing.json
2. Fetch basilwhite.com/ivn/state.json
3. Fetch basilwhite.com/ivn/manifest.json
4. Parse into briefing: spend per tier, staged files, decisions,
   open escalations, cache status, alerts
5. Present decisions one at a time
6. Manual trigger: POST to trigger.php with TRIGGER_TOKEN

TRIGGER_TOKEN: Never in chat. Never in repo. Lives in
trigger_config.php on server and OpenClaw environment config.

---

## Hard Rules

1. Directional rule non-negotiable: Enabling → Dependent.
2. component_id always "source_name::component_name" verbatim.
3. Never write "nan" or "None" strings — use proper nulls.
4. main branch never auto-written. Basil promotes manually.
5. Requeue limit 2, then GitHub escalation issue.
6. Tier A $5/day, Tier B $10/day, Tier C $15/day. Hard stops.
7. TRIGGER_TOKEN never in chat.
8. Tier C disabled until first revenue engagement is contracted.
9. Cache warm cost is sunk if Tier C ceiling hits mid-sweep.
   This is acceptable — cache warm is always cheaper than re-reads.
10. Fable 5.1 Batch has ~24hr turnaround. Never use for deadline tasks.

---

## Key Documents (all in basilwhite/ivn/docs/agent-system/)

IVN_Agent_Recruitment_System_v2.md — this file (architecture)
IVN_Cost_Ceiling_Design.md — original Sonnet-only cost model
IVN_Fable51_Cost_Model.md — three-tier cost model with Fable 5.1
IVN_Quality_Gate_Design.md — staging, escalation, requeue logic
IVN_Alignment_Validation_Design.md — validator agent proposal logic
MANIFEST.json — completeness model (also in repo root)
ivn_site/DEPLOY.md — basilwhite.com/ivn/ deployment instructions
HANDOFF_PROMPT.md — full project knowledge transfer for any LLM
