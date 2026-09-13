# IVN Agent Recruitment System — Fable 5.1 Cost Model and Selective Routing

## Key Pricing Facts

| Item | Rate |
|---|---|
| Fable 5.1 input | $10.00 / 1M tokens |
| Fable 5.1 output (incl. thinking) | $50.00 / 1M tokens |
| Fable 5.1 cache read | $0.25 / 1M tokens (75% cheaper than Fable 5) |
| Sonnet 4.6 input | $3.00 / 1M tokens |
| Sonnet 4.6 output | $15.00 / 1M tokens |
| Opus 5 input | $5.00 / 1M tokens |
| Opus 5 output | $25.00 / 1M tokens |
| Batch API (Fable 5.1) | $5.00 / $25.00 per 1M tokens |

Effort controls output token volume, not per-token rate.
Approximate output tokens per turn by effort level:

| Effort | Output tokens/turn | Fable 5.1 cost/turn (10K input) |
|---|---|---|
| low | ~5,000 | ~$0.35 |
| medium | ~8,000–12,000 | ~$0.50–0.70 |
| high (API default) | ~20,000 | ~$0.70–1.10 |
| xhigh | ~60,000 | ~$3.10 |
| max | ~100,000+ | ~$5.00+ |

Critical feature: effort level can change per message without invalidating
cache. Use a low-effort planning turn, then high-effort execution turn,
in the same conversation — cached context pays only $0.25/M on re-reads.

---

## IVN Workload Taxonomy

Five workload types in the IVN agent recruitment system, classified
by complexity and Fable 5.1 necessity:

### TIER 1: Routine Gap Scanning
What: Null-field detection in Components and Alignments tables.
Complexity: Structured data comparison. No reasoning required.
Can Fable do something Sonnet cannot? No.
Routing: Sonnet 4.6 (keep current)
Volume: ~50–200 checks/day
Cost: $0.01–0.03/check → $0.50–6.00/day
Daily budget impact: Covered by existing $5.00/day Sonnet ceiling

### TIER 2: Citation Format Validation
What: Parse linkage_mandate field for USC/CFR/OMB patterns.
Complexity: Regex pattern matching with LLM normalization.
Can Fable do something Sonnet cannot? No.
Routing: Sonnet 4.6
Volume: ~20–50 validations/day
Cost: $0.017–0.024/call
Daily budget impact: Covered by existing $5.00/day Sonnet ceiling

### TIER 3: EC-DC Pair Mapping (Standard)
What: Generate enabling→dependent alignment for well-understood agency pairs.
Complexity: Medium. Directional rule application, citation lookup.
Can Fable do something Sonnet cannot? Marginally.
Routing: Sonnet 4.6 for intra-agency pairs; Fable 5.1 low effort
for cross-agency pairs where prior context is limited.
Fable cost: ~$0.35/call at low effort
Volume: ~20–50/day
Daily budget impact: $7–18/day at Fable (exceeds current ceiling)
Recommendation: Run cross-agency mapping in Batch API at $5/$25 rates
Batch cost: ~$0.175/call → $3.50–8.75/day

### TIER 4: Alignment Validation Proposals
What: Produce similarity, confidence, rationale, citation check, proposed
valid value, and flags for each Alignments record with null valid field.
Complexity: High. Requires semantic reasoning about policy relationships,
directional rule enforcement, multi-factor scoring.
Can Fable do something Sonnet cannot? YES — substantially.
Sonnet produces proposals but misclassifies directional ambiguity and
confuses citation absence with citation invalidity. Fable 5.1 at
medium effort produces root-cause distinction between these cases.
Routing: Fable 5.1 medium effort
Fable cost: ~$0.50–0.70/call
Volume: ~20–30/day
Daily budget impact: $10–21/day at standard pricing
Recommendation: Batch API → $0.25–0.35/call → $5–10.50/day

### TIER 5: Transitive Chain Discovery and Crosswalk Analysis
What: Given a policy change or new component, identify ALL downstream
dependencies across the full 10,522-component dataset. Trace second-
and third-order effects across agencies. Discover previously unmapped
alignment opportunities.
Complexity: Extremely high. Requires full dataset in context, multi-hop
reasoning, root-cause gap attribution.
Can Fable do something Sonnet cannot? YES — categorically.
Sonnet cannot hold the full IVN dataset in context. It cannot trace
transitive chains without chunking, which breaks cross-batch dependencies.
Fable 5.1 with caching holds the full dataset (Components table ~1M
tokens) in one context and reasons across it at high effort.
Routing: Fable 5.1 high effort with aggressive caching
Architecture: Cache the IVN schema context + full Components table
once ($10/M write, then $0.25/M reads). Each discovery run pays only
cache read cost for the dataset, plus output cost for reasoning.

Cache setup cost (one-time per day):
~900K input tokens × $10/M = $9.00 (written to cache)

Discovery run cost (each run after cache established):
~900K cached tokens × $0.25/M = $0.225 (cache read)
~1K uncached input tokens × $10/M = $0.01 (query)
~20K output tokens at high effort × $50/M = $1.00
Total per discovery run: ~$1.24

Cache write amortized across 10 discovery runs/day: $0.90/run
Effective cost per discovery run (with cache): ~$2.14
Volume: 1–3 runs/day (not every gap requires discovery)
Daily budget impact: $2.14–6.42/day for discovery runs
Plus cache setup: $9.00/day (or amortize across longer cache TTL)

---

## Revised Budget Architecture

Current: Single $5.00/day ceiling, Sonnet 4.6 only.
Problem: Excludes Fable entirely. Leaves transitive discovery impossible.

Proposed: Three-tier daily budget, three models.

TIER A — SONNET 4.6: $5.00/day (unchanged)
Covers: Null scanning, citation format validation, doc generation,
structural checks, companion prompt generation.
~150–200 tasks/day. Full 4,906-gap coverage in ~33 days.

TIER B — FABLE 5.1 BATCH: $10.00/day
Covers: Cross-agency EC-DC mapping, alignment validation proposals.
Batch API at $5/$25 — all output at half standard rate.
~30–60 alignment validation proposals/day at $0.25–0.35 each.
Note: Batch API has ~24-hour turnaround. Use for non-urgent tasks.

TIER C — FABLE 5.1 STANDARD (HIGH EFFORT + CACHING): $15.00/day
Covers: Transitive chain discovery, crosswalk analysis, root-cause
gap attribution.
Cache setup: $9.00 (amortized or paid daily).
Discovery runs: 3–5 per day at ~$1.24 each.
Stays within $15.00 ceiling with headroom.

TOTAL DAILY BUDGET: $30.00/day
(vs. current $5.00/day for Sonnet only)

Monthly at full operation: ~$900/month
Break-even: One Product 1 engagement ($15,000–$40,000) per quarter
covers >3 months of full-operation agent costs.

---

## Per-Message Effort Routing (API Implementation)

Fable 5.1 allows effort to change per message without cache invalidation.
The IVN agent uses this for mixed-complexity runs:

```python
import anthropic

client = anthropic.Anthropic()

def ivn_discovery_run(dataset_context: str, query: str) -> dict:
    """
    Two-turn IVN discovery run:
    Turn 1: Low effort planning (what gaps to probe, which components)
    Turn 2: High effort execution (trace transitive chains, root-cause)
    Cache persists across turns — dataset_context paid once.
    """
    conversation = []

    # Turn 1: Low effort planning
    conversation.append({"role": "user", "content": dataset_context + "\n\n" + query})
    plan_response = client.messages.create(
        model="claude-fable-5-1",
        max_tokens=4096,
        effort="low",  # Cheap planning turn
        system=IVN_SYSTEM_PROMPT,
        messages=conversation
    )
    plan = plan_response.content[0].text
    conversation.append({"role": "assistant", "content": plan})

    # Turn 2: High effort execution (dataset_context now cached)
    conversation.append({"role": "user", "content": "Execute the plan above. Trace all transitive dependencies. Report root causes for each gap cluster."})
    exec_response = client.messages.create(
        model="claude-fable-5-1",
        max_tokens=32768,
        effort="high",  # High effort execution, cached context
        system=IVN_SYSTEM_PROMPT,
        messages=conversation
    )
    return {
        "plan": plan,
        "discovery_output": exec_response.content[0].text,
        "input_tokens": plan_response.usage.input_tokens + exec_response.usage.input_tokens,
        "output_tokens": plan_response.usage.output_tokens + exec_response.usage.output_tokens,
        "cache_read_tokens": exec_response.usage.cache_read_input_tokens
    }
```

---

## What Fable 5.1 Enables That Prior Models Cannot

### 1. Full-Dataset Crosswalk in Single Context
Prior: Chunked analysis across batches. Cross-batch transitive chains lost.
Fable: ~900K–1M token Components table in one context. Full graph visible.
IVN impact: Third- and fourth-order dependency chains discoverable for
the first time. A change to one OMB circular's EC status can be traced
to its effects on all 4,906 known dependency pairs simultaneously.

### 2. Root-Cause Gap Attribution
Prior: Flags null valid field. Proposes rationale. Cannot distinguish why.
Fable: Traces backwards — is the null because (a) no citation exists,
(b) citation was superseded, (c) component_id format error, or
(d) alignment is directionally inverted? Each requires a different
sub-agent response. Prior models confused these categories ~30% of the time.
IVN impact: Correct sub-agent routing, no wasted API calls on wrong fix.

### 3. Novel Alignment Discovery
Prior: Validates existing pairs. Cannot propose genuinely new EC-DC pairs
outside the existing alignment table.
Fable at high effort: Given the full component graph, proposes candidate
EC-DC pairs that do not yet exist in the Alignments table — based on
shared policy domain, shared authority, and shared agency scope —
with rationale and confidence score.
IVN impact: Active graph expansion, not just gap-filling.

### 4. Transitive Gap Clusters
Prior: Reports individual gaps. Cannot group correlated gaps.
Fable: Identifies that gaps A, B, C, D share a common upstream cause —
one missing citation at the source propagates as null linkage_mandate
across all downstream pairs. Fixing the source resolves the cluster.
IVN impact: Dramatically higher ROI per API call. Fix one, resolve ten.

---

## Implementation Recommendation

Build sequence for Fable 5.1 integration:

Step 1: Keep Sonnet 4.6 as Tier A (unchanged). No disruption.

Step 2: Route alignment validation proposals to Fable 5.1 Batch.
Write the Batch API submission script. Test with 10 validation tasks.
Compare output quality against Sonnet baseline.
Expected: Fewer directional errors, better root-cause flags.
Budget: Batch API $5/$25 keeps costs at $0.25–0.35/task.

Step 3: Build IVN schema cache. Cache the full Components table
plus IVN_Normalize_Dataset_prompt.txt once per day.
Write the cache-warming script (runs at midnight, before nightly sweep).
Verify cache_read_input_tokens appear in API responses.

Step 4: Build transitive discovery module (Tier C).
Two-turn architecture: low effort planning + high effort execution.
Run 1–3 discovery sweeps per day on the highest-priority gap clusters.
Report findings to staging/alignments/discovery_[date].json

Step 5: Tune effort levels based on 14-day cost/quality observation.
If Tier B Batch is producing high-quality proposals: keep at medium effort.
If Tier C discovery runs are hitting ceiling before completing: switch to
xhigh effort only for gap clusters with priority score > 20.

---

## Revised Budget Summary for openClaw recruiter_config.json

```json
{
  "daily_budget_tiers": {
    "tier_a_sonnet": {
      "model": "claude-sonnet-4-6",
      "daily_budget_usd": 5.00,
      "tasks": ["null_scan", "citation_format", "doc_gen", "structural_check"],
      "reset_utc_hour": 0
    },
    "tier_b_fable_batch": {
      "model": "claude-fable-5-1",
      "api_mode": "batch",
      "daily_budget_usd": 10.00,
      "tasks": ["cross_agency_mapping", "alignment_validation"],
      "effort": "medium",
      "reset_utc_hour": 0
    },
    "tier_c_fable_standard": {
      "model": "claude-fable-5-1",
      "api_mode": "standard",
      "daily_budget_usd": 15.00,
      "tasks": ["transitive_discovery", "crosswalk_analysis", "root_cause_attribution"],
      "effort_plan_turn": "low",
      "effort_exec_turn": "high",
      "cache_warm_at_utc_hour": 0,
      "reset_utc_hour": 0
    }
  },
  "total_daily_budget_usd": 30.00,
  "model_pricing": {
    "claude-sonnet-4-6": {
      "input_per_mtok": 3.00,
      "output_per_mtok": 15.00,
      "cache_read_per_mtok": 0.30
    },
    "claude-fable-5-1": {
      "input_per_mtok": 10.00,
      "output_per_mtok": 50.00,
      "cache_read_per_mtok": 0.25,
      "batch_input_per_mtok": 5.00,
      "batch_output_per_mtok": 25.00
    }
  }
}
```
