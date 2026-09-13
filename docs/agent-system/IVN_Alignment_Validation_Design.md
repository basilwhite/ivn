# IVN Agent Recruitment System — Alignment Validation Design

## Model

The validator agent proposes. You decide.

The `valid` field stays null until you set it at promotion time.
The validator agent populates `similarity`, `confidence`,
`alignment_rationale`, and a `validation_proposal` block.
You read the proposal and mark valid: Y / N / Uncertain.

---

## What the Validator Agent Produces

One JSON record per alignment pair, dropped to
`staging/alignments/`:

```json
{
  "enabling_component_id": "OMB Circular A-123::Internal Control",
  "dependent_component_id": "USDA::Acquisition Oversight",
  "similarity": 0.74,
  "confidence": 0.81,
  "alignment_rationale": "OMB A-123 requires agencies to maintain internal controls over acquisition. USDA acquisition oversight directly implements this requirement.",
  "linkage_mandate_check": {
    "raw_value": "31 U.S.C. § 3512; OMB Circular A-123",
    "usc_citations_found": ["31 U.S.C. § 3512"],
    "omb_citations_found": ["OMB Circular A-123"],
    "citation_format_valid": true
  },
  "validation_proposal": {
    "proposed_valid": "Y",
    "confidence_tier": "high",
    "evidence_summary": "Strong semantic overlap between internal control mandate and acquisition oversight function. Valid USC citation present. Directional flow confirmed: A-123 enables USDA oversight.",
    "flags": []
  }
}
```

---

## Validator Agent System Prompt (Core Logic)

The validator agent receives:
- Enabling component description and ID
- Dependent component description and ID
- Existing `linkage_mandate` value (may be null)
- Existing `alignment_rationale` value (may be null)
- Directional rule: flow runs FROM Enabling TO Dependent

The agent produces:

**1. Similarity score (0.0–1.0)**
Semantic overlap between EC and DC descriptions.
Agent reasons explicitly: shared policy domain, shared functional
scope, shared regulatory authority. Not cosine similarity — reasoned
estimate with stated basis.

**2. Confidence score (0.0–1.0)**
Agent's confidence in the proposed_valid value.
High (>0.8): strong evidence, clear directional relationship, valid citation.
Medium (0.5–0.8): plausible relationship, missing citation or weak overlap.
Low (<0.5): unclear relationship, no citation, or directional ambiguity.

**3. Alignment rationale (1–3 sentences)**
Plain language. Active verbs. No adjectives.
States the mechanism: what the EC mandates, what the DC delivers,
how delivery progresses the DC closer to its state.

**4. Linkage mandate check**
Parse the `linkage_mandate` field for USC and OMB citation patterns.
USC pattern: `\d+ U\.S\.C\.[ §]+[\d\w]+`
CFR pattern: `\d+ C\.F\.R\.[ §]+[\d\w]+`
OMB pattern: `OMB (Circular|Memo|M-)\s*[\w-]+`
EO pattern: `(Executive Order|E\.O\.)\s*\d+`
Report `citation_format_valid: true` if at least one pattern matches.
Report `citation_format_valid: false` if `linkage_mandate` is null
or no pattern matches.

**5. Proposed valid value**
- "Y": similarity >= 0.65 AND confidence >= 0.70 AND citation valid
- "N": similarity < 0.40 OR directional rule violated
- "Uncertain": everything else

**6. Flags (array of strings)**
Conditions that should catch your attention:
- `"no_citation"`: linkage_mandate is null
- `"citation_format_invalid"`: linkage_mandate present but no pattern match
- `"low_similarity"`: similarity < 0.50
- `"directional_ambiguity"`: agent cannot confirm EC→DC direction
- `"orphan_component"`: EC or DC has no other alignments in dataset
- `"duplicate_pair"`: another alignment exists with same or similar IDs

---

## Confidence Tiers

| Tier | Range | Meaning |
|---|---|---|
| high | > 0.80 | Agent is confident. Likely correct without deep review. |
| medium | 0.50–0.80 | Review alignment_rationale before deciding. |
| low | < 0.50 | Needs your judgment. Agent uncertain. |

At promotion time, filter staging by confidence tier.
Review `low` first — those are the decisions that need you most.
Promote `high` in bulk when flags array is empty.

---

## Your Decision Interface at Promotion

For each staged alignment proposal, you set one field:

```
valid: Y | N | Uncertain
```

Nothing else. The agent already wrote `similarity`, `confidence`,
`alignment_rationale`, and `linkage_mandate_check`.

If you set `valid: N`, add a `rejection_reason` from the
standard taxonomy (see Quality Gate Design doc) and the
system re-queues for mapper agent to produce a better pair.

If you set `valid: Uncertain`, the record promotes with
`valid: Uncertain` — it is in the dataset but flagged for
future review. The recruiter does not re-queue Uncertain records
unless you explicitly trigger a manual sweep.

---

## Promotion Throughput Estimate

At $5/day ceiling:

| Action | Time estimate |
|---|---|
| Review high-confidence, no-flag proposal | 15 seconds |
| Review medium-confidence proposal | 45 seconds |
| Review low-confidence proposal | 90 seconds |

At a typical mix (50% high, 35% medium, 15% low) across
150 staged proposals/day, promotion review takes roughly
60–90 minutes per day if done in one session, or
10–15 minutes across 6 short review sessions.

Batch promotion of high-confidence, no-flag proposals
is the highest-leverage habit in this system.
