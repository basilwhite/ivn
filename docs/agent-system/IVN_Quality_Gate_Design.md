# IVN Agent Recruitment System — Quality Gate Design

## Model

Everything stages. You validate at promotion time.
If you reject, you escalate. The system re-queues the gap.

No sub-agent output is discarded automatically.
No sub-agent output blocks the staging pipeline.
The recruiter does not make semantic judgments.

---

## What the Recruiter Does at Staging (Structural Check Only)

Before writing a file to `agent-staging`, the recruiter runs one
lightweight structural check per output type. This is not semantic
validation — it is format integrity only.

| Output Type | Structural Check | Pass Criteria |
|---|---|---|
| EC-DC pairs (CSV/JSON) | Required fields present | `enabling_component_id`, `dependent_component_id`, `linkage_mandate` all non-null |
| Alignment rationale (JSON) | Required fields present | `enabling_component_id`, `dependent_component_id`, `alignment_rationale` all non-null |
| Source citations (JSON) | Required fields present | `component_id`, `component_url`, `linkage_mandate` all non-null |
| Cypher scripts (.cypher) | Non-empty file | File size > 100 bytes |
| Python scripts (.py) | Non-empty, parseable | File size > 200 bytes; no SyntaxError on ast.parse() |
| Markdown docs (.md) | Non-empty | File size > 500 bytes |

**Structural check result is a tag — not a gate.**
The file stages regardless of pass or fail.
The tag goes into `staging/manifest.json` as `structural_check: "pass"` or `structural_check: "warn"`.

---

## What Stages with Every File

Every file dropped to `agent-staging` is accompanied by a metadata
record in `staging/manifest.json`:

```json
{
  "filename": "ec-dc-pairs/USDA_OMB_A123_2026050823.json",
  "staged_at": "2026-05-08T23:14:00Z",
  "gap_id": "gap-20260508-042",
  "gap_type": "missing_alignment_citation",
  "sub_agent_type": "mapper",
  "api_model": "claude-sonnet-4-20250514",
  "source_component_id": "OMB Circular A-123::Internal Control",
  "structural_check": "pass",
  "promotion_status": "pending",
  "rejection_reason": null,
  "requeue_count": 0
}
```

You review this manifest entry alongside the file at promotion time.

---

## Promotion Decision (Your Job)

When you review `agent-staging`:

**Promote:** Copy file to `main`. Set `promotion_status: "promoted"` in manifest.

**Reject:** Set `promotion_status: "rejected"` and write a
`rejection_reason` string. The system escalates automatically on
next sweep.

Rejection reasons drive re-queuing logic:

| Rejection Reason | System Action |
|---|---|
| `"wrong_component_ids"` | Re-queues gap with corrected component context |
| `"citation_not_valid"` | Re-queues gap to validator agent with citation check flag |
| `"incomplete_output"` | Re-queues gap to same sub-agent type with higher max_tokens |
| `"schema_mismatch"` | Re-queues gap to coder agent with schema correction prompt |
| `"out_of_scope"` | Closes gap — no requeue. Flags gap definition for your review. |
| `"low_quality"` | Re-queues gap with quality flag — escalates to you after 2 retries |

---

## Escalation Mechanism

Escalation = a GitHub issue created automatically in the IVN repo.

**Trigger conditions:**
- You set `promotion_status: "rejected"` on any file
- A gap has `requeue_count >= 2` with no successful promotion
- Staging backlog exceeds 20 unreviewed files
- A data file exceeds `max_age_days` from MANIFEST.json

**Issue format:**

```
Title: [IVN-ESCALATION] {gap_type} — {component_id or filename}

Body:
Gap ID: {gap_id}
Gap Type: {gap_type}
Sub-agent: {sub_agent_type} / {api_model}
Staged: {staged_at}
Rejection reason: {rejection_reason}
Requeue count: {requeue_count}

Staged file: agent-staging/{filename}
Manifest entry: staging/manifest.json#{gap_id}

Action required: Review staged file and manifest entry.
Close this issue to re-queue with updated instructions,
or label 'closed-no-requeue' to retire the gap.
```

**Issue labels applied automatically:**
- `escalation`
- `gap-type/{gap_type}` (e.g. `gap-type/missing_alignment_citation`)
- `agent/{sub_agent_type}`
- `requeue-count/{n}`

You close the issue to re-queue. You label `closed-no-requeue` to retire.
The recruiter reads open issues labeled `escalation` on each sweep
and skips re-queuing those gaps until the issue is closed.

---

## Requeue Logic

On each sweep, after gap detection, the recruiter checks
`staging/manifest.json` for entries where:

- `promotion_status: "rejected"`
- `requeue_count < 2`

It re-queues those gaps with the `rejection_reason` appended to the
sub-agent's system prompt as a correction instruction.

After 2 rejections, it creates a GitHub escalation issue and stops
re-queuing until you close it.

---

## What This Model Guarantees

- No output is silently discarded.
- No output blocks the pipeline.
- Every staging decision is auditable via `staging/manifest.json`.
- Every rejection produces a traceable GitHub issue.
- You retain full authority over what enters `main`.
- The system never retries more than twice without your involvement.
