# IVN Agent Recruitment System — Cost Ceiling Design

## Model

Per-day ceiling. Hard stop mid-run when ceiling is hit.
Ceiling value is a configurable field — set it in `recruiter_config.json`.

---

## Configuration

`recruiter_config.json` (lives in repo root, committed):

```json
{
  "daily_budget_usd": 5.00,
  "budget_reset_utc_hour": 0,
  "cost_alert_threshold_pct": 80,
  "model_pricing": {
    "claude-sonnet-4-20250514": {
      "input_per_mtok":  3.00,
      "output_per_mtok": 15.00
    },
    "gpt-4o": {
      "input_per_mtok":  2.50,
      "output_per_mtok": 10.00
    }
  }
}
```

`daily_budget_usd` is the only value you need to tune.
Start at $5.00 — adjust after the first week of sweep data.

---

## State Tracking

`recruiter_state.json` (lives on the host, NOT committed — gitignored):

```json
{
  "budget_date": "2026-05-08",
  "daily_spend_usd": 1.23,
  "daily_token_log": [
    {
      "timestamp": "2026-05-08T02:14:00Z",
      "gap_id": "gap-20260508-001",
      "sub_agent_type": "mapper",
      "model": "claude-sonnet-4-20250514",
      "input_tokens": 4200,
      "output_tokens": 800,
      "cost_usd": 0.024
    }
  ],
  "budget_halted": false,
  "halt_timestamp": null
}
```

At midnight UTC, the recruiter resets `budget_date`, `daily_spend_usd`,
`daily_token_log`, `budget_halted`, and `halt_timestamp`.

---

## Cost Calculation

After every API call, the recruiter reads token usage from the
response and computes cost:

```
cost = (input_tokens / 1,000,000 × input_per_mtok)
     + (output_tokens / 1,000,000 × output_per_mtok)
```

It adds `cost` to `daily_spend_usd` and appends the log entry.

---

## Mid-Run Stop Logic

```
BEFORE each sub-agent API call:
  if daily_spend_usd >= daily_budget_usd:
    set budget_halted = true
    set halt_timestamp = now()
    write partial sweep report to staging/manifest.json
    create GitHub issue: [IVN-BUDGET] Daily ceiling reached
    stop sweep — exit
  elif daily_spend_usd >= daily_budget_usd × cost_alert_threshold_pct:
    add alert to sweep report: "80% of daily budget consumed"
    continue sweep
```

The check runs **before** each call, not after. This prevents a single
expensive call from pushing spend past the ceiling without a stop.

Gap tasks already dispatched in the current run complete normally.
No task is killed mid-call. The stop applies to the **next** task.

---

## Budget Halt Escalation Issue

```
Title: [IVN-BUDGET] Daily ceiling reached — {date}

Body:
Date: {budget_date}
Ceiling: ${daily_budget_usd}
Spend at halt: ${daily_spend_usd}
Halt time: {halt_timestamp}
Tasks completed this run: {n}
Tasks not dispatched (backlog): {m}

Remaining gaps will be picked up on next sweep after midnight UTC reset.

Action required: Review daily_spend_usd trend.
If ceiling is consistently hit before sweep completes,
increase daily_budget_usd in recruiter_config.json
or reduce sweep scope (gap severity threshold).
```

Label: `budget-halt`

This issue auto-closes on midnight reset if no action is needed.
If you want to raise the ceiling, edit `recruiter_config.json` and
close the issue manually.

---

## Gap Priority Under Budget Pressure

When `daily_spend_usd` crosses 80%, the recruiter reorders the
remaining gap queue by severity score (descending) before continuing.
This ensures that if the ceiling hits, the lowest-severity gaps are
the ones left undispatched — not the high-value ones.

Severity order (from `gap_scoring.weights` in MANIFEST.json):

| Priority | Gap Type | Weight |
|---|---|---|
| 1 | missing_alignment_citation | 5 |
| 2 | missing_alignment_rationale | 4 |
| 3 | missing_alignment_validation | 4 |
| 4 | missing_component_description | 3 |
| 5 | missing_required_file | 3 |
| 6 | missing_companion_prompt | 2 |
| 7 | missing_component_url | 2 |
| 8 | stale_data_file | 1 |

Stale data alerts are always last. They are zero-cost (no API call)
so they are dispatched after the ceiling check loop exits.

---

## Expected Cost Range

At the default $5.00/day ceiling and using Claude Sonnet 4 pricing:

| Scenario | Est. tokens/task | Est. cost/task | Tasks/day at $5 |
|---|---|---|---|
| EC-DC pair mapping | ~5,000 in / 1,000 out | ~$0.03 | ~160 |
| Alignment rationale | ~4,000 in / 800 out | ~$0.024 | ~200 |
| Citation validation | ~3,000 in / 500 out | ~$0.017 | ~290 |
| Doc generation | ~2,000 in / 2,000 out | ~$0.036 | ~135 |
| Cypher script | ~3,000 in / 1,500 out | ~$0.032 | ~155 |

A $5/day ceiling supports roughly 100–200 tasks per day depending
on mix. The IVN dataset has ~4,906 known cross-agency dependencies.
At 150 tasks/day, full coverage of known gaps takes ~33 days.
