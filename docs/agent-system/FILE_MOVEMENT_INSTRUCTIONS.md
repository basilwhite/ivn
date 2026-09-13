# IVN Agent System — File Movement Instructions
# Where to put files so Claude can find them in any future session

---

## The Problem

Claude's session memory files hold summaries, not full document content.
If a session ends before key documents are committed somewhere Claude
can fetch, the detailed architecture, cost models, and system prompts
are lost. The solution is to commit everything to two places Claude
can always read: the GitHub repo and basilwhite.com/ivn/.

---

## ACTION 1: Create /docs/agent-system/ in the IVN GitHub repo

In the basilwhite/ivn repo, create a new directory:
  /docs/agent-system/

Commit the following files to that directory:

FROM your downloads (from this session):
  IVN_Agent_Recruitment_System_v2.md  → /docs/agent-system/
  IVN_Fable51_Cost_Model.md           → /docs/agent-system/
  IVN_Quality_Gate_Design.md          → /docs/agent-system/
  IVN_Alignment_Validation_Design.md  → /docs/agent-system/
  IVN_Cost_Ceiling_Design.md          → /docs/agent-system/

FROM your downloads (also commit to repo root):
  MANIFEST.json                       → / (repo root)

Why GitHub: Claude can fetch raw.githubusercontent.com URLs directly.
At the start of any session, Claude fetches:
  https://raw.githubusercontent.com/basilwhite/ivn/main/docs/agent-system/IVN_Agent_Recruitment_System_v2.md
and has the full architecture in context immediately.

---

## ACTION 2: Add HANDOFF_PROMPT.md to the repo

The handoff prompt from our earlier session (the large text block
you saved) should be committed as:
  /docs/agent-system/HANDOFF_PROMPT.md

This is the fastest recovery path if Claude loses all memory context.
Any new Claude session can fetch this file and resume with full context.

---

## ACTION 3: Update basilwhite.com/ivn/briefing.json

Add a field to briefing.json that points to the key docs:

{
  "generated_at": "...",
  "status": "not_initialized",
  "agent_system_docs": {
    "architecture": "https://raw.githubusercontent.com/basilwhite/ivn/main/docs/agent-system/IVN_Agent_Recruitment_System_v2.md",
    "cost_model": "https://raw.githubusercontent.com/basilwhite/ivn/main/docs/agent-system/IVN_Fable51_Cost_Model.md",
    "quality_gate": "https://raw.githubusercontent.com/basilwhite/ivn/main/docs/agent-system/IVN_Quality_Gate_Design.md",
    "alignment_validation": "https://raw.githubusercontent.com/basilwhite/ivn/main/docs/agent-system/IVN_Alignment_Validation_Design.md",
    "handoff_prompt": "https://raw.githubusercontent.com/basilwhite/ivn/main/docs/agent-system/HANDOFF_PROMPT.md",
    "manifest": "https://raw.githubusercontent.com/basilwhite/ivn/main/MANIFEST.json"
  },
  ...
}

Why: When Claude fetches briefing.json at session start, the doc
URLs are right there. Claude fetches whichever docs the session needs
without Basil having to paste anything.

---

## ACTION 4: Update basilwhite.com/ivn/ state files for three-tier model

The current state.json has single-tier structure. Update it to:

{
  "generated_at": "2026-09-13T00:00:00Z",
  "budget_date": "2026-09-13",
  "tier_a": {
    "model": "claude-sonnet-4-6",
    "daily_budget_usd": 5.00,
    "daily_spend_usd": 0.00,
    "budget_halted": false
  },
  "tier_b": {
    "model": "claude-fable-5-1",
    "api_mode": "batch",
    "daily_budget_usd": 10.00,
    "daily_spend_usd": 0.00,
    "budget_halted": false,
    "enabled": false
  },
  "tier_c": {
    "model": "claude-fable-5-1",
    "api_mode": "standard",
    "daily_budget_usd": 15.00,
    "daily_spend_usd": 0.00,
    "budget_halted": false,
    "cache_warmed": false,
    "cache_warm_cost_usd": 0.00,
    "enabled": false
  },
  "last_sweep_timestamp": null,
  "last_sweep_trigger": null
}

Upload updated state.json to basilwhite.com/ivn/ via FTP.

---

## ACTION 5: Keep the Revenue Development Plan in Google Drive

Already done. Google Doc ID: 1nLV50VncZFap5Hvoy2uxBs2U-7Gf4OtfxKaXfMGRJzk
Claude can read this via the Google Drive connector in any session.
No action needed — just keep the connector enabled.

---

## SUMMARY: Three Recovery Paths for Future Sessions

PATH A — GitHub fetch (fastest for architecture docs):
  Claude fetches raw.githubusercontent.com/basilwhite/ivn/main/docs/agent-system/
  Has full architecture in 2 tool calls.

PATH B — basilwhite.com/ivn/ fetch (fastest for session state):
  Claude fetches briefing.json → finds doc URLs → fetches what's needed.
  Has full state + architecture in 3 tool calls.

PATH C — Google Drive (fastest for revenue plan):
  Claude reads Google Doc ID directly via Drive connector.
  Has full revenue plan in 1 tool call.

PATH D — Memory files (always available, summaries only):
  /areas/ivn-agent-recruitment-system.md
  /areas/ivn-monetization.md
  /areas/ivn.md
  These hold summaries — enough context to know which docs to fetch.

---

## FILE CHECKLIST

[ ] /docs/agent-system/ directory created in basilwhite/ivn repo
[ ] IVN_Agent_Recruitment_System_v2.md committed
[ ] IVN_Fable51_Cost_Model.md committed
[ ] IVN_Quality_Gate_Design.md committed
[ ] IVN_Alignment_Validation_Design.md committed
[ ] IVN_Cost_Ceiling_Design.md committed
[ ] HANDOFF_PROMPT.md committed (paste text from earlier in session)
[ ] MANIFEST.json committed to repo root
[ ] briefing.json updated with agent_system_docs URLs
[ ] state.json updated with three-tier structure
[ ] Updated state.json uploaded to basilwhite.com/ivn/ via FTP
[ ] Updated briefing.json uploaded to basilwhite.com/ivn/ via FTP
