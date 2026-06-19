# IVN Genealogy Graph

This project implements a legal genealogy graph that models American governance as a family tree. It uses a graph database to represent the complex relationships between legal documents, provisions, and institutions.

The suite includes:
- **Graph Backends**: Support for both Neo4j (via Docker) and Kùzu (embedded).
- **Data Loaders**: Scripts to ingest data from Cypher files and CSVs.
- **Query Packs**: Pre-defined queries for ancestry, compliance, and impact analysis.
- **Proposal Utilities**: A structured proposal package builder for evidence-based governance recommendations.
- **API**: A FastAPI application to expose query endpoints.
- **CLI**: A Typer-based command-line interface for data loading and querying.
- **Visualization**: A Streamlit application and Pyvis for interactive graph exploration.
- **Tests**: A `pytest` suite to ensure correctness.

## Quick Start

### 1. Environment Setup

First, copy the example environment file and fill in your details.

```bash
cp .env.example .env
```

You will need to set the `NEO4J_PASSWORD` if you are using the Neo4j backend.

### 2. Install Dependencies

This project uses Poetry for dependency management.

```bash
poetry install
```

### 3. Running with Neo4j (Docker)

This is the recommended setup for a robust, multi-user environment.

**Start the Neo4j container:**
```bash
docker-compose up -d
```

**Set the backend environment variable:**
```bash
export GRAPH_BACKEND=neo4j
```

**Load the data:**
You can use the CLI or the API to load the data.

*Using the CLI:*
```bash
poetry run python -m gov_graph.cli.app load
```

*Using the API:*
First, start the API server:
```bash
poetry run uvicorn gov_graph.api.main:app --reload
```
Then, send a POST request to the `/load/all` endpoint (e.g., using `curl` or the API docs at `http://localhost:8000/docs`).

### 4. Running with Kùzu (Embedded)

This setup is ideal for local, single-user exploration without Docker.

**Set the backend and database path environment variables:**
```bash
export GRAPH_BACKEND=kuzu
export KUZU_DB_PATH=./kuzu_db
```

**Load the data:**
```bash
poetry run python -m gov_graph.cli.app load
```

## How to Use

### API

Start the FastAPI server:
```bash
poetry run uvicorn gov_graph.api.main:app --reload
```
Access the interactive API documentation at [http://localhost:8000/docs](http://localhost:8000/docs).

### CLI

The CLI provides a convenient way to load data and run queries.

```bash
# Load data
poetry run python -m gov_graph.cli.app load

# Run a query
poetry run python -m gov_graph.cli.app query lineage-memo
```
Run `poetry run python -m gov_graph.cli.app --help` for a full list of commands.

### Visualization (Streamlit)

The Streamlit app provides an interactive dashboard for running queries and viewing results.

**Run the app:**
```bash
poetry run streamlit run gov_graph/viz/graph_viz.py
```
Access the dashboard in your browser (usually at `http://localhost:8501`).

### Jupyter Notebooks

The `notebooks/` directory contains examples of how to use the query packs for analysis.

- `01_explore_lineage.ipynb`: Traces the lineage of OMB M-26-10.
- `02_compliance_surface.ipynb`: Explores USDA and CIO compliance surfaces.
- `03_heatmap.ipynb`: Generates an influence heatmap for M-26-10.

## Extending the Graph

This project is designed to be extensible. Here are some ways you could build upon it:

- **Add More Agencies/Directives**: Create new CSV files in `data/` and corresponding loaders in `gov_graph/loaders/`.
- **Materialize Ancestry**: For performance, you could create a loader that pre-computes and stores `ANCESTOR_OF` relationships.
- **Add Employee Logic**: Extend the schema to include employees and write queries to determine things like earliest retirement dates based on the legal framework.

## Evidence-Based Governance Proposals

This repository now includes a standardized proposal package utility for converting IVN alignments into decision-ready governance recommendations.

- Python utility: `gov_graph/queries/proposals.py`
- Protocol: `docs/evidence_proposal_protocol.md`
- Communication guide: `docs/communication_playbook.md`
- Reusable template: `templates/proposal_package_template.md`

The proposal package includes:

1. Decision Question
2. Evidence Graph
3. Authority Chain (policy, program, regulation, Executive Order, Public Law)
4. Change Hypothesis
5. Impact Analysis (first-order and second-order)
6. Alternatives (including no-change baseline)
7. Confidence and Source Trace
8. Implementation Path

You can validate and communication-tune packages via API and CLI.

### API Validation Endpoint

POST `/proposals/validate`

- Accepts a proposal package as JSON body.
- Supports `audience` query parameter: `executive`, `oversight`, `implementation`.
- Returns readiness, validation issues, and an audience-specific communication brief.

### CLI Communication Brief

```bash
poetry run python -m gov_graph.cli.app proposal sample-brief --audience executive
poetry run python -m gov_graph.cli.app proposal sample-brief --audience oversight
poetry run python -m gov_graph.cli.app proposal sample-brief --audience implementation
```

### Example (Python)

```python
from gov_graph.queries.proposals import build_proposal_package

package = build_proposal_package(
	decision_question="Should directive section 4 be revised to align with control requirements?",
	evidence_graph=[{"enabling": "AL-001", "dependent": "AL-004", "effect": "authorization"}],
	authority_chain={
		"policy": ["Department Policy A"],
		"program": ["Program Directive B"],
		"regulation": ["7 CFR Part X"],
		"executive_order": ["EO 14000"],
		"public_law": ["Pub. L. 123-45"],
	},
	change_hypothesis="Clarifying section language reduces compliance ambiguity.",
	impact_analysis={
		"first_order": ["Reduced contradictory guidance"],
		"second_order": ["Improved audit consistency"],
		"risks": ["Temporary retraining burden"],
	},
	alternatives=[
		{"option": "No change", "tradeoffs": "Retains ambiguity."},
		{"option": "Revise section 4", "tradeoffs": "Needs retraining."},
	],
	implementation_path=[
		{"step": 1, "owner": "Policy Office", "action": "Draft revision"},
		{"step": 2, "owner": "OCIO", "action": "Validate control mapping"},
	],
	confidence_score=0.86,
	source_trace=[{"alignment_id": "AL-001", "snapshot": {"source": "m2610_alignment.csv"}}],
)

print(package["is_decision_ready"], package["validation_issues"])
```

## Acceptance Criteria

The following criteria should be met for a successful deployment:

- [ ] **Loaders execute without error** on both Neo4j and Kùzu backends.
- [ ] The API endpoint `GET /lineage/m2610` returns the five implementing USDA directives.
- [ ] The heatmap endpoint (`GET /heatmap/m2610`) returns influence scores, with directly implementing directives ranked higher than those included only by category.
