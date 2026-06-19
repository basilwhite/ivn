"""CLI for interacting with the governance graph."""
import typer
import json
from typing_extensions import Annotated

from gov_graph.loaders import seed_loader
from gov_graph.queries import ancestry, compliance, heatmap, impact, proposals
from gov_graph.viz import graph_viz
from gov_graph.config import GRAPH_BACKEND

app = typer.Typer(help="Legal Genealogy Graph CLI")
query_app = typer.Typer(help="Run pre-defined queries.")
app.add_typer(query_app, name="query")
proposal_app = typer.Typer(help="Build and validate evidence-based proposal packages.")
app.add_typer(proposal_app, name="proposal")

def print_json(data):
    """Helper to pretty-print JSON."""
    print(json.dumps(data, indent=2))

@app.command()
def load(
    backend: Annotated[str, typer.Option(help="Override the backend for this run.")] = None
):
    """Load all data into the graph."""
    backend_to_use = backend if backend else GRAPH_BACKEND
    print(f"Loading data for backend: {backend_to_use}...")
    seed_loader.load_all(backend_override=backend)
    print("Data loading complete.")

@query_app.command("lineage-memo")
def query_lineage_memo():
    """Query the full lineage from Constitution to USDA via M-26-10."""
    print_json(ancestry.full_lineage_memo_to_usda())

@query_app.command("compliance-usda")
def query_compliance_usda():
    """Query USDA's compliance surface."""
    print_json({
        "bound_provisions": compliance.usda_bound_provisions(),
        "issued_directives": compliance.usda_directives(),
    })

@query_app.command("impact-cio")
def query_impact_cio():
    """Query the CIO's compliance surface and impact paths."""
    print_json({
        "cio_compliance_surface": compliance.cio_compliance_surface(),
        "m2610_impact_paths": impact.cio_m2610_impact_paths(),
    })

@query_app.command("heatmap-memo")
def query_heatmap_memo():
    """Query the M-26-10 influence heatmap."""
    print_json(heatmap.get_m2610_influence_heatmap())


@proposal_app.command("sample-brief")
def proposal_sample_brief(
    audience: Annotated[
        str,
        typer.Option(help="Audience mode: executive, oversight, or implementation."),
    ] = "executive",
):
    """Build and evaluate a sample proposal package with communication guidance."""
    package = proposals.build_proposal_package(
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
    brief = proposals.build_communication_brief(package, audience=audience)
    print_json({"package": package, "communication_brief": brief})

@app.command()
def viz(
    subset: Annotated[str, typer.Option(help="Render a specific subgraph: 'retirement' or 'memo'.")] = "memo"
):
    """Generate and open a graph visualization."""
    if subset == 'retirement':
        print("Generating retirement slice visualization...")
        graph_viz.visualize_retirement_slice()
    elif subset == 'memo':
        print("Generating M-26-10 lineage visualization...")
        graph_viz.visualize_m2610_lineage()
    else:
        print(f"Unknown subset: {subset}")

if __name__ == "__main__":
    app()
