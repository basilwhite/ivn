"""FastAPI application for the governance graph."""
from fastapi import Body, FastAPI, HTTPException
from gov_graph.loaders import seed_loader
from gov_graph.queries import ancestry, compliance, heatmap, impact, proposals

app = FastAPI(
    title="IVN Genealogy Graph API",
    description="API for querying the legal genealogy graph.",
    version="0.1.0",
)

@app.get("/health", summary="Health Check")
def health_check():
    """Check if the API is running."""
    return {"status": "ok"}

@app.post("/load/all", summary="Load All Data")
def load_all_data():
    """
    Trigger the loading of all data into the graph.
    """
    try:
        seed_loader.load_all()
        return {"message": "Data loading process started successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/lineage/m2610", summary="M-26-10 Full Lineage")
def get_m2610_lineage():
    """Get the full lineage from Constitution to USDA directives via M-26-10."""
    return ancestry.full_lineage_memo_to_usda()

@app.get("/compliance/usda", summary="USDA Compliance Surface")
def get_usda_compliance():
    """List all provisions and directives binding USDA."""
    return {
        "bound_provisions": compliance.usda_bound_provisions(),
        "issued_directives": compliance.usda_directives(),
    }

@app.get("/impact/cio", summary="CIO Impact and Compliance")
def get_cio_impact():
    """Get the CIO's compliance surface and impact paths from M-26-10."""
    return {
        "cio_compliance_surface": compliance.cio_compliance_surface(),
        "m2610_impact_paths": impact.cio_m2610_impact_paths(),
    }

@app.get("/heatmap/m2610", summary="M-26-10 Influence Heatmap")
def get_m2610_heatmap():
    """Get influence scores for USDA directives related to M-26-10."""
    return heatmap.get_m2610_influence_heatmap()


@app.post("/proposals/validate", summary="Validate Proposal Package")
def validate_proposal(
    proposal_package: dict = Body(...),
    audience: str = "executive",
):
    """Validate a proposal package and return audience-specific communication guidance."""
    issues = proposals.validate_proposal_package(proposal_package)
    normalized = dict(proposal_package)
    normalized["validation_issues"] = issues
    normalized["is_decision_ready"] = len(issues) == 0

    try:
        brief = proposals.build_communication_brief(normalized, audience=audience)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {
        "is_decision_ready": normalized["is_decision_ready"],
        "validation_issues": issues,
        "communication_brief": brief,
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
