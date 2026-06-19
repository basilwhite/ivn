"""Unit tests for evidence-based proposal package utilities."""

from gov_graph.queries import proposals


def test_confidence_label_thresholds():
    assert proposals.confidence_label(0.80) == "High"
    assert proposals.confidence_label(0.79) == "Medium"
    assert proposals.confidence_label(0.50) == "Medium"
    assert proposals.confidence_label(0.49) == "Low"


def test_build_proposal_package_decision_ready():
    package = proposals.build_proposal_package(
        decision_question="Should policy X be updated to align with control Y?",
        evidence_graph=[{"enabling": "A1", "dependent": "B4", "effect": "authorization"}],
        authority_chain={
            "policy": ["Dept Policy X"],
            "program": ["Program Directive Y"],
            "regulation": ["7 CFR Z"],
            "executive_order": ["EO 14000"],
            "public_law": ["Pub. L. 123-45"],
        },
        change_hypothesis="Clarifying section 4 removes conflict and improves compliance consistency.",
        impact_analysis={
            "first_order": ["Reduced contradictory implementation guidance"],
            "second_order": ["Improved audit consistency"],
            "risks": ["Temporary training burden"],
        },
        alternatives=[
            {"option": "No change", "tradeoffs": "Preserves current ambiguity."},
            {"option": "Revise section 4", "tradeoffs": "Requires retraining."},
        ],
        implementation_path=[
            {"step": 1, "owner": "Policy Office", "action": "Draft update"},
            {"step": 2, "owner": "OCIO", "action": "Validate control alignment"},
        ],
        confidence_score=0.86,
        source_trace=[{"alignment_id": "AL-001", "snapshot": {"source": "m2610_alignment.csv"}}],
    )

    assert package["is_decision_ready"] is True
    assert package["validation_issues"] == []
    assert package["confidence"]["label"] == "High"


def test_build_communication_brief_executive_mode():
    package = proposals.build_proposal_package(
        decision_question="Should policy X be updated?",
        evidence_graph=[{"enabling": "A1", "dependent": "B4", "effect": "authorization"}],
        authority_chain={
            "policy": ["Dept Policy X"],
            "program": ["Program Directive Y"],
            "regulation": ["7 CFR Z"],
            "executive_order": ["EO 14000"],
            "public_law": ["Pub. L. 123-45"],
        },
        change_hypothesis="Update improves consistency.",
        impact_analysis={"first_order": [], "second_order": [], "risks": []},
        alternatives=[
            {"option": "No change", "tradeoffs": "Keep current state."},
            {"option": "Change", "tradeoffs": "Requires rollout."},
        ],
        implementation_path=[{"step": 1, "owner": "Policy Office", "action": "Draft"}],
        confidence_score=0.82,
        source_trace=[{"alignment_id": "AL-001", "snapshot": {"source": "m2610_alignment.csv"}}],
    )

    brief = proposals.build_communication_brief(package, audience="executive")
    assert brief["audience"] == "executive"
    assert len(brief["key_points"]) >= 4


def test_validate_proposal_package_flags_authority_chain_missing_layers():
    package = {
        "decision_question": "Test",
        "evidence_graph": [{"enabling": "A", "dependent": "B", "effect": "compliance"}],
        "authority_chain": {"policy": ["P-1"]},
        "change_hypothesis": "Hypothesis",
        "impact_analysis": {"first_order": [], "second_order": [], "risks": []},
        "alternatives": [
            {"option": "No change", "tradeoffs": "Status quo"},
            {"option": "Change", "tradeoffs": "Implementation overhead"},
        ],
        "confidence": {"score": 0.8, "label": "High"},
        "source_trace": [{"alignment_id": "AL-1"}],
        "implementation_path": [{"step": 1, "owner": "Team", "action": "Review"}],
    }

    issues = proposals.validate_proposal_package(package)
    assert "Authority chain missing layer: program" in issues
    assert "Authority chain missing layer: regulation" in issues


def test_build_proposal_package_flags_missing_requirements():
    package = proposals.build_proposal_package(
        decision_question="Should regulation language be revised?",
        evidence_graph=[{"enabling": "A1", "dependent": "B9", "effect": "compliance"}],
        authority_chain={"regulation": ["7 CFR X"]},
        change_hypothesis="A narrow revision reduces implementation conflict.",
        impact_analysis={"first_order": [], "second_order": [], "risks": []},
        alternatives=[{"option": "Revise", "tradeoffs": "Needs legal review."}],
        implementation_path=[{"step": 1, "owner": "GC", "action": "Review language"}],
        confidence_score=0.60,
        source_trace=[],
    )

    assert package["is_decision_ready"] is False
    assert len(package["validation_issues"]) == 2
    assert "At least two alternatives are required" in package["validation_issues"][0]
    assert "Source trace must include at least one evidence reference." in package["validation_issues"][1]
