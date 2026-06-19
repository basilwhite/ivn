"""Utilities for building and validating evidence-based governance proposals."""
from __future__ import annotations

from typing import Any

AUTHORITY_LAYERS = ["policy", "program", "regulation", "executive_order", "public_law"]


def confidence_label(score: float) -> str:
    """Return the standard confidence label for a numeric score."""
    if score < 0 or score > 1:
        raise ValueError("confidence_score must be between 0.00 and 1.00")
    if score >= 0.80:
        return "High"
    if score >= 0.50:
        return "Medium"
    return "Low"


def validate_proposal_package(package: dict[str, Any]) -> list[str]:
    """Validate the proposal package against baseline governance quality checks."""
    issues: list[str] = []

    required_fields = [
        "decision_question",
        "evidence_graph",
        "authority_chain",
        "change_hypothesis",
        "impact_analysis",
        "alternatives",
        "confidence",
        "source_trace",
        "implementation_path",
    ]

    for field in required_fields:
        if field not in package:
            issues.append(f"Missing required field: {field}")

    authority_chain = package.get("authority_chain", {})
    for layer in AUTHORITY_LAYERS:
        if layer not in authority_chain:
            issues.append(f"Authority chain missing layer: {layer}")

    alternatives = package.get("alternatives", [])
    if len(alternatives) < 2:
        issues.append("At least two alternatives are required (including no-change baseline).")

    confidence = package.get("confidence", {})
    score = confidence.get("score")
    if score is None:
        issues.append("Confidence score is required.")
    else:
        try:
            expected_label = confidence_label(float(score))
            if confidence.get("label") != expected_label:
                issues.append(
                    "Confidence label does not match score threshold. "
                    f"Expected '{expected_label}'."
                )
        except (TypeError, ValueError) as exc:
            issues.append(str(exc))

    source_trace = package.get("source_trace", [])
    if not source_trace:
        issues.append("Source trace must include at least one evidence reference.")

    return issues


def build_communication_brief(package: dict[str, Any], audience: str = "executive") -> dict[str, Any]:
    """Generate a concise communication brief from a proposal package."""
    audience_modes = {"executive", "oversight", "implementation"}
    if audience not in audience_modes:
        raise ValueError("audience must be one of: executive, oversight, implementation")

    decision_question = package.get("decision_question", "Decision question not provided")
    readiness = package.get("is_decision_ready", False)
    confidence = package.get("confidence", {})

    points = [
        f"Decision: {decision_question}",
        f"Readiness: {'Decision-ready' if readiness else 'Needs additional evidence or structure'}",
        f"Confidence: {confidence.get('score', 'N/A')} ({confidence.get('label', 'N/A')})",
    ]

    if audience == "executive":
        points.extend([
            "Focus: mission impact, urgency, and decision ask.",
            "Include no-change baseline and preferred option tradeoff.",
        ])
    elif audience == "oversight":
        points.extend([
            "Focus: authority-chain completeness and legal defensibility.",
            "Include source trace references for each major claim.",
        ])
    else:
        points.extend([
            "Focus: owners, sequencing, dependencies, and measures.",
            "Include risk mitigations and near-term execution milestones.",
        ])

    return {
        "audience": audience,
        "key_points": points,
        "validation_issues": package.get("validation_issues", []),
    }


def build_proposal_package(
    decision_question: str,
    evidence_graph: list[dict[str, Any]],
    authority_chain: dict[str, Any],
    change_hypothesis: str,
    impact_analysis: dict[str, Any],
    alternatives: list[dict[str, Any]],
    implementation_path: list[dict[str, Any]],
    confidence_score: float,
    source_trace: list[dict[str, Any]],
    assumptions: list[str] | None = None,
) -> dict[str, Any]:
    """Build a standardized proposal package from IVN alignment evidence."""
    package = {
        "decision_question": decision_question,
        "evidence_graph": evidence_graph,
        "authority_chain": authority_chain,
        "change_hypothesis": change_hypothesis,
        "impact_analysis": impact_analysis,
        "alternatives": alternatives,
        "confidence": {
            "score": confidence_score,
            "label": confidence_label(confidence_score),
        },
        "source_trace": source_trace,
        "implementation_path": implementation_path,
        "assumptions": assumptions or [],
    }

    package["validation_issues"] = validate_proposal_package(package)
    package["is_decision_ready"] = len(package["validation_issues"]) == 0
    return package
