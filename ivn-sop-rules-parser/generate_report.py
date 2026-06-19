import json

def generate_strategic_report(alignments_path, report_path, top_n=5):
    """
    Generates a strategic, human-readable leadership report from the scored alignments.
    """
    with open(alignments_path, 'r', encoding='utf-8') as f:
        alignments = json.load(f)

    # Group alignments by M-25-22 component
    grouped_alignments = {}
    for align in alignments:
        component_title = align['m_25_22_component']['component']
        if component_title not in grouped_alignments:
            grouped_alignments[component_title] = []
        grouped_alignments[component_title].append(align)

    with open(report_path, 'w', encoding='utf-8') as f:
        # --- Executive Summary ---
        f.write("# Strategic Alignment Report: M-25-22 and IVN\n\n")
        f.write("**Date:** April 28, 2026\n\n")
        f.write("## 1. Executive Summary\n\n")
        f.write("This report details the strategic alignment between OMB Memorandum M-25-22, \"Driving Efficient Acquisition of Artificial Intelligence in Government,\" and the USDA's Internal Vision for the Nation (IVN) database. Our analysis, using a keyword-based scoring model, reveals significant synergies, particularly concerning **risk management, compliance, and system authorization**.\n\n")
        f.write("The key takeaway is that USDA's existing governance frameworks and data sources, as cataloged in the IVN, provide a strong foundation for meeting the AI acquisition requirements mandated by M-25-22. By leveraging these existing assets, USDA can ensure a compliant, secure, and efficient AI adoption process.\n\n")
        f.write("This document groups the most significant alignments by their relevant M-25-22 components and concludes with a consolidated set of actionable recommendations for leadership to capitalize on these findings.\n\n")
        f.write("---\n\n")

        # --- Alignments by Component ---
        f.write("## 2. Alignments by M-25-22 Component\n\n")
        f.write("The following sections detail the specific alignments found between M-25-22 requirements and existing USDA and Federal guidance within the IVN database.\n\n")

        # Focus on a few high-impact components for the detailed report body
        high_impact_components = [
            "Contracts must ensure compliance with minimum risk management practices for high-impact use cases as required under M-25-21.",
            "Any AI systems and services operated as an information system by or on behalf of an agency must receive an authorization to operate."
        ]

        for component_title in high_impact_components:
            if component_title in grouped_alignments:
                f.write(f"### Component: {component_title}\n\n")
                f.write("**Analysis:** This requirement is directly supported by several foundational OMB circulars and NIST frameworks present in our IVN. These documents establish the government-wide standards for risk management, information security, and privacy upon which our AI acquisition strategies can be built.\n\n")
                f.write("**Key Alignments:**\n\n")
                f.write("| IVN Source Name | Alignment Score |\n")
                f.write("|---|---|\n")
                
                # Sort the alignments for this component by score
                sorted_comp_alignments = sorted(grouped_alignments[component_title], key=lambda x: x['alignment_score'], reverse=True)
                
                for align in sorted_comp_alignments[:top_n]:
                    f.write(f"| {align['ivn_component']} | {align['alignment_score']} |\n")
                f.write("\n")

        f.write("---\n\n")

        # --- Consolidated Recommendations ---
        f.write("## 3. Consolidated Recommendations\n\n")
        f.write("To leverage these strategic alignments, we propose the following actionable recommendations for leadership:\n\n")
        f.write("| Recommendation ID | Recommendation | M-25-22 Component(s) Addressed |\n")
        f.write("|:---|:---|:---|\n")
        f.write("| REC-01 | **Integrate AI Risk into ERM:** Formally integrate the NIST AI Risk Management Framework (AI RMF 1.0) into the agency's existing Enterprise Risk Management (ERM) processes as defined by OMB Circular A-123. | 4.d.iii.D |\n")
        f.write("| REC-02 | **Update ATO Procedures for AI:** Direct the OCIO to update the agency's Authorization to Operate (ATO) procedures to include specific controls and considerations for AI systems, referencing NIST SP 800-37 and SP 800-53. | 4.e.i |\n")
        f.write("| REC-03 | **Develop AI-Specific Contract Language:** Create standardized contract clauses for AI acquisitions that explicitly require compliance with M-25-21 and reference the risk management practices found in the aligned OMB memoranda. | 4.d.iii.D |\n")
        f.write("| REC-04 | **Mandate Cross-Functional Teams:** For all AI procurements, mandate the formation of cross-functional teams including representatives from acquisition, legal, privacy, and cybersecurity to ensure all facets of risk and compliance are addressed from the outset. | 3.b, 4.a.i |\n")

    print(f"Successfully generated strategic leadership report at {report_path}")

if __name__ == "__main__":
    generate_strategic_report('alignments.json', 'Strategic_Alignment_Report.md')
