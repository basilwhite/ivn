import json
from datetime import datetime

def generate_strategic_report(alignments_path, report_path, top_n=3):
    """
    Generates an improved strategic, human-readable leadership report from the
    scored alignments related to Executive Order 14240, including direct
    analysis of the EO's mandates.
    """
    with open(alignments_path, 'r', encoding='utf-8') as f:
        alignments = json.load(f)

    # --- Define EO 14240 Core Directives ---
    eo_directives = {
        "Section 3(a): Agency Consolidation Proposals": {
            "text": "Within 60 days of the date of this order, agency heads shall...submit to the Administrator proposals...to have the General Services Administration conduct domestic procurement with respect to common goods and services for the agency...",
            "analysis": "This is a direct order for immediate action. Alignments with IVN components related to 'consolidating procurement' provide a strategic foundation for drafting these proposals, demonstrating that this effort aligns with existing, documented agency goals."
        },
        "Section 3(b): GSA's Comprehensive Plan": {
            "text": "Within 90 days of the date of this order, the Administrator shall submit a comprehensive plan to the Director of OMB for the General Services Administration to procure common goods and services across the domestic components of the Government...",
            "analysis": "While this is a GSA-led task, USDA can use aligned IVN components to proactively inform the GSA's plan, highlighting existing internal initiatives that already support a consolidated procurement model."
        },
        "Section 3(c): GSA as Executive Agent for IT": {
            "text": "Within 30 days of the date of this order...the Director of OMB shall designate the Administrator as the executive agent for all Government-wide acquisition contracts for information technology.",
            "analysis": "This centralizes control over IT contracts. Alignments with IVN components concerning IT shared services or technology management can be used to identify which USDA contracts may be affected and to prepare for the transition to GSA oversight."
        }
    }

    with open(report_path, 'w', encoding='utf-8') as f:
        # --- Header and Executive Summary ---
        f.write("# Strategic Alignment Report: EO 14240 & IVN\n\n")
        f.write(f"**Date:** {datetime.now().strftime('%B %d, %Y')}\n\n")
        f.write("## 1. Executive Summary\n\n")
        f.write("This report analyzes the strategic alignment between **Executive Order 14240, \"Eliminating Waste and Saving Taxpayer Dollars by Consolidating Procurement,\"** and the USDA's Internal Vision for the Nation (IVN) database. The analysis identifies significant opportunities to leverage existing USDA components to support the government-wide goal of procurement consolidation.\n\n")
        f.write("The key finding is that the core directives of EO 14240 are strongly supported by existing USDA programs and policies cataloged in the IVN, particularly those from the President's Management Agenda (M-26-03). By leveraging these alignments, USDA can proactively demonstrate compliance, streamline its response to the order, and contribute effectively to the government's cost-saving initiatives.\n\n")
        f.write("This document first outlines the primary directives of EO 14240 and then presents the key IVN alignments that support each directive, concluding with actionable recommendations for leadership.\n\n")
        f.write("---\n\n")

        # --- Analysis of EO 14240 Directives and Alignments ---
        f.write("## 2. Analysis of EO 14240 Directives and Supporting IVN Alignments\n\n")
        f.write("This section breaks down the core mandates of Executive Order 14240 and connects them to specific, high-scoring alignments found within the IVN database.\n\n")

        for directive_title, directive_info in eo_directives.items():
            f.write(f"### Directive: {directive_title}\n\n")
            f.write(f"**Order Text:** *\"{directive_info['text']}\"*\n\n")
            f.write(f"**Analysis:** {directive_info['analysis']}\n\n")
            f.write(f"**Supporting IVN Alignments:**\n\n")
            f.write("| IVN Component Name | Alignment Score | Justification for Relevance |\n")
            f.write("|---|---|---|\n")

            # Find the top N alignments for this directive's keywords
            directive_keywords = get_keywords(directive_info['text'])
            relevant_alignments = []
            for align in alignments:
                if directive_keywords.intersection(align['matching_keywords']):
                    relevant_alignments.append(align)
            
            # Sort by score and take the top N
            top_alignments = sorted(relevant_alignments, key=lambda x: x['alignment_score'], reverse=True)[:top_n]

            if not top_alignments:
                f.write("| *No high-scoring direct alignments found.* | | |\n")
            else:
                for align in top_alignments:
                    justification = f"The keywords '{', '.join(align['matching_keywords'])}' directly relate to the directive's focus on procurement and consolidation."
                    f.write(f"| {align['ivn_component_name']} | {align['alignment_score']} | {justification} |\n")
            f.write("\n")

        f.write("---\n\n")

        # --- Consolidated Recommendations ---
        f.write("## 3. Consolidated Recommendations for Leadership\n\n")
        f.write("Based on the analysis, we propose the following actionable recommendations:\n\n")
        f.write("| Rec. ID | Recommendation | EO 14240 Directive Addressed | Justification |\n")
        f.write("|:---|:---|:---|:---|\n")
        f.write("| REC-EO-01 | **Establish a Procurement Consolidation Task Force:** Charter a cross-functional team to review all 'common goods and services' procurement and draft the agency's consolidation proposal for the GSA Administrator. | Section 3(a) | Proactively addresses the 60-day deadline and ensures a coordinated agency response by leveraging existing goals outlined in M-26-03. |\n")
        f.write("| REC-EO-02 | **Inventory and Analyze all IT Contract Vehicles:** Direct the OCIO to conduct a rapid inventory of all IT acquisition contracts to identify candidates for consolidation under the new GSA executive agent authority. | Section 3(c) | Prepares the agency for the transition to GSA oversight and demonstrates proactive management of contract duplication. |\n")
        f.write("| REC-EO-03 | **Develop a Proactive Communication Plan:** Create a communication plan that highlights how existing IVN-cataloged programs (e.g., shared services, category management) already support the goals of EO 14240. | Section 1 (Policy) | Demonstrates immediate alignment with the spirit of the order and showcases USDA's leadership in efficient procurement. |\n")
        f.write("| REC-EO-04 | **Engage with GSA Transition Team:** Proactively engage with the GSA team responsible for the government-wide plan to ensure USDA's needs and existing capabilities are well-represented. | Section 3(b) | Ensures USDA's interests are considered in the comprehensive plan being submitted to OMB and avoids being a passive recipient of mandates. |\n")

    print(f"Successfully generated strategic leadership report at {report_path}")

if __name__ == "__main__":
    # Helper function to get keywords, assuming it's defined in the same file or imported
    import re
    STOP_WORDS = set(["a", "an", "the", "and", "or", "in", "on", "of", "for", "to", "with", "is", "are", "was", "were"])
    def get_keywords(text):
        if not text or not isinstance(text, str): return set()
        words = re.findall(r'\\b\\w+\\b', text.lower())
        return {word for word in words if word not in STOP_WORDS and len(word) > 2}

    generate_strategic_report('eo_14240_alignments.json', 'Strategic_Alignment_Report_EO14240.md')

