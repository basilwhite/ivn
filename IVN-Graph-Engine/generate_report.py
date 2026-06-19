import json

def generate_leadership_report(alignments_path, report_path):
    """
    Generates an improved, human-readable leadership report from the alignments JSON data.
    """
    try:
        with open(alignments_path, 'r') as f:
            alignments = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading or parsing alignments file: {e}")
        return

    with open(report_path, 'w', encoding='utf-8') as f:
        # --- Executive Summary ---
        f.write("# M-25-22 Strategic Alignment Report\n\n")
        f.write("## Executive Summary\n\n")
        f.write("This report outlines strategic alignments between **OMB Memorandum M-25-22** and the **IVN database**. The analysis reveals opportunities to leverage existing IVN components to support M-25-22's goals for efficient and responsible AI acquisition. Key findings and actionable recommendations are detailed below.\n\n")
        f.write("---\n\n")

        # --- Group Alignments ---
        grouped_alignments = {}
        for alignment in alignments:
            m2522_comp_name = alignment['m_25_22_component']['component']
            if m2522_comp_name not in grouped_alignments:
                grouped_alignments[m2522_comp_name] = []
            grouped_alignments[m2522_comp_name].append(alignment)

        # --- Alignments by M-25-22 Component ---
        f.write("## Alignments by M-25-22 Component\n\n")
        if not grouped_alignments:
            f.write("No significant alignments were found based on the current criteria.\n\n")
        else:
            for m2522_comp, comp_alignments in grouped_alignments.items():
                f.write(f"### M-25-22 Component: {m2522_comp}\n\n")
                # Sort alignments within the group by score
                comp_alignments.sort(key=lambda x: x['alignment_score'], reverse=True)
                for align in comp_alignments[:3]: # Show top 3 alignments
                    f.write(f"- **IVN Component:** {align['ivn_component']}\n")
                    f.write(f"  - **Alignment Score:** {align['alignment_score']}\n")
                    f.write(f"  - **Common Keywords:** `{', '.join(align['common_keywords'])}`\n")
                    f.write(f"  - **IVN Description:** {align['ivn_description']}\n\n")
                if len(comp_alignments) > 3:
                    f.write(f"  - *...and {len(comp_alignments) - 3} more alignments.*\n\n")
        f.write("---\n\n")

        # --- Appendix: Consolidated Recommendations ---
        f.write("## Appendix: Consolidated Recommendations\n\n")
        f.write("| M-25-22 Component | IVN Component | Recommendation |\n")
        f.write("|---|---|---|\n")
        
        recommendation_count = 0
        for m2522_comp, comp_alignments in grouped_alignments.items():
            if recommendation_count >= 30: break
            for align in comp_alignments:
                if recommendation_count >= 30: break
                recommendation = "Review IVN component to ensure it fully supports the principles of M-25-22. Enhance documentation and communication to highlight this alignment."
                ivn_comp_display = (align['ivn_component'][:75] + '...') if len(align['ivn_component']) > 78 else align['ivn_component']
                m2522_comp_display = (m2522_comp[:75] + '...') if len(m2522_comp) > 78 else m2522_comp
                f.write(f"| {m2522_comp_display} | {ivn_comp_display} | {recommendation} |\n")
                recommendation_count += 1

    print(f"Successfully generated leadership report at {report_path}")

if __name__ == "__main__":
    generate_leadership_report('alignments.json', 'Strategic_Alignment_Report.md')
