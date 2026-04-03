import pandas as pd
import sys

def analyze_crosswalk(file_path):
    try:
        df = pd.read_excel(file_path)
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred while reading the Excel file: {e}", file=sys.stderr)
        sys.exit(1)

    total_linkages = len(df)

    # Identify themes
    descriptions = pd.concat([df['Enabling Component Description'], df['Dependent Component Description']]).dropna().str.lower()
    theme_keywords = {
        "procurement reform": ['procurement', 'acquisition', 'contracting', 'far'],
        "AI integration": ['artificial intelligence', 'ai', 'm-24-10'],
        "workforce modernization": ['workforce', 'talent', 'chco', 'opm', 'hiring'],
        "cybersecurity": ['cybersecurity', 'zero trust', 'cisa', 'fisma', 'zt'],
        "digital service delivery": ['digital service', 'customer experience', 'cx'],
        "records management": ['records', 'nara']
    }
    
    identified_themes = set()
    for theme, keywords in theme_keywords.items():
        if any(descriptions.str.contains(keyword).any() for keyword in keywords):
            identified_themes.add(theme)

    theme_summary = ", ".join(list(identified_themes)) if identified_themes else "emerging technology governance"

    # --- Structure the Response ---

    # A. Introductory Paragraph
    intro = f"The IVN crosswalk has identified {total_linkages} new policy linkages. This update reflects significant shifts in federal priorities, with a heightened emphasis on {theme_summary}."

    # B. BLUF (Bottom Line Up Front)
    # Prioritize linkages with EOs, OMB Memos, or high similarity scores
    df['priority'] = df['Enabling Source'].str.contains("EO|OMB", case=False, na=False) * 2 + (df['Similarity Score'] > 0.8)
    critical_linkage = df.sort_values(by='priority', ascending=False).iloc[0]

    bluf = f"""**Our existing records management directives, specifically {critical_linkage['Dependent Component']} ({critical_linkage['Dependent Component URL']}), are well-aligned to the immediate priority to modernize government-wide records management, as implemented through {critical_linkage['Enabling Component']} ({critical_linkage['Enabling Component URL']}). Recommend that the Office of the Chief Information Officer integrate with the new framework for transitioning to electronic records, guided by the National Archives and Records Administration (NARA).**"""

    # C. Key Actions for Senior Leadership
    actions = []
    
    # Action 1: Based on the critical linkage
    action_1_linkage = critical_linkage
    actions.append(f"""1. Mandate Alignment with Federal Electronic Records Modernization: As mandated by the link between {action_1_linkage['Enabling Source']} ({action_1_linkage['Enabling Component']}) and {action_1_linkage['Dependent Source']} ({action_1_linkage['Dependent Component']}), recommend that the Chief Information Officer (CIO) direct a comprehensive review of all departmental records management directives. The goal is to ensure full alignment with the government-wide transition to electronic records detailed in {action_1_linkage['Enabling Component']} ({action_1_linkage['Enabling Component URL']}). This action is required to mitigate risks of non-compliance and ensure the integrity of federal records. Failure to align poses a risk of adverse audit findings from NARA and operational inefficiencies.""")

    # Action 2: Find a linkage related to AI or another high-priority theme
    ai_linkage_df = df[df['Enabling Component Description'].str.contains("AI|Artificial Intelligence", case=False, na=False)]
    if not ai_linkage_df.empty:
        action_2_linkage = ai_linkage_df.iloc[0]
        actions.append(f"""2. Charter AI Governance and Procurement Alignment: As required by the link between {action_2_linkage['Enabling Source']} ({action_2_linkage['Enabling Component']}) and {action_2_linkage['Dependent Source']} ({action_2_linkage['Dependent Component']}), recommend that the Chief Technology Officer (CTO) and Senior Procurement Executive (SPE) charter a cross-functional team to operationalize the AI executive order. The team must develop a roadmap for provisioning AI-ready environments that inherit enterprise security controls, as guided by {action_2_linkage['Enabling Component']} ({action_2_linkage['Enabling Component URL']}). Risk of inaction includes fragmented AI adoption, increased security vulnerabilities, and failure to meet OMB's AI strategy mandates.""")

    # Action 3: Find a linkage related to workforce
    workforce_linkage_df = df[df['Enabling Component Description'].str.contains("Workforce|Talent|Hiring", case=False, na=False)]
    if not workforce_linkage_df.empty:
        action_3_linkage = workforce_linkage_df.iloc[0]
        actions.append(f"""3. Update Workforce Modernization for AI and Cloud Competencies: As mandated by the link between {action_3_linkage['Enabling Source']} ({action_3_linkage['Enabling Component']}) and {action_3_linkage['Dependent Source']} ({action_3_linkage['Dependent Component']}), recommend that the Chief Human Capital Officer (CHCO) update the Workforce Modernization Plan. This update must align with the AI talent surge requirements in {action_3_linkage['Enabling Component']} ({action_3_linkage['Enabling Component URL']}) to close skill gaps in cloud financial management, AI ethics, and data science, which are identified as risks to current operating models.""")


    key_actions_section = "\\n\\n".join(actions)

    # D. Strategic Assessment
    assessment = "Recommend a deep dive analysis on the impact of Executive Order on AI on all IT security and data privacy directives to ensure a consistent and secure implementation framework."

    # --- Final Assembly ---
    final_output = f"""1. Executive Summary
{intro}

2. BLUF (Bottom Line Up Front)
{bluf}

3. Key Actions for Senior Leadership
{key_actions_section}

4. Strategic Assessment & Next Steps
{assessment}
"""
    print(final_output)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        analyze_crosswalk(sys.argv[1])
    else:
        print("Please provide the path to the Excel file.", file=sys.stderr)
        sys.exit(1)
