
import pandas as pd
import re

def get_m2522_components():
    """
    This function will hold the detailed breakdown of M-25-22 requirements.
    This needs to be populated with the actual granular requirements from the text.
    For now, it contains more detailed placeholders than the previous version.
    """
    return {
        "3b": "Maximize the use of AI products and services that are developed and produced in the United States.",
        "3d": "Ensure compliance with privacy requirements in law and policy whenever agencies acquire an AI system or service.",
        "3e_i": "Scope licensing and other IP rights appropriately, based on the intended use of AI, to avoid vendor lock-in.",
        "3e_iv": "Ensure contracts permanently prohibit the use of non-public inputted agency data and outputted results to further train publicly or commercially available AI algorithms, absent explicit agency consent.",
        "4a_i": "Convene a cross-functional team to inform the procurement of AI systems or services.",
        "4a_ii": "Identify reasonably foreseeable use cases arising from the use of an AI system or service and determine if it is likely to host high-impact AI use cases.",
        "4b_i": "Conduct thorough market research to seek state-of-the-art AI capabilities.",
        "4b_ii": "Seek detailed demonstrations and tests of potentially useful AI systems or services in scenarios that closely reflect the intended real-world operating environment.",
        "4c_i": "Disclose in solicitations whether a planned use of an AI system meets the threshold of a high-impact use case.",
        "4d_i": "Test proposed solutions to understand the capabilities and limitations of any offered AI system or service.",
        "4d_iii_C": "Include contract terms that reduce the risk that switching vendors could become cost-prohibitive (vendor lock-in).",
        "4d_iii_E": "Include contractual terms that provide the contracting agency the ability to regularly monitor and evaluate performance, risks, and effectiveness of an AI system or service.",
        "4e_i": "Any AI systems and services operated as an information system by or on behalf of an agency must receive an authorization to operate (ATO).",
        "4e_iv": "Determine criteria for sunsetting the use of an AI system."
    }

def analyze_ivn_database(file_path):
    """
    Reads the IVN database from an Excel file and returns the DataFrame.
    """
    try:
        df = pd.read_excel(file_path)
        # Basic data cleaning: forward fill common columns that might be merged in Excel
        for col in ['ID', 'Parent ID', 'Name', 'Description', 'Category']:
            if col in df.columns:
                df[col] = df[col].ffill()
        return df
    except FileNotFoundError:
        print(f"Error: IVN database file not found at {file_path}")
        return None
    except Exception as e:
        print(f"An error occurred while reading the IVN database: {e}")
        return None

def discover_alignments(m2522_components, ivn_df):
    """
    Discovers and rationalizes specific component-to-component alignments.
    """
    alignments = []
    if ivn_df is None:
        return alignments

    # Convert dataframe to a list of dictionaries for easier iteration
    ivn_components = ivn_df.to_dict('records')

    for m_id, m_req in m2522_components.items():
        for ivn_comp in ivn_components:
            rationale = []
            ivn_id = ivn_comp.get('ID', 'N/A')
            ivn_name = str(ivn_comp.get('Name', ''))
            ivn_desc = str(ivn_comp.get('Description', ''))
            ivn_text = ivn_name + " " + ivn_desc

            # Heuristic 1: Privacy
            if re.search(r'\bprivacy\b', m_req, re.IGNORECASE) and re.search(r'\b(privacy|pii|personally identifiable information)\b', ivn_text, re.IGNORECASE):
                rationale.append(f"IVN component '{ivn_name}' (ID: {ivn_id}) mentions privacy-related terms, aligning with M-25-22's focus on privacy protection.")

            # Heuristic 2: IP & Data Rights
            if re.search(r'\b(ip rights|licensing|data)\b', m_req, re.IGNORECASE) and re.search(r'\b(data rights|ip|intellectual property|license|proprietary)\b', ivn_text, re.IGNORECASE):
                rationale.append(f"IVN component '{ivn_name}' (ID: {ivn_id}) addresses data or IP, which is a key concern in M-25-22 regarding government data usage and IP rights.")

            # Heuristic 3: High-Impact AI & Risk
            if re.search(r'\b(high-impact|risk)\b', m_req, re.IGNORECASE) and re.search(r'\b(risk|impact|critical|sensitive)\b', ivn_text, re.IGNORECASE):
                rationale.append(f"IVN component '{ivn_name}' (ID: {ivn_id}) relates to risk or impact, aligning with M-25-22's requirement to identify and manage high-impact AI.")

            # Heuristic 4: Testing & Performance
            if re.search(r'\b(test|monitor|performance|evaluate)\b', m_req, re.IGNORECASE) and re.search(r'\b(test|performance|monitoring|evaluation|metric|benchmark)\b', ivn_text, re.IGNORECASE):
                rationale.append(f"IVN component '{ivn_name}' (ID: {ivn_id}) involves testing or performance monitoring, a specific requirement for contract administration in M-25-22.")
            
            # Heuristic 5: Vendor Lock-in
            if re.search(r'\b(vendor lock-in|interoperability|portability)\b', m_req, re.IGNORECASE) and re.search(r'\b(open source|standard|api|export|portable|interoperable)\b', ivn_text, re.IGNORECASE):
                rationale.append(f"IVN component '{ivn_name}' (ID: {ivn_id}) mentions standards or interoperability, which aligns with M-25-22's goal to prevent vendor lock-in.")

            if rationale:
                alignments.append({
                    "m2522_id": m_id,
                    "m2522_requirement": m_req,
                    "ivn_id": ivn_id,
                    "ivn_name": ivn_name,
                    "rationale": " ".join(rationale)
                })

    return alignments

def generate_report(alignments):
    """
    Generates a detailed report for leadership.
    """
    report = "# M-25-22 Component Alignment Report (V2)\n\n"
    report += "## Introduction\n"
    report += "This report provides a detailed, component-to-component alignment between the Information Value Network (IVN) database and the specific requirements of memorandum M-25-22, 'Driving Efficient Acquisition of Artificial Intelligence in Government'. Each alignment includes a rationale to support the connection.\n\n"
    
    if alignments:
        report += "## Specific Component Alignments\n\n"
        # Sort by M-25-22 ID for a structured report
        sorted_alignments = sorted(alignments, key=lambda x: x['m2522_id'])
        for alignment in sorted_alignments:
            report += f"### M-25-22 Requirement (Section {alignment['m2522_id']})\n"
            report += f"**Requirement:** *{alignment['m2522_requirement']}*\n\n"
            report += f"**Aligned IVN Component:**\n"
            report += f"- **ID:** {alignment['ivn_id']}\n"
            report += f"- **Name:** {alignment['ivn_name']}\n\n"
            report += f"**Rationale for Alignment:**\n"
            report += f"{alignment['rationale']}\n"
            report += "---\n"
    else:
        report += "## No Specific Alignments Found\n"
        report += "No specific component-to-component alignments were discovered between the IVN database and M-25-22 requirements based on the implemented heuristics. This suggests a potential gap in the IVN's documented relevance to AI acquisition policy.\n\n"

    report += "## Recommendations for Leadership\n"
    if alignments:
        report += "Based on the specific alignments found, we recommend the following actions:\n"
        report += "1.  **Validate Alignments:** Convene subject matter experts to validate the accuracy of these machine-generated alignments and refine the rationale.\n"
        report += "2.  **Develop Compliance Narratives:** For each validated alignment, develop a clear narrative explaining how the specified IVN component is used to meet the corresponding M-25-22 requirement. This will be critical for demonstrating compliance.\n"
        report += "3.  **Prioritize Action:** Focus on the aligned IVN components. If they are policies, ensure they are being enforced. If they are systems, ensure their performance and risk management data is being collected as per M-25-22.\n"
        report += "4.  **Enhance IVN Data:** Update the IVN database to include a dedicated field for 'Policy Alignment' to make this process more robust and repeatable for future governance documents.\n"
    else:
        report += "Given the lack of discoverable alignments, we recommend:\n"
        report += "1.  **Manual Review:** Initiate a targeted manual review of the IVN portfolio against M-25-22's requirements. The current data may lack the necessary keywords for automated discovery.\n"
        report += "2.  **Data Enrichment Initiative:** Launch a project to enrich the IVN database. Add explicit metadata to each component that details its purpose, data handling properties, risk posture, and relationship to federal policies.\n"
        report += "3.  **Strategic Gap Analysis:** The lack of alignment may indicate a strategic gap. Assess whether new governance components need to be created within the IVN to address the specific demands of AI acquisition policy.\n"

    return report

def main():
    ivn_file_path = "ivn-genealogy/data/USDA-IVN-dataset.xlsx"
    
    m2522_components = get_m2522_components()
    ivn_df = analyze_ivn_database(ivn_file_path)
    
    alignments = discover_alignments(m2522_components, ivn_df)
    
    report = generate_report(alignments)
    
    report_path = "m2522_alignment_report_v2.md"
    with open(report_path, "w", encoding='utf-8') as f:
        f.write(report)
        
    print(f"Successfully generated the V2 alignment report: {report_path}")

if __name__ == "__main__":
    main()
