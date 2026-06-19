import pandas as pd
import re

def find_alignments(m2522_components, ivn_database):
    """
    Finds alignments between M-25-22 components and the IVN database
    using manually selected keywords and phrases.
    """
    alignments = {}

    for section, items in m2522_components.items():
        alignments[section] = []
        for item in items:
            # Manually selected keywords/phrases for each component
            # This is a more targeted approach
            keyword_map = {
                "Ensure vendor sourcing, data portability, and long-term interoperability to avoid vendor lock-in.": ["vendor lock-in", "data portability", "interoperability"],
                "Communicate clear and specific requirements.": ["requirements"],
                "Track AI performance and manage risk.": ["performance", "risk management", "ai risk"],
                "Ensure AI systems are fit for purpose and deliver consistent results.": ["fit for purpose", "consistent results"],
                "Promote Effective AI Acquisition:": ["acquisition", "procurement"],
                "Foster cross-functional engagement (acquisition, IT, cybersecurity, privacy, civil rights, etc.).": ["cross-functional", "cybersecurity", "privacy", "civil rights"],
                "Update Agency Policies (within 270 days):": ["policy", "acquisition"],
                "Maximize Use of American-Made AI.": ["american-made", "buy american"],
                "Protect Privacy:": ["privacy", "pii"],
                "Protect IP Rights and Use of Government Data:": ["intellectual property", "ip rights", "government data"],
                "Spotlight AI Acquisition (GSA, within 100 days):": ["gsa", "acquisition guide"],
                "Contribute to Shared Repository (GSA, within 200 days):": ["gsa", "repository", "best practices"],
                "Determine Necessary Disclosures of AI Use:": ["disclosure", "ai use"],
                "Identification of Requirements:": ["requirements", "high-impact"],
                "Market Research & Planning:": ["market research", "performance-based"],
                "Solicitation Development:": ["solicitation", "vendor lock-in", "high-impact"],
                "Selection and Award:": ["testing", "evaluation", "ip rights"],
                "Contract Administration:": ["authorization to operate", "ato", "oversight"],
                "Contract Closeout:": ["vendor lock-in", "data portability"]
            }
            
            # Get the keywords for the current item, if any
            keywords = keyword_map.get(item.strip().lstrip('- '), [])

            for keyword in keywords:
                # Use word boundaries for more precise matching
                matches = ivn_database[ivn_database['source_name'].str.lower().str.contains(r'\b{}\b'.format(re.escape(keyword)), regex=True)]
                if not matches.empty:
                    for index, row in matches.iterrows():
                        alignment = {
                            "m2522_component": item,
                            "ivn_database_source": row['source_name'],
                            "keyword_match": keyword
                        }
                        if alignment not in alignments[section]:
                            alignments[section].append(alignment)
    return alignments

def write_alignment_report(alignments, output_file):
    """
    Writes the alignment report to a file.
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("M-25-22 and IVN Database Alignment Report (v3)\n")
        f.write("=============================================\n\n")
        for section, alignment_list in alignments.items():
            if alignment_list:
                f.write(f"## {section}\n\n")
                for alignment in alignment_list:
                    f.write(f"- **M-25-22 Component:** {alignment['m2522_component']}\n")
                    f.write(f"- **IVN Database Source:** {alignment['ivn_database_source']}\n")
                    f.write(f"- **Keyword Match:** {alignment['keyword_match']}\n\n")

def extract_m2522_components(file_path):
    """
    Extracts components from the M-25-22 text file.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    components = {}
    sections = content.split('\n## ')[1:] 
    for section in sections:
        lines = section.strip().split('\n')
        section_title = lines[0]
        items = [line.strip() for line in lines[1:] if line.strip().startswith('- ')]
        components[section_title] = items
    return components

if __name__ == "__main__":
    m2522_components_file = "M-25-22_components.txt"
    ivn_database_file = r"C:\Users\Basil.White\OneDrive - USDA\OCIO-STRATUS Governance Document Working Group - Documents\USDA-IVN-dataset.xlsx"
    alignment_report_file = "alignment_report_v3.txt"

    # Extract M-25-22 components
    m2522_components = extract_m2522_components(m2522_components_file)

    # Read IVN database
    ivn_database = pd.read_excel(ivn_database_file)
    
    # Filter IVN database for relevant agencies
    relevant_agencies = ['USDA', 'OMB', 'GSA', 'NIST']
    ivn_database = ivn_database[ivn_database['source_agency'].isin(relevant_agencies)]

    # Find alignments
    alignments = find_alignments(m2522_components, ivn_database)

    # Write the report
    write_alignment_report(alignments, alignment_report_file)

    print(f"Alignment report generated: {alignment_report_file}")
