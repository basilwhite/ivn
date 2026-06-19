# ivn_simple_text_parser.py

import re
import pandas as pd
from datetime import datetime

def extract_eo_components_from_text(eo_text, source_url, source_title):
    """
    Extracts components from the raw text of an Executive Order.
    This is a simple parser focusing on identifying sections and subsections.
    """
    components = []
    
    # A simple regex to find sections, which are the main components
    # This regex looks for "Section" followed by a number and a period.
    section_pattern = re.compile(r"(Sec\.\s\d+\.\s.*?(?=\nSec\.\s\d+\.|\Z))", re.DOTALL)
    
    sections = section_pattern.findall(eo_text)
    
    for i, section_text in enumerate(sections):
        section_text = section_text.strip()
        if not section_text:
            continue
            
        # The first line is the title
        lines = section_text.split('\\n')
        component_title = lines[0].strip()
        
        # The rest is the description
        component_description = "\\n".join(lines[1:]).strip()
        
        components.append({
            "Source": source_title,
            "Component": component_title,
            "Component Description": component_description,
            "Component URL": source_url,
        })
        
    return components

def save_components_to_excel(components, source_title):
    """
    Saves the extracted components to an Excel file.
    """
    if not components:
        print("No components were extracted.")
        return

    df = pd.DataFrame(components)
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"ivn_components_{source_title.replace(' ', '_')}_{timestamp}.xlsx"
    
    with pd.ExcelWriter(filename) as writer:
        df.to_excel(writer, sheet_name="Extracted Components", index=False)
        
    print(f"Successfully saved {len(components)} components to {filename}")
    return filename

def main():
    """
    Main function to extract components from the EO text and save them to an Excel file.
    """
    eo_text = """
Executive Order 14240 of March 21, 2025
Eliminating Waste and Saving Taxpayer Dollars by Consolidating Procurement

By the authority vested in me as President by the Constitution and the laws of the United States of America, and in order to promote economy and efficiency in Federal Government procurement, it is hereby ordered as follows:

Section 1. Policy. It is the policy of the United States Government to eliminate waste and maximize the value of taxpayer dollars. A key way to achieve this is by consolidating procurement practices across the Federal Government to leverage the Government's significant buying power. By centralizing procurement for common goods and services, we can reduce duplication, increase efficiency, drive down prices, and create a more unified and modern acquisition system. This order directs agencies to take steps to expand the use of strategic sourcing, category management, and other multi-agency procurement methods.

Sec. 2. Definitions. For purposes of this order:

(a) "Agency" has the meaning given to it in 44 U.S.C. 3502(1).

(b) "Category management" means the business practice of buying common goods and services as an enterprise to eliminate redundancies, increase efficiency, and deliver more value and savings from the Government's acquisition programs.

(c) "Strategic sourcing" means a structured and collaborative process of critically analyzing an organization's spending patterns to better leverage its purchasing power, reduce costs, and improve overall performance.

(d) "Shared services" means the consolidation of administrative or support functions from across multiple agencies or organizations into a single, shared provider to reduce costs and improve service quality.

Sec. 3. Consolidation of Procurement and Expansion of Shared Services.

(a) The Administrator of the Office of Federal Procurement Policy (OFPP), in coordination with the Administrator of the General Services Administration (GSA) and the Director of the Office of Management and Budget (OMB), shall lead a Government-wide initiative to consolidate procurement and expand the use of shared services for common goods and services.

(b) Within 90 days of the date of this order, the Administrator of OFPP shall issue guidance to agencies on the implementation of category management principles. This guidance shall include a framework for identifying common procurement areas suitable for consolidation and establishing Government-wide acquisition strategies.

(c) Within 180 days of the date of this order, the head of each agency shall submit a plan to the Director of OMB and the Administrator of OFPP. This plan shall:

(i) Identify at least three new categories of procurement to be managed through a Government-wide strategic sourcing or category management approach.

(ii) Detail the agency's strategy for increasing its use of existing Government-wide contract solutions and shared services for common requirements.

(iii) Establish clear goals and metrics for measuring cost savings, reduction in duplicative contracts, and improvements in efficiency.

Sec. 4. Improving Data and Transparency.

(a) The GSA Administrator, in consultation with the Administrator of OFPP, shall develop and maintain a centralized data analytics platform to provide agencies with comprehensive data on Government-wide spending. This platform shall enable agencies to identify opportunities for consolidation and track progress against their procurement goals.

(b) Agencies shall cooperate with the GSA Administrator to provide the necessary procurement data to populate and maintain this platform.

(c) The Director of OMB shall, as appropriate and consistent with applicable law, make aggregated procurement data publicly available to increase transparency and accountability.

Sec. 5. General Provisions.

(a) Nothing in this order shall be construed to impair or otherwise affect:

(i) the authority granted by law to an executive department or agency, or the head thereof; or

(ii) the functions of the Director of the Office of Management and Budget relating to budgetary, administrative, or legislative proposals.

(b) This order shall be implemented consistent with applicable law and subject to the availability of appropriations.

(c) This order is not intended to, and does not, create any right or benefit, substantive or procedural, enforceable at law or in equity by any party against the United States, its departments, agencies, or entities, its officers, employees, or agents, or any other person.

THE WHITE HOUSE,

March 21, 2025.
"""
    
    source_document_name = "Executive Order 14240"
    source_url = "https://www.federalregister.gov/executive-order/14240"
    source_title = "Executive Order 14240"
    
    components = extract_eo_components_from_text(eo_text, source_url, source_title)
    if components:
        save_components_to_excel(components, source_title)

if __name__ == "__main__":
    main()
