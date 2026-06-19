import re
import openpyxl
from openpyxl.workbook import Workbook

def parse_text_file(file_path):
    """
    Parses a text file and extracts the components.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='latin-1') as f:
            text = f.read()

    components = []
    current_pillar = ""
    current_section = ""

    pillar_regex = re.compile(r"Pillar (I|II|III): (.*)")
    action_regex = re.compile(r"^\W\s*(.*)") # More robust action regex

    lines = text.split('\n')
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue

        pillar_match = pillar_regex.match(line)
        if pillar_match:
            current_pillar = f"Pillar {pillar_match.group(1)}: {pillar_match.group(2)}"
            current_section = ""  # Reset section on new pillar
            continue

        action_match = action_regex.match(line)
        if action_match:
            action_text = action_match.group(1).strip()
            
            # Find the section header
            if not current_section:
                for j in range(i - 2, -1, -1): # Look 2 lines above for section
                    prev_line = lines[j].strip()
                    if prev_line and prev_line.istitle() and not pillar_regex.match(prev_line):
                        current_section = prev_line
                        break
            
            if not current_section:
                current_section = "Uncategorized"

            source_id = "America's AI Action Plan July 2025"
            component_name = action_text
            component_id = f"{component_name}|{source_id}"

            components.append({
                'component_name': component_name,
                'component_description': action_text,
                'component_url': '',
                'component_agency': '',
                'component_ofc_of_primary_interest': '',
                'source_id': source_id,
                'component_id': component_id,
                'fetch_status': ''
            })
        elif "Recommended Policy Actions" in line:
            # The line before this is likely the section title
            if i > 0:
                section_line = lines[i-1].strip()
                if section_line and section_line.istitle() and not pillar_regex.match(section_line):
                    current_section = section_line

    return components

def create_excel_file(components, output_filename):
    """
    Creates an Excel file with a 'Components' sheet.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Components"

    headers = [
        'component_name',
        'component_description',
        'component_url',
        'component_agency',
        'component_ofc_of_primary_interest',
        'source_id',
        'component_id',
        'fetch_status'
    ]
    ws.append(headers)

    for component in components:
        row = [component.get(h, '') for h in headers]
        ws.append(row)

    wb.save(output_filename)

if __name__ == "__main__":
    text_file_path = 'c:\\Users\\Basil.White\\OneDrive - USDA\\OCIO-STRATUS Governance Document Working Group - Documents\\Americas-AI-Action-Plan.txt'
    output_excel_file = 'Americas-AI-Action-Plan-IVN-Inventory-generated.xlsx'
    
    parsed_components = parse_text_file(text_file_path)
    create_excel_file(parsed_components, output_excel_file)
    print(f"Generated '{output_excel_file}' with {len(parsed_components)} components.")
