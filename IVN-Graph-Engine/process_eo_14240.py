import json
import re

def parse_eo_from_text(txt_path, json_path):
    """
    Parses the Executive Order 14240 components from a structured text file into a JSON format.
    """
    with open(txt_path, 'r', encoding='utf-8') as f:
        content = f.read()

    inventory = {}
    current_section = None
    lines = content.split('\n')

    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Match section headers like "Section 1. Policy." or "Sec. 2. Definitions."
        section_match = re.match(r'^(?:Section|Sec\.)\s(\d+)\.\s(.*?)\.?$', line, re.IGNORECASE)
        if section_match:
            section_number = section_match.group(1)
            section_title = section_match.group(2).strip()
            current_section = f"Section {section_number}: {section_title}"
            inventory[current_section] = []
        elif line.startswith('-') and current_section:
            # Treat each bullet point as a component
            component_text = line.lstrip('- ').strip()
            # A simple way to create a "component" name is to take the first few words.
            component_name = ' '.join(component_text.split()[:5]) + '...'
            inventory[current_section].append({
                "component": component_name,
                "description": component_text
            })

    with open(json_path, 'w') as f:
        json.dump(inventory, f, indent=4)

    print(f"Successfully parsed {txt_path} and created {json_path}")

if __name__ == "__main__":
    parse_eo_from_text('eo_14240_components.txt', 'eo_14240_components.json')
