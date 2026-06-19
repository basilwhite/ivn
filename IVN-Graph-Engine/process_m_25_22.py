import json
import re

def parse_components_from_text(txt_path, json_path):
    """
    Parses the M-25-22 components from a structured text file into a JSON format.
    """
    with open(txt_path, 'r', encoding='utf-8') as f:
        content = f.read()

    inventory = {}
    current_category = None
    lines = content.split('\n')

    for line in lines:
        line = line.strip()
        if line.startswith('## '):
            current_category = line.replace('## ', '').strip()
            inventory[current_category] = []
        elif line.startswith('- **'):
            component_match = re.match(r'- \*\*(.*?):\*\*', line)
            if component_match and current_category:
                component_name = component_match.group(1).strip()
                description = line.split(':**', 1)[1].strip()
                inventory[current_category].append({
                    "component": component_name,
                    "description": description
                })
        elif line.startswith('- ') and current_category and inventory[current_category]:
            # This handles cases where the description is on a new line without the bolded component.
            # It appends to the last component's description.
            description_part = line.replace('- ', '').strip()
            if inventory[current_category][-1]["description"]:
                 inventory[current_category][-1]["description"] += " " + description_part
            else:
                 inventory[current_category][-1]["description"] = description_part


    with open(json_path, 'w') as f:
        json.dump(inventory, f, indent=4)

    print(f"Successfully parsed {txt_path} and created {json_path}")

if __name__ == "__main__":
    # Note: The input file is the Markdown file, which has the necessary structure.
    parse_components_from_text('M-25-22_components_inventory.md', 'm_25_22_components.json')
