import json
import re

def parse_components_from_text(txt_path, json_path):
    """
    Parses the M-25-22 components from a structured text file into a JSON format.
    """
    with open(txt_path, 'r') as f:
        content = f.read()

    # This parsing logic is based on the structure observed in M-25-22_components.txt
    # It may need to be adjusted if the text file format varies.
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
            if component_match:
                component_name = component_match.group(1).strip()
                inventory[current_category].append({
                    "component": component_name,
                    "description": "" # Description will be added from subsequent lines if available
                })
        elif line.startswith('- ') and current_category and inventory[current_category]:
            # This assumes the description follows the component line
            description = line.replace('- ', '').strip()
            # Append to the last component's description
            if inventory[current_category][-1]["description"] == "":
                inventory[current_category][-1]["description"] = description
            else:
                # If a component has multi-line description
                inventory[current_category][-1]["description"] += " " + description


    with open(json_path, 'w') as f:
        json.dump(inventory, f, indent=4)

    print(f"Successfully parsed {txt_path} and created {json_path}")

if __name__ == "__main__":
    parse_components_from_text('M-25-22_components.txt', 'm_25_22_components.json')
