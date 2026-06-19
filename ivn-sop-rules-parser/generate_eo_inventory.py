import json

def generate_markdown_inventory(json_path, md_path):
    """
    Reads a JSON file containing components of an executive order and
    creates a human-readable Markdown inventory.
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    with open(md_path, 'w', encoding='utf-8') as f:
        f.write("# Inventory of Executive Order 14240 Components\n\n")
        f.write("This document provides a structured inventory of the key components extracted from Executive Order 14240, \"Eliminating Waste and Saving Taxpayer Dollars by Consolidating Procurement.\"\n\n")
        f.write("---\n\n")

        for component in data:
            section_number = component.get('section_number', 'N/A')
            section_title = component.get('section_title', 'N/A')
            text = component.get('component', 'No content.')

            f.write(f"## Section {section_number}: {section_title}\n\n")
            f.write(f"**Component Text:**\n")
            f.write(f"> {text}\n\n")
            f.write("---\n\n")

    print(f"Successfully created Markdown inventory at {md_path}")

if __name__ == "__main__":
    generate_markdown_inventory('eo_14240_components.json', 'eo_14240_components.md')
