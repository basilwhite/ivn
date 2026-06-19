import json

def generate_inventory_md(json_path, md_path):
    """
    Converts a JSON inventory of M-25-22 components into a structured
    and human-readable Markdown file.
    """
    with open(json_path, 'r') as f:
        data = json.load(f)

    with open(md_path, 'w', encoding='utf-8') as f:
        f.write("# M-25-22 Components Inventory\n\n")
        f.write("This document inventories the key components, requirements, and directives from OMB Memorandum M-25-22, 'Driving Efficient Acquisition of Artificial Intelligence in Government.'\n\n")

        for category, components in data.items():
            f.write(f"## {category}\n")
            if not components:
                f.write("- No specific components listed.\n")
            for component in components:
                # Ensure description is not empty before writing
                description = component.get('description', 'No description provided.')
                if description:
                    f.write(f"- **{component.get('component', 'N/A')}:** {description}\n")
                else:
                    f.write(f"- **{component.get('component', 'N/A')}**\n")
            f.write("\n")

    print(f"Successfully created Markdown inventory at {md_path}")

if __name__ == "__main__":
    generate_inventory_md('m_25_22_components.json', 'M-25-22_components_inventory.md')
