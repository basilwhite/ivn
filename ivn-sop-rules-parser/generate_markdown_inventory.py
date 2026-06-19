import json

def create_markdown_inventory(json_path, md_path):
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
                f.write(f"- **{component.get('component', 'N/A')}:** {component.get('description', 'No description provided.')}\n")
            f.write("\n")

    print(f"Successfully created Markdown inventory at {md_path}")

if __name__ == "__main__":
    create_markdown_inventory('m_25_22_components.json', 'm_25_22_components.md')