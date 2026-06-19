import re
import json

def process_executive_order(input_path, output_path):
    """
    Parses the plain text of Executive Order 14240, extracts key sections
    and subsections, and saves them as a structured JSON file.
    """
    with open(input_path, 'r', encoding='utf-8') as f:
        text = f.read()

    components = []
    
    # Split the document into sections
    sections = re.split(r'Sec\.\s\d+\.\s', text)
    
    # The first split part is the preamble before "Sec. 1."
    preamble = sections[0].strip()
    if preamble:
        components.append({
            "section_number": "Preamble",
            "section_title": "Preamble",
            "component": preamble
        })

    # Process each subsequent section
    for i, section_content in enumerate(sections[1:], start=1):
        lines = section_content.strip().split('\n')
        full_title = lines[0].strip()
        
        # Extract section title
        section_title_match = re.match(r'([A-Za-z\s]+)\.', full_title)
        section_title = section_title_match.group(1).strip() if section_title_match else f"Section {i}"

        # Use regex to find subsections like (a), (b), (i), (ii)
        subsections = re.split(r'\s+\([a-z0-9]+\)\s+', section_content)
        
        if len(subsections) > 1:
            # The first part of the split is the section's introductory text
            if subsections[0].strip():
                components.append({
                    "section_number": str(i),
                    "section_title": section_title,
                    "component": subsections[0].strip()
                })
            # Add each subsection as a separate component
            sub_labels = re.findall(r'\s+\(([a-z0-9]+)\)\s+', section_content)
            for j, sub_content in enumerate(subsections[1:]):
                if sub_content.strip():
                    label = sub_labels[j].replace('(', '').replace(')', '')
                    components.append({
                        "section_number": f"{i}.{label}",
                        "section_title": section_title,
                        "component": sub_content.strip()
                    })
        else:
            # If no subsections, the whole content is one component
            if section_content.strip():
                components.append({
                    "section_number": str(i),
                    "section_title": section_title,
                    "component": section_content.strip()
                })

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(components, f, indent=4)

    print(f"Successfully parsed Executive Order and saved {len(components)} components to {output_path}")

if __name__ == "__main__":
    process_executive_order('eo_14240_text.txt', 'eo_14240_components.json')
