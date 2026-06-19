import json
import csv

def convert_json_to_csv(json_path, csv_path):
    """
    Converts the semantic alignments from a JSON file to a CSV file,
    flattening the nested structure for easier analysis.
    """
    print(f"Converting {json_path} to {csv_path}...")

    with open(json_path, 'r', encoding='utf-8') as f:
        alignments = json.load(f)

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        # Define the header for the CSV file
        header = [
            'eo_section_number',
            'eo_section_title',
            'eo_component_text',
            'ivn_component_name',
            'ivn_component_description',
            'alignment_score'
        ]
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()

        # Write each alignment as a row in the CSV
        for align in alignments:
            row = {
                'eo_section_number': align.get('eo_14240_component', {}).get('section_number', ''),
                'eo_section_title': align.get('eo_14240_component', {}).get('section_title', ''),
                'eo_component_text': align.get('eo_14240_component', {}).get('component', ''),
                'ivn_component_name': align.get('ivn_component_name', ''),
                'ivn_component_description': align.get('ivn_component_description', ''),
                'alignment_score': align.get('alignment_score', 0.0)
            }
            writer.writerow(row)

    print(f"Successfully converted {len(alignments)} alignments to {csv_path}")

if __name__ == "__main__":
    convert_json_to_csv(
        'eo_14240_semantic_alignments.json',
        'eo_14240_semantic_alignments.csv'
    )
