import pandas as pd
import re

def extract_m2522_components(file_path):
    """
    Extracts components from the M-25-22 text file.
    This is a simple extraction based on the structure of the components file.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    components = {}
    sections = content.split('\n## ')[1:] 
    for section in sections:
        lines = section.strip().split('\n')
        section_title = lines[0]
        items = [line.strip() for line in lines[1:] if line.strip().startswith('- ')]
        components[section_title] = items
    return components

def find_alignments(m2522_components, ivn_database):
    """
    Finds alignments between M-25-22 components and the IVN database.
    """
    alignments = {}
    # More comprehensive stop words list
    stop_words = set([
        'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't", 'as', 'at',
        'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by',
        'can', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 'down', 'during',
        'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 'his', 'how', "how's",
        'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself',
        "let's", 'me', 'more', 'most', "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'over', 'own',
        'same', "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such',
        'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very',
        'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't",
        'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves'
    ])

    for section, items in m2522_components.items():
        alignments[section] = []
        for item in items:
            # Extract more meaningful phrases (e.g., "vendor lock-in", "data portability")
            # This regex looks for sequences of capitalized words or nouns/adjectives.
            # It's still basic but better than single words.
            phrases = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b|\b[a-z-]{3,}\b', item)
            keywords = [p.lower() for p in phrases if p.lower() not in stop_words]

            for keyword in keywords:
                # Use word boundaries for more precise matching
                matches = ivn_database[ivn_database['source_name'].str.lower().str.contains(r'\b{}\b'.format(re.escape(keyword)), regex=True)]
                if not matches.empty:
                    for index, row in matches.iterrows():
                        alignment = {
                            "m2522_component": item,
                            "ivn_database_source": row['source_name'],
                            "keyword_match": keyword
                        }
                        if alignment not in alignments[section]:
                            alignments[section].append(alignment)
    return alignments

def write_alignment_report(alignments, output_file):
    """
    Writes the alignment report to a file.
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("M-25-22 and IVN Database Alignment Report\n")
        f.write("=========================================\n\n")
        for section, alignment_list in alignments.items():
            if alignment_list:
                f.write(f"## {section}\n\n")
                for alignment in alignment_list:
                    f.write(f"- **M-25-22 Component:** {alignment['m2522_component']}\n")
                    f.write(f"- **IVN Database Source:** {alignment['ivn_database_source']}\n")
                    f.write(f"- **Keyword Match:** {alignment['keyword_match']}\n\n")

if __name__ == "__main__":
    m2522_components_file = "M-25-22_components.txt"
    ivn_database_file = r"C:\Users\Basil.White\OneDrive - USDA\OCIO-STRATUS Governance Document Working Group - Documents\USDA-IVN-dataset.xlsx"
    alignment_report_file = "alignment_report_v2.txt"

    # Extract M-25-22 components
    m2522_components = extract_m2522_components(m2522_components_file)

    # Read IVN database
    ivn_database = pd.read_excel(ivn_database_file)

    # Find alignments
    alignments = find_alignments(m2522_components, ivn_database)

    # Write the report
    write_alignment_report(alignments, alignment_report_file)

    print(f"Alignment report generated: {alignment_report_file}")
