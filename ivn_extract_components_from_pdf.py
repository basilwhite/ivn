# ivn_extract_components_from_pdf.py
# Updated: 2026-02-04
# Description: Extracts properly structured requirement components from a PDF source document,
# and exports IVN-compatible Enabling and Dependent inventories with required fields.
# Prompt file: ivn_extract_components_from_pdf_prompt.txt (authoritative specification)

import re
import csv
import requests
import tempfile
import sys
import time
import pandas as pd
import io
import logging
from pathlib import Path
from datetime import datetime
from io import StringIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pdfminer.high_level import extract_text_to_fp, extract_text
from pdfminer.layout import LAParams, LTTextContainer, LTChar, LTPage, LTTextBox
from pdfminer.converter import TextConverter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def is_likely_requirement(text: str) -> bool:
    """Determine if text contains requirement language"""
    if not isinstance(text, str):
        return False
    text = text.strip()
    if len(text.split()) < 7:
        return False
    if text.isupper() and len(text.split()) < 10:
        return False
    if not re.search(
        r'\b(is|are|was|were|be|being|been|shall|must|will|should|establish|develop|create|submit|report|coordinate|implement|modernize|digitize)\b',
        text, re.IGNORECASE
    ):
        return False
    if re.match(r'^(First|Second|Third|Fourth|Fifth)[\s,:]', text, re.IGNORECASE):
        return True
    if re.match(r'^\d{1,3}\s+(U\.?S\.?C\.?|H\.?R\.?)', text, re.IGNORECASE):
        return True
    if re.search(
        r'\b(establish|develop|implement|digitize|coordinate|fund|enhance|require|submit|carry out|allocate|build|deliver|modernize|report|plan|strengthen)\b',
        text, re.IGNORECASE
    ):
        return True
    if re.search(
        r'\b(The (Secretary|Agency|Department|Administrator|Office|Program|Director))\b.+?\b(shall|must|will|is to|is required to)\b',
        text, re.IGNORECASE
    ):
        return True
    return False

def download_pdf_with_browser_headers(url: str) -> Path:
    """Download PDF with retry support and browser-like headers"""
    print("📥 Downloading PDF with retry support...")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/pdf",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com"
    }

    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    try:
        response = session.get(url, headers=headers, stream=True, timeout=10)
        response.raise_for_status()
        total = int(response.headers.get('content-length', 0))
        tmp_path = Path(tempfile.gettempdir()) / f"ivn_temp_{datetime.now().timestamp()}.pdf"

        with open(tmp_path, "wb") as f:
            downloaded = 0
            chunk_size = 8192
            last_percent = -1
            for chunk in response.iter_content(chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        percent = int((downloaded / total) * 100)
                        if percent != last_percent:
                            print(f"  → Downloaded {percent}%")
                            last_percent = percent
        print(f"✅ PDF saved to: {tmp_path}")
        return tmp_path

    except Exception as e:
        print(f"❌ Final download error: {e}")
        return None

def ask_for_pdf_path() -> tuple:
    """Prompt user for PDF input method and return path and original URL"""
    print("Choose PDF input method:")
    print("1. Paste a URL of a PDF")
    print("2. Browse to a local PDF file")
    choice = input("Enter 1 or 2: ").strip()

    component_url = ""
    if choice == "1":
        # Enforce a non-empty URL for remote downloads
        while not component_url:
            component_url = input("Paste the full URL to the PDF: ").strip()
            if not component_url:
                print("⚠️ URL is required. Please enter a valid URL.")
        return download_pdf_with_browser_headers(component_url), component_url
    elif choice == "2":
        try:
            import tkinter as tk
            from tkinter import filedialog
            root = tk.Tk()
            root.withdraw()
            root.call('wm', 'attributes', '.', '-topmost', True)
            file_path = filedialog.askopenfilename(filetypes=[("PDF files", "*.pdf")])
            root.destroy()
            if file_path:
                print(f"📂 Selected local file: {file_path}")
                
                # Always prompt for URL for local files and make it mandatory
                while not component_url:
                    component_url = input("Enter the URL for this document (required): ").strip()
                    if not component_url:
                        print("⚠️ URL is required. Please enter a valid URL.")
                
                return Path(file_path), component_url
            else:
                print("❌ No file selected.")
                return None, ""
        except Exception as e:
            print(f"❌ Failed to open file dialog: {e}")
            return None, ""
    else:
        print("❌ Invalid choice.")
        return None, ""

def fix_hyphenation(text: str) -> str:
    """Fix hyphenated words that were broken across lines"""
    # Replace hyphenated words at end of lines
    fixed = re.sub(r'(\w+)-\n(\w+)', r'\1\2', text)
    
    # Also handle cases where there's no newline but spaces
    fixed = re.sub(r'(\w+)-\s+(\w+)', r'\1\2', fixed)
    
    # Fix other common PDF extraction issues
    fixed = re.sub(r'\s+', ' ', fixed)  # Normalize whitespace
    fixed = re.sub(r'([a-z])- ([a-z])', r'\1\2', fixed)  # Fix "word- ization" -> "wordization"
    
    return fixed.strip()

def clean_text(text: str) -> str:
    """Clean text by removing redundant whitespace and normalizing quotes"""
    # Remove redundant whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Normalize quotes
    text = text.replace('"', '"').replace('"', '"')
    text = text.replace('’', "'").replace('‘', "'")
    
    # Fix common OCR errors
    text = text.replace('|', 'I')
    text = re.sub(r'(\d)l(\d)', r'\1l\2', text)  # Fix "l" used as "1"
    
    return text.strip()

def extract_text_with_layout(pdf_path: Path, laparams=None) -> list:
    """Extract text with layout information to preserve document structure"""
    print("📄 Extracting text with layout information...")
    
    if laparams is None:
        laparams = LAParams(
            line_margin=0.5,
            char_margin=2.0,
            word_margin=0.1,
            boxes_flow=0.5,
            detect_vertical=True
        )
    
    layout_elements = []
    
    with open(pdf_path, 'rb') as fp:
        parser = PDFParser(fp)
        document = PDFDocument(parser)
        rsrcmgr = PDFResourceManager()
        
        for page_num, page in enumerate(PDFPage.create_pages(document)):
            print(f"  → Processing page {page_num + 1}...")
            
            # Create a device to capture layout information
            device = LayoutCollector(rsrcmgr, laparams=laparams)
            interpreter = PDFPageInterpreter(rsrcmgr, device)
            interpreter.process_page(page)
            
            # Get the layout for this page
            layout = device.get_result()
            
            # Store the layout elements for this page
            layout_elements.append({
                'page_num': page_num + 1,
                'elements': process_layout(layout)
            })
    
    return layout_elements

class LayoutCollector(TextConverter):
    """Custom collector to capture PDF layout elements"""
    def __init__(self, rsrcmgr, laparams=None):
        self.result = []
        super().__init__(rsrcmgr, StringIO(), laparams=laparams)

    def receive_layout(self, ltpage):
        self.result.append(ltpage)

    def get_result(self):
        return self.result

def process_layout(layout):
    """Process layout to extract text with font information"""
    elements = []
    
    for page in layout:
        for element in page:
            if isinstance(element, LTTextBox):
                text = element.get_text().strip()
                if text:
                    # Extract font information from the first character
                    font_info = extract_font_info(element)
                    
                    elements.append({
                        'text': text,
                        'bbox': element.bbox,
                        'font': font_info.get('font', 'Unknown'),
                        'size': font_info.get('size', 0),
                        'is_bold': font_info.get('is_bold', False),
                        'x0': element.bbox[0],
                        'y0': element.bbox[1],
                        'x1': element.bbox[2],
                        'y1': element.bbox[3],
                    })
    
    return elements

def extract_font_info(text_element):
    """Extract font information from text element"""
    font_info = {'font': 'Unknown', 'size': 0, 'is_bold': False}
    
    for obj in text_element:
        if isinstance(obj, LTTextContainer):
            for char in obj:
                if isinstance(char, LTChar):
                    font_info['font'] = char.fontname
                    font_info['size'] = round(char.size)
                    font_info['is_bold'] = 'Bold' in char.fontname or 'bold' in char.fontname
                    return font_info
    
    return font_info

def identify_source_document(pdf_path: Path, text: str) -> str:
    """Extract formal document name from PDF metadata or content"""
    print("🔍 Identifying source document name...")
    
    # Try to extract from PDF metadata first
    suggested_title = ""
    
    # Try to use pikepdf if available or can be installed
    if try_install_package("pikepdf"):
        try:
            import pikepdf
            with pikepdf.open(pdf_path) as pdf:
                if pdf.docinfo.get('/Title'):
                    title = pdf.docinfo['/Title']
                    if isinstance(title, bytes):
                        title = title.decode('utf-8', errors='ignore')
                    # Ensure we always store a plain string here
                    suggested_title = str(title)
                    print(f"  → Found title in PDF metadata: {suggested_title}")
        except Exception as e:
            print(f"  → Could not extract metadata with pikepdf: {e}")
    else:
        print("  → Proceeding without pikepdf for metadata extraction")
    
    # Normalize to a string before any length checks to avoid TypeError from non-str objects
    if suggested_title is None:
        suggested_title = ""
    else:
        suggested_title = str(suggested_title)
    
    # Try to extract from first few pages if no metadata title
    if not suggested_title or len(suggested_title) < 10:
        # Enhanced patterns for government document titles
        title_patterns = [
            # Common act patterns with number prefixes
            r"(?i)(\d+(?:st|nd|rd|th)\s+Century\s+[A-Za-z\s]+Act(?:\s+of\s+\d{4})?)",
            r"(?i)((?:The\s+)?[A-Z][A-Za-z\s]+Act\s+of\s+\d{4})",
            r"(?i)((?:The\s+)?[A-Z][A-Za-z\s]+\s+Act)",
            # Public laws and U.S. Code references
            r"(?i)(Public\s+Law\s+\d+[-–]\d+)",
            r"(?i)(Title\s+\d+[A-Z]*\s+of\s+the\s+.+?\s+Code)",
            r"(?i)(\d+\s+U\.?S\.?C\.?\s+.*)",
            # Federal Register and regulations
            r"(?i)(Code\s+of\s+Federal\s+Regulations\s+.*)",
            r"(?i)(Executive\s+Order\s+\d+)",
            r"(?i)(Federal\s+Register\s+.*)",
            # General document titles in all caps
            r"([A-Z][A-Z\s]{10,}(?:\s+[A-Z]+){1,})"
        ]
        
        first_pages = text[:8000]  # Examine more text to find the title
        for pattern in title_patterns:
            matches = re.findall(pattern, first_pages)
            if matches:
                # Use the longest match as it's likely the most complete title
                if isinstance(matches[0], tuple):
                    # If the match is a tuple (from capturing groups), use the first group
                    matches = [m[0] for m in matches]
                
                matches.sort(key=len, reverse=True)
                result = matches[0]
                print(f"  → Found title in document: {result}")
                suggested_title = result
                break
    
    # Additional cleanup for detected titles
    if suggested_title:
        # Fix common OCR issues in titles
        suggested_title = re.sub(r'(?<!\d)l(?=\d)', '1', suggested_title)  # Replace lone 'l' with '1' before digits
        suggested_title = re.sub(r'\s+', ' ', suggested_title).strip()  # Normalize whitespace
    
    # If still no title, use filename
    if not suggested_title:
        suggested_title = pdf_path.stem.replace('_', ' ').title()
    
    # Always prompt for confirmation of title
    print(f"\n📝 Suggested document title: {suggested_title}")
    user_title = input("Press Enter to accept this title, or type a new title: ").strip()
    
    # Use user input if provided, otherwise use suggested title
    final_title = user_title if user_title else suggested_title
    print(f"✅ Using document title: {final_title}")
    
    return final_title

def identify_sections(layout_elements: list) -> dict:
    """Identify document sections based on layout information with enhanced pattern recognition"""
    print("🔍 Identifying document sections...")
    
    sections = {}
    current_section = None
    section_counter = 0
    
    # Additional section header patterns for government documents
    section_patterns = [
        r'^(?:\d+\.)+\s+\w+',                  # Numbered sections like "1.2.3 Title"
        r'^Section\s+\d+[\.\-]?\s+\w+',        # "Section 123" format
        r'^\([a-z]\)\s+[A-Z]',                 # "(a) Text..." format common in legislation
        r'^\d+\.\s+[A-Z]',                     # "1. TEXT" format
        r'^[A-Z][A-Z\s]+$',                    # ALL CAPS TEXT as headers
        r'^[IVXLCDM]+\.\s+[A-Z]',              # Roman numerals: "IV. SECTION"
        r'^\d+\s+U\.S\.C\.\s+\d+',             # U.S. Code references
        r'^(Subpart|Part|Chapter|Subtitle)\s+[A-Z0-9]', # Common legal document divisions
    ]
    
    # First pass: identify potential sections based on visual characteristics
    all_elements = []
    for page in layout_elements:
        all_elements.extend([(element, page['page_num']) for element in page['elements']])
    
    # Sort elements by font size (larger first) to identify hierarchy clues
    font_size_groups = {}
    for element, page_num in all_elements:
        font_size = element['size']
        if font_size not in font_size_groups:
            font_size_groups[font_size] = []
        font_size_groups[font_size].append((element, page_num))
    
    # Sort font sizes from largest to smallest
    sorted_font_sizes = sorted(font_size_groups.keys(), reverse=True)
    
    print(f"  → Detected {len(sorted_font_sizes)} different font sizes")
    
    # Second pass: identify sections from largest font to smallest
    for level, font_size in enumerate(sorted_font_sizes[:4]):  # Consider top 4 font sizes as potential headers
        for element, page_num in font_size_groups[font_size]:
            text = element['text'].strip()
            is_bold = element['is_bold']
            
            # Check if this looks like a section header
            is_header = False
            
            # Check against our patterns
            for pattern in section_patterns:
                if re.search(pattern, text, re.MULTILINE):
                    is_header = True
                    break
            
            # Also consider layout characteristics
            if font_size > 10 and (is_bold or text.isupper() or len(text.split()) <= 10):
                is_header = True
            
            if is_header and len(text) > 3:  # Avoid very short headers
                section_counter += 1
                current_section = {
                    'id': f"section_{section_counter}",
                    'header': text,
                    'level': level + 1,  # Level based on font size rank
                    'text': text + "\n",
                    'page': page_num,
                    'font_size': font_size,
                    'is_bold': is_bold,
                    'x0': element['x0'],  # Save position for hierarchy analysis
                    'y0': element['y0']
                }
                sections[current_section['id']] = current_section
    
    # Third pass: assign text content to each section
    for page in layout_elements:
        page_num = page['page_num']
        # Sort elements top to bottom
        elements = sorted(page['elements'], key=lambda e: -e['y0'])
        
        # Find sections on this page
        page_sections = [(section_id, section) for section_id, section in sections.items() 
                          if section['page'] == page_num]
        
        # Sort sections top to bottom
        page_sections.sort(key=lambda x: -x[1]['y0'])
        
        if not page_sections:
            continue
        
        # For each text element, assign to the nearest preceding section
        current_section_idx = 0
        for element in elements:
            if element['text'].strip() == "":
                continue
                
            # Skip if this element is a section header (already processed)
            is_section_header = False
            for section_id, section in page_sections:
                if section['header'] == element['text'].strip():
                    is_section_header = True
                    break
            if is_section_header:
                continue
            
            # Find the appropriate section for this text
            # (section above this element's position)
            while (current_section_idx < len(page_sections) - 1 and 
                   element['y0'] < page_sections[current_section_idx][1]['y0']):
                current_section_idx += 1
            
            if current_section_idx < len(page_sections):
                section_id = page_sections[current_section_idx][0]
                # Add this text to the section content
                if element['text'].strip() not in sections[section_id]['text']:
                    sections[section_id]['text'] += element['text'].strip() + "\n"
    
    # Fourth pass: establish parent-child relationships and structure hierarchy
    establish_section_hierarchy(sections)
    
    # Clean up section text
    for section_id, section in sections.items():
        cleaned_text = fix_hyphenation(section['text'])
        cleaned_text = clean_text(cleaned_text)
        sections[section_id]['text'] = cleaned_text
    
    print(f"✅ Identified {len(sections)} document sections")
    
    # If we found fewer than 3 sections, it's likely we missed the structure
    if len(sections) < 3:
        print("⚠️ Few sections detected, attempting alternative detection method...")
        alternative_sections = identify_sections_by_patterns(layout_elements)
        if len(alternative_sections) > len(sections):
            sections = alternative_sections
            print(f"✅ Alternative method found {len(sections)} sections")
    
    return sections

def establish_section_hierarchy(sections):
    """Establish parent-child relationships between sections"""
    # Sort sections by level (smallest/highest level first)
    sorted_sections = sorted(sections.items(), key=lambda x: x[1]['level'])
    
    # For each section, find its parent
    for section_id, section in sorted_sections:
        if section['level'] == 1:  # Top level sections have no parent
            continue
            
        # Find potential parents (sections with lower level numbers)
        potential_parents = [(pid, p) for pid, p in sorted_sections 
                              if p['level'] < section['level'] and 
                              p['page'] <= section['page']]
        
        # Skip if no potential parents
        if not potential_parents:
            continue
            
        # Find the closest preceding parent
        best_parent = None
        for pid, parent in reversed(potential_parents):
            # If on same page, check if parent is above this section
            if parent['page'] == section['page']:
                if parent['y0'] > section['y0']:
                    best_parent = pid
                    break
            else:  # Parent on earlier page
                best_parent = pid
                break
        
        # If parent found, establish relationship
        if best_parent:
            if 'children' not in sections[best_parent]:
                sections[best_parent]['children'] = []
            sections[best_parent]['children'].append(section_id)
            section['parent'] = best_parent

def identify_sections_by_patterns(layout_elements):
    """Alternative section identification based purely on text patterns"""
    sections = {}
    section_counter = 0
    all_text = ""
    
    # First combine all text in reading order
    for page in layout_elements:
        elements = sorted(page['elements'], key=lambda e: (-e['y0'], e['x0']))
        for element in elements:
            all_text += element['text'] + "\n"
    
    # Look for common section patterns
    section_patterns = [
        # Section with number then title
        (r'Section\s+(\d+)[\.\s]+([A-Z][^\n]+)', 1),
        # Numbered sections 
        (r'(\d+\.\d+)\s+([A-Z][^\n]+)', 1),
        # Lettered sections
        (r'^\s*\(([a-z])\)\s+([A-Z][^\n]+)', 2),
        # ALL CAPS headers
        (r'([A-Z][A-Z\s]{5,}[A-Z])\s*$', 1),
        # Roman numeral sections
        (r'([IVXLCDM]+)[\.\s]+([A-Z][^\n]+)', 1)
    ]
    
    # Find all matches
    for pattern, level in section_patterns:
        matches = re.finditer(pattern, all_text, re.MULTILINE)
        for match in matches:
            section_counter += 1
            header = match.group(0).strip()
            
            # Get the text after this header until the next potential header
            start_pos = match.end()
            end_pos = start_pos
            while end_pos < len(all_text):
                # Look for pattern that might be the next header
                next_header = False
                for p, _ in section_patterns:
                    if re.match(p, all_text[end_pos:end_pos+50], re.MULTILINE):
                        next_header = True
                        break
                if next_header:
                    break
                end_pos += 1
            
            # Extract the section text
            section_text = header + "\n" + all_text[start_pos:end_pos].strip()
            
            sections[f"section_{section_counter}"] = {
                'id': f"section_{section_counter}",
                'header': header,
                'level': level,
                'text': section_text,
                'page': 0  # We don't know the page in this method
            }
    
    return sections

def extract_policy_requirements(text: str) -> list:
    """Extract policy-specific requirements from text with enhanced detection"""
    requirements = []
    
    # Split text into sentences
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z0-9])', text)
    
    # Policy-specific keywords for stronger filtering
    policy_verbs = r'\b(establish|develop|implement|create|submit|report|coordinate|modernize|digitize|require|fund|enhance|allocate|deliver|plan|strengthen|provide|ensure)\b'
    obligation_terms = r'\b(shall|must|will|should|is required to|are required to|is to|are to)\b'
    timeframe_terms = r'\b(\d+\s+days|\d+\s+months|\d+\s+years|annually|quarterly|within\s+\d+)\b'
    
    # Process each sentence
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
            
        score = 0
        
        # Check for policy verbs
        if re.search(policy_verbs, sentence, re.IGNORECASE):
            score += 3
            
        # Check for obligation terms
        if re.search(obligation_terms, sentence, re.IGNORECASE):
            score += 3
            
        # Check for timeframes
        if re.search(timeframe_terms, sentence, re.IGNORECASE):
            score += 2
            
        # Check for agency mentions
        if re.search(r'\b(agency|department|secretary|administrator|director|office)\b', 
                    sentence, re.IGNORECASE):
            score += 2
            
        # Higher threshold for policy-relevant requirements
        if score >= 5:
            # Get some context (try to include previous sentence if available)
            context = get_sentence_context(sentences, sentence)
            
            requirements.append({
                'text': sentence,
                'context': context,
                'score': score
            })
    
    # Sort by relevance score
    requirements.sort(key=lambda x: x['score'], reverse=True)
    
    return requirements

def extract_policy_action(text: str) -> str:
    """Extract the primary policy action from text"""
    # Look for common policy verbs and the noun that follows
    policy_verbs = [
        'establish', 'develop', 'implement', 'create', 'submit', 'report', 
        'coordinate', 'modernize', 'digitize', 'require', 'fund', 'enhance', 
        'allocate', 'deliver', 'plan', 'strengthen', 'provide', 'ensure'
    ]
    
    for verb in policy_verbs:
        pattern = fr'\b({verb})\b\s+([a-z]{{1,20}}\s+){{0,3}}([a-z]{{3,20}})'
        match = re.search(pattern, text.lower())
        if match:
            return f"{match.group(1).title()} {match.group(3).title()}"
    
    # Fallback to extracting a policy-relevant phrase
    words = text.split()
    for i, word in enumerate(words):
        if word.lower() in policy_verbs and i < len(words) - 1:
            return f"{word.title()} {' '.join(words[i+1:i+3])}"
    
    # Last resort: just take the first few words
    return " ".join(words[:3]).title()

def extract_best_sentence(text: str) -> str:
    """Extract the most informative sentence from text"""
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z0-9])', text)
    
    if not sentences:
        return text[:100] + "..."
    
    # Score sentences based on policy relevance
    scored_sentences = []
    for sentence in sentences:
        score = 0
        # Longer sentences tend to be more informative (up to a point)
        words = len(sentence.split())
        if 10 <= words <= 40:
            score += 2
        
        # Sentences with policy keywords
        if re.search(r'\b(shall|must|will|should|require|implement|establish|develop)\b', 
                    sentence, re.IGNORECASE):
            score += 3
        
        # Sentences that mention timeframes
        if re.search(r'\b(day|week|month|year|annually|quarterly)\b', sentence, re.IGNORECASE):
            score += 1
            
        # Sentences with measurements or quantities
        if re.search(r'\b(\d+%|\d+\s+percent|\$\d+|\d+\s+dollars)\b', sentence, re.IGNORECASE):
            score += 1
            
        scored_sentences.append((score, sentence))
    
    # Return the highest scoring sentence
    scored_sentences.sort(reverse=True)
    return scored_sentences[0][1]

def get_sentence_context(sentences: list, target_sentence: str) -> str:
    """Get context for a sentence (including previous sentence if possible)"""
    try:
        index = sentences.index(target_sentence)
        if index > 0:
            prev_sentence = sentences[index - 1]
            if len(prev_sentence.split()) < 50:  # Only include reasonably short sentences
                return prev_sentence + " " + target_sentence
    except ValueError:
        pass
    
    return target_sentence

def try_install_package(package_name):
    """Try to install a Python package if not already installed"""
    try:
        # First check if the module can be imported
        __import__(package_name)
        return True
    except ImportError:
        print(f"📦 Package '{package_name}' not found. Attempting to install...")
        try:
            import subprocess
            # Use subprocess to run pip install
            subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
            print(f"✅ Successfully installed {package_name}")
            return True
        except Exception as e:
            print(f"❌ Failed to install {package_name}: {e}")
            return False

def clean_component_name(header: str) -> str:
    """Clean component name to be concise and meaningful - preserving section numbers"""
    # No longer removing section numbers - preserve them as requested
    
    # Just limit length if necessary
    if len(header) > 50:
        header = header[:47] + "..."
    
    return header

def identify_components(sections: dict) -> list:
    """Identify proper components based on document structure with policy focus"""
    print("🔍 Identifying components from document structure...")
    
    components = []
    component_counter = 0
    
    # First pass: gather sections by level for hierarchy-aware processing
    sections_by_level = {}
    for section_id, section in sections.items():
        level = section['level']
        if level not in sections_by_level:
            sections_by_level[level] = []
        sections_by_level[level].append((section_id, section))
    
    # Sort levels from highest to lowest (1 is highest)
    sorted_levels = sorted(sections_by_level.keys())
    
    # Process sections level by level, focusing on policy-relevant content
    for level in sorted_levels:
        # For higher levels (1-2), use the sections as components directly
        if level <= 2:
            for section_id, section in sections_by_level[level]:
                # Use section headers as component names - preserving section numbers
                component_name = clean_component_name(section['header'])
                
                # Extract policy-specific requirements from section text
                policy_requirements = extract_policy_requirements(section['text'])
                
                if policy_requirements:
                    for req in policy_requirements:
                        component_counter += 1
                        components.append({
                            'section_id': section_id,
                            'section_name': section['header'],
                            'component': component_name,
                            'description': req['text'],
                            'context': req['context'],
                            'level': section['level'],
                            'id': f"comp_{component_counter}"
                        })
                else:
                    # If no policy requirements found but section is important, use standard requirements
                    standard_requirements = extract_requirements_from_text(section['text'])
                    if standard_requirements:
                        for req in standard_requirements:
                            component_counter += 1
                            components.append({
                                'section_id': section_id,
                                'section_name': section['header'],
                                'component': component_name,
                                'description': req['text'],
                                'context': req['context'],
                                'level': section['level'],
                                'id': f"comp_{component_counter}"
                            })
                    else:
                        # Last resort: use best sentence as component
                        component_counter += 1
                        components.append({
                            'section_id': section_id,
                            'section_name': section['header'],
                            'component': component_name,
                            'description': extract_best_sentence(section['text']),
                            'context': section['text'][:500],
                            'level': section['level'],
                            'id': f"comp_{component_counter}"
                        })
        
        # For lower levels (3+), extract policy-relevant sentences as components
        else:
            for section_id, section in sections_by_level[level]:
                # Extract only the most policy-relevant requirements
                policy_requirements = extract_policy_requirements(section['text'])
                
                for req in policy_requirements:
                    component_counter += 1
                    
                    # Create component name from section and policy action
                    policy_action = extract_policy_action(req['text'])
                    component_name = f"{section['header']} - {policy_action}"
                    if len(component_name) > 50:
                        component_name = component_name[:47] + "..."
                    
                    components.append({
                        'section_id': section_id,
                        'section_name': section['header'],
                        'component': component_name,
                        'description': req['text'],
                        'context': req['context'],
                        'level': section['level'],
                        'id': f"comp_{component_counter}"
                    })
    
    # If no components were found from structure, fall back to sentence-based extraction
    if not components:
        print("  → No structured components found, falling back to sentence-based extraction")
        all_text = "\n".join([section['text'] for section in sections.values()])
        fallback_components = extract_fallback_components(all_text)
        components.extend(fallback_components)
    
    print(f"✅ Identified {len(components)} components")
    return components

def extract_requirements_from_text(text: str) -> list:
    """Extract requirements from section text"""
    requirements = []
    
    # Split text into sentences
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z0-9])', text)
    
    # Process each sentence
    for sentence in sentences:
        sentence = sentence.strip()
        if is_likely_requirement(sentence):
            # Get some context (try to include previous sentence if available)
            context = get_sentence_context(sentences, sentence)
            
            requirements.append({
                'text': sentence,
                'context': context
            })
    
    return requirements

def extract_fallback_components(text: str) -> list:
    """Extract components when document structure cannot be determined"""
    components = []
    component_counter = 0
    
    # Try to extract policy-relevant content first
    policy_requirements = extract_policy_requirements(text)
    
    if policy_requirements:
        for req in policy_requirements:
            # Create a component name from policy action
            policy_action = extract_policy_action(req['text'])
            
            component_counter += 1
            components.append({
                'section_id': "fallback",
                'section_name': "Policy Requirement",
                'component': policy_action,
                'description': req['text'],
                'context': req['context'],
                'level': 3,
                'id': f"comp_{component_counter}"
            })
    else:
        # Fall back to standard requirement extraction
        sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z0-9])', text)
        
        # Process each sentence
        for sentence in sentences:
            sentence = sentence.strip()
            if is_likely_requirement(sentence):
                # Create a component name from the beginning of the sentence
                words = sentence.split()
                component_name = " ".join(words[:min(8, len(words))])
                if len(component_name) > 50:
                    component_name = component_name[:47] + "..."
                
                component_counter += 1
                components.append({
                    'section_id': "fallback",
                    'section_name': "Unstructured Content",
                    'component': component_name,
                    'description': sentence,
                    'context': sentence,
                    'level': 3,
                    'id': f"comp_{component_counter}"
                })
    
    return components

def format_inventory_rows(components, source_name, component_url, mode_label):
    """Format components into inventory rows for export"""
    print(f"🛠️ Formatting {mode_label} component rows...")
    rows = []
    
    for component in components:
        # Clean and prepare the component description
        description = clean_text(component['description'])
        
        # Create row with only the required four columns
        row = {
            "Source": source_name,
            "Component": component['component'],
            "Component Description": description,
            "Component URL": component_url
        }
        rows.append(row)
    
    print(f"✅ {len(rows)} {mode_label} components ready.")
    return rows

def validate_data(components: list) -> list:
    """Validate components to ensure quality and completeness"""
    print("🧪 Validating component data quality...")
    valid_components = []
    
    for component in components:
        # Skip empty or extremely short descriptions
        if not component['description'] or len(component['description']) < 20:
            continue
            
        # Clean up the description
        component['description'] = clean_text(component['description'])
        
        # Make sure component name is meaningful
        if len(component['component']) < 5:
            # Create better component name from description using policy action
            policy_action = extract_policy_action(component['description'])
            component['component'] = policy_action
            if len(component['component']) > 50:
                component['component'] = component['component'][:47] + "..."
        
        valid_components.append(component)
    
    print(f"✅ {len(valid_components)} valid components after validation")
    return valid_components

def save_validated_excel(enabling_rows, dependent_rows, source_name):
    """Save validated data to Excel with proper formatting"""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    script_dir = Path(__file__).parent.resolve()
    
    # Better sanitize source name for filename - handle newlines and more invalid chars
    safe_source_name = source_name.replace('\n', ' ').replace('\r', ' ')
    safe_source_name = re.sub(r'\s+', ' ', safe_source_name).strip()  # Normalize whitespace
    safe_source_name = re.sub(r'[\\/*?:"<>|]', '_', safe_source_name)  # Replace invalid chars
    
    # Limit filename length to avoid potential issues
    if len(safe_source_name) > 100:
        safe_source_name = safe_source_name[:97] + "..."
    
    output_file = script_dir / f"ivn_requirements_{safe_source_name}_{timestamp}.xlsx"
    
    try:
        save_start = time.time()
        print("💾 Saving Excel file with both component tables...")
        
        # Convert to pandas DataFrames
        df_enabling = pd.DataFrame(enabling_rows)
        df_dependent = pd.DataFrame(dependent_rows)
        
        # Save both DataFrames to a single Excel file with separate sheets
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            df_enabling.to_excel(writer, sheet_name='Enabling Components', index=False)
            df_dependent.to_excel(writer, sheet_name='Dependent Components', index=False)
            
            # Get workbook and set up formats
            workbook = writer.book
            wrap_format = workbook.add_format({'text_wrap': True})
            header_format = workbook.add_format({
                'bold': True, 
                'text_wrap': True, 
                'valign': 'top', 
                'bg_color': '#D9D9D9'
            })
            
            # Format enabling sheet
            worksheet = writer.sheets['Enabling Components']
            for i, col in enumerate(df_enabling.columns):
                column_width = max(15, min(50, len(col) + 2))
                worksheet.set_column(i, i, column_width, wrap_format)
            
            # Format description column wider
            desc_col = df_enabling.columns.get_loc("Component Description")
            worksheet.set_column(desc_col, desc_col, 80, wrap_format)
            
            # Format dependent sheet
            worksheet = writer.sheets['Dependent Components']
            for i, col in enumerate(df_dependent.columns):
                column_width = max(15, min(50, len(col) + 2))
                worksheet.set_column(i, i, column_width, wrap_format)
                
            # Format description column wider
            desc_col = df_dependent.columns.get_loc("Component Description")
            worksheet.set_column(desc_col, desc_col, 80, wrap_format)
        
        print(f"📤 Excel file saved with both component sheets: {output_file}")
        print(f"   - Enabling Components: {len(enabling_rows)} items")
        print(f"   - Dependent Components: {len(dependent_rows)} items")
        print(f"   - Save time: {time.time() - save_start:.1f}s")
        
        return output_file
    
    except Exception as e:
        print(f"❌ Error saving Excel file: {e}")
        # Create a fallback filename with just the timestamp if there's an issue
        fallback_file = script_dir / f"ivn_requirements_export_{timestamp}.xlsx"
        print(f"⚠️ Trying again with simplified filename: {fallback_file}")
        
        try:
            with pd.ExcelWriter(fallback_file, engine='xlsxwriter') as writer:
                df_enabling.to_excel(writer, sheet_name='Enabling Components', index=False)
                df_dependent.to_excel(writer, sheet_name='Dependent Components', index=False)
            print(f"✅ Successfully saved with fallback filename")
            return fallback_file
        except Exception as e2:
            print(f"❌ Final error saving Excel file: {e2}")
            return None

def main():
    """Main function to orchestrate the extraction and processing"""
    overall_start = time.time()
    pdf_result = ask_for_pdf_path()
    pdf_path, component_url = pdf_result if isinstance(pdf_result, tuple) else (pdf_result, "")
    
    if not pdf_path or not pdf_path.exists():
        print("❌ PDF path not valid. Exiting.")
        return
    
    # Extract raw text for source document identification
    raw_text = extract_text(pdf_path)
    
    # Identify proper source document name with mandatory confirmation
    source_name = identify_source_document(pdf_path, raw_text)
    
    # Extract text with layout information
    layout_elements = extract_text_with_layout(pdf_path)
    
    # Identify document sections
    sections = identify_sections(layout_elements)
    
    # Extract components from sections with policy focus
    components = identify_components(sections)
    
    # Validate components
    valid_components = validate_data(components)
    
    # Format for export with URL column
    enabling_rows = format_inventory_rows(valid_components, source_name, component_url, "Enabling")
    dependent_rows = format_inventory_rows(valid_components, source_name, component_url, "Dependent")
    
    # Save to Excel with validation
    output_file = save_validated_excel(enabling_rows, dependent_rows, source_name)
    
    print(f"🏁 All operations complete | Total elapsed: {time.time() - overall_start:.1f}s")
    print(f"📊 Results saved to: {output_file}")

if __name__ == "__main__":
    main()
