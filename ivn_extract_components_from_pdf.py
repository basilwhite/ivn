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

# pdfminer.six
from pdfminer.high_level import extract_text
from pdfminer.layout import LAParams, LTTextContainer, LTChar, LTTextBox
from pdfminer.converter import TextConverter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument

# Jupyter helpers
from IPython.display import display, FileLink
try:
    import ipywidgets as widgets
except Exception:
    widgets = None  # Graceful fallback if ipywidgets isn't available

# TQDM for optional progress indications
try:
    from tqdm import tqdm
except Exception:
    tqdm = None

# --- Notebook-friendly logging ---
logger = logging.getLogger("policy_extractor")
logger.setLevel(logging.INFO)
if not logger.handlers:
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(sh)
def is_likely_requirement(text: str) -> bool:
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


def fix_hyphenation(text: str) -> str:
    fixed = re.sub(r'(\w+)-\n(\w+)', r'\1\2', text)
    fixed = re.sub(r'(\w+)-\s+(\w+)', r'\1\2', fixed)
    fixed = re.sub(r'\s+', ' ', fixed)
    fixed = re.sub(r'([a-z])- ([a-z])', r'\1\2', fixed)
    return fixed.strip()


def clean_text(text: str) -> str:
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)

    # Normalize curly quotes to straight quotes
    text = text.replace('“', '"').replace('”', '"')
    text = text.replace('‘', "'").replace('’', "'")

    # Fix common OCR issues
    text = text.replace('|', 'I')
    text = re.sub(r'(\d)l(\d)', r'\1l\2', text)

    return text.strip()
def download_pdf_with_browser_headers(url: str) -> Path | None:
    """Download PDF with retry support and browser-like headers, returns a temp Path or None."""
    logger.info("📥 Downloading PDF with retry support...")

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
        response = session.get(url, headers=headers, stream=True, timeout=15)
        response.raise_for_status()
        total = int(response.headers.get('content-length', 0))
        tmp_path = Path(tempfile.gettempdir()) / f"ivn_temp_{datetime.now().timestamp()}.pdf"

        downloaded = 0
        chunk_size = 8192
        last_percent = -1
        with open(tmp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        percent = int((downloaded / total) * 100)
                        if percent != last_percent:
                            print(f"  → Downloaded {percent}%")
                            last_percent = percent
        logger.info(f"✅ PDF saved to: {tmp_path}")
        return tmp_path

    except Exception as e:
        logger.error(f"❌ Final download error: {e}")
        return None


class LayoutCollector(TextConverter):
    """Custom collector to capture PDF layout elements."""
    def __init__(self, rsrcmgr, laparams=None):
        self.result = []
        super().__init__(rsrcmgr, StringIO(), laparams=laparams)

    def receive_layout(self, ltpage):
        self.result.append(ltpage)

    def get_result(self):
        return self.result


def extract_text_with_layout(pdf_path: Path, laparams=None) -> list[dict]:
    """Extract text with layout information to preserve document structure."""
    logger.info("📄 Extracting text with layout information...")

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

        pages = list(PDFPage.create_pages(document))
        iterator = tqdm(pages, desc="Pages") if tqdm else pages
        for page_num, page in enumerate(iterator, start=1):
            # Create a device to capture layout information
            device = LayoutCollector(rsrcmgr, laparams=laparams)
            interpreter = PDFPageInterpreter(rsrcmgr, device)
            interpreter.process_page(page)
            layout = device.get_result()

            layout_elements.append({
                'page_num': page_num,
                'elements': process_layout(layout)
            })

    return layout_elements


def process_layout(layout):
    elements = []
    for page in layout:
        for element in page:
            if isinstance(element, LTTextBox):
                text = element.get_text().strip()
                if text:
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
def identify_sections(layout_elements: list) -> dict:
    logger.info("🔍 Identifying document sections...")
    sections = {}
    section_counter = 0

    section_patterns = [
        r'^(?:\d+\.)+\s+\w+',
        r'^Section\s+\d+[\.\-]?\s+\w+',
        r'^\([a-z]\)\s+[A-Z]',
        r'^\d+\.\s+[A-Z]',
        r'^[A-Z][A-Z\s]+$',
        r'^[IVXLCDM]+\.\s+[A-Z]',
        r'^\d+\s+U\.S\.C\.\s+\d+',
        r'^(Subpart|Part|Chapter|Subtitle)\s+[A-Z0-9]',
    ]

    # Flatten and group by font size
    all_elements = []
    for page in layout_elements:
        all_elements.extend([(e, page['page_num']) for e in page['elements']])

    font_size_groups = {}
    for element, page_num in all_elements:
        font_size_groups.setdefault(element['size'], []).append((element, page_num))

    sorted_font_sizes = sorted(font_size_groups.keys(), reverse=True)
    logger.info(f"  → Detected {len(sorted_font_sizes)} different font sizes")

    # Identify headers by font size and patterns (top 4 sizes)
    for level, font_size in enumerate(sorted_font_sizes[:4]):
        for element, page_num in font_size_groups[font_size]:
            text = element['text'].strip()
            is_bold = element['is_bold']
            is_header = False

            for pattern in section_patterns:
                if re.search(pattern, text, re.MULTILINE):
                    is_header = True
                    break

            if font_size > 10 and (is_bold or text.isupper() or len(text.split()) <= 10):
                is_header = True

            if is_header and len(text) > 3:
                section_counter += 1
                sections[f"section_{section_counter}"] = {
                    'id': f"section_{section_counter}",
                    'header': text,
                    'level': level + 1,
                    'text': text + "\n",
                    'page': page_num,
                    'font_size': font_size,
                    'is_bold': is_bold,
                    'x0': element['x0'],
                    'y0': element['y0'],
                }

    # Assign content to sections by page, top-to-bottom
    for page in layout_elements:
        page_num = page['page_num']
        elements = sorted(page['elements'], key=lambda e: -e['y0'])
        page_sections = [(sid, s) for sid, s in sections.items() if s['page'] == page_num]
        page_sections.sort(key=lambda x: -x[1]['y0'])

        if not page_sections:
            continue

        current_section_idx = 0
        for element in elements:
            if not element['text'].strip():
                continue
            if any(s['header'] == element['text'].strip() for _, s in page_sections):
                continue

            while (current_section_idx < len(page_sections) - 1 and 
                   element['y0'] < page_sections[current_section_idx][1]['y0']):
                current_section_idx += 1

            if current_section_idx < len(page_sections):
                section_id = page_sections[current_section_idx][0]
                if element['text'].strip() not in sections[section_id]['text']:
                    sections[section_id]['text'] += element['text'].strip() + "\n"

    establish_section_hierarchy(sections)

    # Clean text
    for sid, section in sections.items():
        cleaned = clean_text(fix_hyphenation(section['text']))
        sections[sid]['text'] = cleaned

    logger.info(f"✅ Identified {len(sections)} document sections")

    if len(sections) < 3:
        logger.warning("⚠️ Few sections detected, attempting alternative detection method...")
        alt = identify_sections_by_patterns(layout_elements)
        if len(alt) > len(sections):
            sections = alt
            logger.info(f"✅ Alternative method found {len(sections)} sections")

    return sections


def establish_section_hierarchy(sections):
    """
    Establish parent-child relationships between sections using level, page, and vertical position.
    Robust against missing y0 (e.g., when created by text-only fallback).
    """
    if not sections:
        return

    # Prepare items as (sid, sdict) and ensure required keys exist
    items = [(sid, s) for sid, s in sections.items()
             if isinstance(s, dict) and 'level' in s and 'page' in s]

    # Sort by level (ascending: 1 is top level)
    items.sort(key=lambda x: x[1].get('level', 1))

    for sid, s in items:
        s_level = s.get('level', 1)
        s_page = s.get('page', 0)
        s_y0 = s.get('y0', None)

        # Top-level sections have no parent
        if s_level == 1:
            continue

        # Potential parents: lower level and on or before this section's page
        potential = [(pid, p) for pid, p in items
                     if p.get('level', 1) < s_level and p.get('page', 0) <= s_page]

        if not potential:
            continue

        best_parent = None
        # Choose the closest preceding parent, preferring same-page parents above the section (higher y0)
        for pid, parent in reversed(potential):
            p_page = parent.get('page', 0)
            p_y0 = parent.get('y0', None)

            if p_page == s_page and p_y0 is not None and s_y0 is not None:
                # On same page: parent must be above (higher y0)
                if p_y0 > s_y0:
                    best_parent = pid
                    break
            else:
                # Different page or missing y-coordinates: accept the most recent lower-level section
                best_parent = pid
                break

        if best_parent:
            sections[best_parent].setdefault('children', []).append(sid)
            sections[sid]['parent'] = best_parent
            
def identify_sections_by_patterns(layout_elements):
    sections = {}
    section_counter = 0
    all_text = ""
    for page in layout_elements:
        elements = sorted(page['elements'], key=lambda e: (-e['y0'], e['x0']))
        for element in elements:
            all_text += element['text'] + "\n"

    patterns = [
        (r'Section\s+(\d+)[\.\s]+([A-Z][^\n]+)', 1),
        (r'(\d+\.\d+)\s+([A-Z][^\n]+)', 1),
        (r'^\s*\(([a-z])\)\s+([A-Z][^\n]+)', 2),
        (r'([A-Z][A-Z\s]{5,}[A-Z])\s*$', 1),
        (r'([IVXLCDM]+)[\.\s]+([A-Z][^\n]+)', 1)
    ]

    for pattern, level in patterns:
        for match in re.finditer(pattern, all_text, re.MULTILINE):
            section_counter += 1
            header = match.group(0).strip()
            start_pos = match.end()
            end_pos = start_pos
            while end_pos < len(all_text):
                next_header = any(re.match(p, all_text[end_pos:end_pos+50], re.MULTILINE) for p, _ in patterns)
                if next_header:
                    break
                end_pos += 1
            section_text = header + "\n" + all_text[start_pos:end_pos].strip()
            sections[f"section_{section_counter}"] = {
                'id': f"section_{section_counter}",
                'header': header,
                'level': level,
                'text': section_text,
                'page': 0
            }
    return sections
def get_sentence_context(sentences: list, target_sentence: str) -> str:
    try:
        index = sentences.index(target_sentence)
        if index > 0:
            prev_sentence = sentences[index - 1]
            if len(prev_sentence.split()) < 50:
                return prev_sentence + " " + target_sentence
    except ValueError:
        pass
    return target_sentence


def extract_policy_requirements(text: str) -> list[dict]:
    requirements = []
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z0-9])', text)

    policy_verbs = r'\b(establish|develop|implement|create|submit|report|coordinate|modernize|digitize|require|fund|enhance|allocate|deliver|plan|strengthen|provide|ensure)\b'
    obligation_terms = r'\b(shall|must|will|should|is required to|are required to|is to|are to)\b'
    timeframe_terms = r'\b(\d+\s+days|\d+\s+months|\d+\s+years|annually|quarterly|within\s+\d+)\b'

    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
        score = 0
        if re.search(policy_verbs, sentence, re.IGNORECASE):
            score += 3
        if re.search(obligation_terms, sentence, re.IGNORECASE):
            score += 3
        if re.search(timeframe_terms, sentence, re.IGNORECASE):
            score += 2
        if re.search(r'\b(agency|department|secretary|administrator|director|office)\b', sentence, re.IGNORECASE):
            score += 2
        if score >= 5:
            requirements.append({
                'text': sentence,
                'context': get_sentence_context(sentences, sentence),
                'score': score
            })

    requirements.sort(key=lambda x: x['score'], reverse=True)
    return requirements


def extract_policy_action(text: str) -> str:
    verbs = [
        'establish', 'develop', 'implement', 'create', 'submit', 'report', 
        'coordinate', 'modernize', 'digitize', 'require', 'fund', 'enhance', 
        'allocate', 'deliver', 'plan', 'strengthen', 'provide', 'ensure'
    ]
    lower = text.lower()
    for verb in verbs:
        pattern = fr'\b({verb})\b\s+([a-z]{{1,20}}\s+){{0,3}}([a-z]{{3,20}})'
        m = re.search(pattern, lower)
        if m:
            return f"{m.group(1).title()} {m.group(3).title()}"
    words = lower.split()
    for i, w in enumerate(words):
        if w in verbs and i < len(words) - 1:
            return f"{w.title()} {' '.join(words[i+1:i+3])}"
    return " ".join(text.split()[:3]).title()


def extract_best_sentence(text: str) -> str:
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z0-9])', text)
    if not sentences:
        return (text[:100] + "...") if text else ""
    scored = []
    for s in sentences:
        score = 0
        words = len(s.split())
        if 10 <= words <= 40:
            score += 2
        if re.search(r'\b(shall|must|will|should|require|implement|establish|develop)\b', s, re.IGNORECASE):
            score += 3
        if re.search(r'\b(day|week|month|year|annually|quarterly)\b', s, re.IGNORECASE):
            score += 1
        if re.search(r'\b(\d+%|\d+\s+percent|\$\d+|\d+\s+dollars)\b', s, re.IGNORECASE):
            score += 1
        scored.append((score, s))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


def extract_requirements_from_text(text: str) -> list[dict]:
    requirements = []
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z0-9])', text)
    for sentence in sentences:
        sentence = sentence.strip()
        if is_likely_requirement(sentence):
            requirements.append({
                'text': sentence,
                'context': get_sentence_context(sentences, sentence)
            })
    return requirements


def extract_fallback_components(text: str) -> list[dict]:
    components = []
    c = 0
    policy_requirements = extract_policy_requirements(text)
    if policy_requirements:
        for req in policy_requirements:
            c += 1
            components.append({
                'section_id': "fallback",
                'section_name': "Policy Requirement",
                'component': extract_policy_action(req['text']),
                'description': req['text'],
                'context': req['context'],
                'level': 3,
                'id': f"comp_{c}"
            })
    else:
        sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z0-9])', text)
        for sentence in sentences:
            sentence = sentence.strip()
            if is_likely_requirement(sentence):
                words = sentence.split()
                name = " ".join(words[:min(8, len(words))])
                if len(name) > 50:
                    name = name[:47] + "..."
                c += 1
                components.append({
                    'section_id': "fallback",
                    'section_name': "Unstructured Content",
                    'component': name,
                    'description': sentence,
                    'context': sentence,
                    'level': 3,
                    'id': f"comp_{c}"
                })
    return components


def clean_component_name(header: str) -> str:
    if len(header) > 50:
        header = header[:47] + "..."
    return header


def identify_components(sections: dict) -> list[dict]:
    logger.info("🔍 Identifying components from document structure...")
    components = []
    c = 0

    sections_by_level = {}
    for sid, s in sections.items():
        sections_by_level.setdefault(s['level'], []).append((sid, s))

    for level in sorted(sections_by_level.keys()):
        if level <= 2:
            for sid, s in sections_by_level[level]:
                name = clean_component_name(s['header'])
                policy_requirements = extract_policy_requirements(s['text'])
                source_requirements = policy_requirements or extract_requirements_from_text(s['text'])
                if source_requirements:
                    for req in source_requirements:
                        c += 1
                        components.append({
                            'section_id': sid,
                            'section_name': s['header'],
                            'component': name,
                            'description': req['text'],
                            'context': req.get('context', s['text'][:500]),
                            'level': s['level'],
                            'id': f"comp_{c}"
                        })
                else:
                    c += 1
                    components.append({
                        'section_id': sid,
                        'section_name': s['header'],
                        'component': name,
                        'description': extract_best_sentence(s['text']),
                        'context': s['text'][:500],
                        'level': s['level'],
                        'id': f"comp_{c}"
                    })
        else:
            for sid, s in sections_by_level[level]:
                policy_requirements = extract_policy_requirements(s['text'])
                for req in policy_requirements:
                    c += 1
                    action = extract_policy_action(req['text'])
                    name = f"{s['header']} - {action}"
                    if len(name) > 50:
                        name = name[:47] + "..."
                    components.append({
                        'section_id': sid,
                        'section_name': s['header'],
                        'component': name,
                        'description': req['text'],
                        'context': req['context'],
                        'level': s['level'],
                        'id': f"comp_{c}"
                    })

    if not components:
        logger.info("  → No structured components found, using fallback...")
        all_text = "\n".join([s['text'] for s in sections.values()])
        components.extend(extract_fallback_components(all_text))

    logger.info(f"✅ Identified {len(components)} components")
    return components
def suggest_source_document(pdf_path: Path, text: str) -> str:
    """
    Suggest a human-readable document title using PDF metadata or early content.
    Robust against pikepdf object types by normalizing everything to str.
    """
    logger.info("🔍 Identifying source document name...")

    def to_plain_text(value) -> str:
        """Safely convert bytes/pikepdf objects/None to a plain Python str."""
        if value is None:
            return ""
        try:
            if isinstance(value, bytes):
                return value.decode("utf-8", errors="ignore")
            # pikepdf objects stringify reasonably; fallback if needed
            return str(value)
        except Exception:
            return ""

    suggested_title = ""

    # Ensure text is a plain string
    text = to_plain_text(text)

    # Try PDF metadata via pikepdf (optional)
    try:
        import pikepdf
        with pikepdf.open(pdf_path) as pdf:
            meta_title_obj = pdf.docinfo.get('/Title')
            title = to_plain_text(meta_title_obj)
            if title:
                logger.info(f"  → Found title in PDF metadata: {title}")
                suggested_title = title
    except Exception as e:
        logger.info(f"  → Could not extract metadata with pikepdf (optional): {e}")

    # If no usable metadata title, scan first chunk of text
    if not suggested_title or len(suggested_title) < 10:
        patterns = [
            r"(?i)(\d+(?:st|nd|rd|th)\s+Century\s+[A-Za-z\s]+Act(?:\s+of\s+\d{4})?)",
            r"(?i)((?:The\s+)?[A-Z][A-Za-z\s]+Act\s+of\s+\d{4})",
            r"(?i)((?:The\s+)?[A-Z][A-Za-z\s]+\s+Act)",
            r"(?i)(Public\s+Law\s+\d+[-–]\d+)",
            r"(?i)(Title\s+\d+[A-Z]*\s+of\s+the\s+.+?\s+Code)",
            r"(?i)(\d+\s+U\.?S\.?C\.?\s+.*)",
            r"(?i)(Code\s+of\s+Federal\s+Regulations\s+.*)",
            r"(?i)(Executive\s+Order\s+\d+)",
            r"(?i)(Federal\s+Register\s+.*)",
            r"([A-Z][A-Z\s]{10,}(?:\s+[A-Z]+){1,})"
        ]
        first_pages = text[:8000]  # early content likely contains the title block
        for pattern in patterns:
            matches = re.findall(pattern, first_pages)
            if matches:
                # Some patterns use capture groups; flatten to the longest string
                if isinstance(matches[0], tuple):
                    matches = [m[0] for m in matches]
                matches.sort(key=len, reverse=True)
                result = to_plain_text(matches[0])
                logger.info(f"  → Found title in document: {result}")
                suggested_title = result
                break

    # Cleanup
    if suggested_title:
        suggested_title = re.sub(r'(?<!\d)l(?=\d)', '1', suggested_title)  # fix OCR l→1 before digits
        suggested_title = re.sub(r'\s+', ' ', suggested_title).strip()

    # Fallback to filename
    if not suggested_title:
        suggested_title = pdf_path.stem.replace('_', ' ').title()

    return suggested_title
def validate_data(components: list) -> list:
    logger.info("🧪 Validating component data quality...")
    valid = []
    for comp in components:
        if not comp['description'] or len(comp['description']) < 20:
            continue
        comp['description'] = clean_text(comp['description'])
        if len(comp['component']) < 5:
            comp['component'] = extract_policy_action(comp['description'])
            if len(comp['component']) > 50:
                comp['component'] = comp['component'][:47] + "..."
        valid.append(comp)
    logger.info(f"✅ {len(valid)} valid components after validation")
    return valid


def format_inventory_rows(components, source_name, component_url, mode_label):
    logger.info(f"🛠️ Formatting {mode_label} component rows...")
    rows = []
    for component in components:
        description = clean_text(component['description'])
        rows.append({
            "Source": source_name,
            "Component": component['component'],
            "Component Description": description,
            "Component URL": component_url
        })
    logger.info(f"✅ {len(rows)} {mode_label} components ready.")
    return rows


def save_validated_excel(enabling_rows, dependent_rows, source_name, directory: Path | None = None) -> Path | None:
    """Save validated data to Excel in current working directory (or provided directory)."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    script_dir = directory or Path.cwd()

    safe_source_name = re.sub(r'\s+', ' ', source_name.replace('\n', ' ').replace('\r', ' ')).strip()
    safe_source_name = re.sub(r'[\\/*?:"<>|]', '_', safe_source_name)
    # Truncate source name to ensure full path is under 240 chars
    max_filename_len = 240 - len(str(script_dir)) - len("ivn_requirements__" + timestamp + ".xlsx")
    if len(safe_source_name) > max_filename_len:
        safe_source_name = safe_source_name[:max_filename_len] + "..."


    output_file = script_dir / f"ivn_requirements_{safe_source_name}_{timestamp}.xlsx"
    max_path_len = 240
    if len(str(output_file)) > max_path_len:
        logger.warning(f"⚠️ Output path is too long ({len(str(output_file))} chars). Saving to Desktop with short filename.")
        try:
            import os
            desktop = Path(os.path.expanduser("~")) / "Desktop"
            output_file = desktop / f"ivn_requirements_short_{timestamp}.xlsx"
        except Exception as e:
            logger.error(f"❌ Error saving to Desktop: {e}. Using short fallback filename in current directory.")
            output_file = script_dir / f"ivn_requirements_short_{timestamp}.xlsx"

    try:
        save_start = time.time()
        logger.info("💾 Saving Excel file with both component tables...")
        df_enabling = pd.DataFrame(enabling_rows)
        df_dependent = pd.DataFrame(dependent_rows)

        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            df_enabling.to_excel(writer, sheet_name='Enabling Components', index=False)
            df_dependent.to_excel(writer, sheet_name='Dependent Components', index=False)

            workbook = writer.book
            wrap_format = workbook.add_format({'text_wrap': True})
            header_format = workbook.add_format({
                'bold': True, 'text_wrap': True, 'valign': 'top', 'bg_color': '#D9D9D9'
            })

            ws_en = writer.sheets['Enabling Components']
            for i, col in enumerate(df_enabling.columns):
                column_width = max(15, min(50, len(col) + 2))
                ws_en.set_column(i, i, column_width, wrap_format)
            desc_col = df_enabling.columns.get_loc("Component Description")
            ws_en.set_column(desc_col, desc_col, 80, wrap_format)

            ws_dep = writer.sheets['Dependent Components']
            for i, col in enumerate(df_dependent.columns):
                column_width = max(15, min(50, len(col) + 2))
                ws_dep.set_column(i, i, column_width, wrap_format)
            desc_col = df_dependent.columns.get_loc("Component Description")
            ws_dep.set_column(desc_col, desc_col, 80, wrap_format)

        logger.info(f"📤 Excel file saved: {output_file}")
        logger.info(f"   - Enabling Components: {len(enabling_rows)} items")
        logger.info(f"   - Dependent Components: {len(dependent_rows)} items")
        logger.info(f"   - Save time: {time.time() - save_start:.1f}s")

        display(FileLink(str(output_file)))
        return output_file

    except Exception as e:
        logger.error(f"❌ Error saving Excel file: {e}")
        fallback_file = script_dir / f"ivn_requirements_export_{timestamp}.xlsx"
        logger.info(f"⚠️ Trying fallback: {fallback_file}")
        try:
            with pd.ExcelWriter(fallback_file, engine='xlsxwriter') as writer:
                pd.DataFrame(enabling_rows).to_excel(writer, sheet_name='Enabling Components', index=False)
                pd.DataFrame(dependent_rows).to_excel(writer, sheet_name='Dependent Components', index=False)
            logger.info(f"✅ Successfully saved with fallback filename")
            display(FileLink(str(fallback_file)))
            return fallback_file
        except Exception as e2:
            logger.error(f"❌ Final error saving Excel file: {e2}")
            return None
def run_pipeline(pdf_path: Path, component_url: str, source_name: str | None = None):
    """Run the end-to-end extraction pipeline and return dataframes + saved path."""
    overall_start = time.time()
    timing_file = Path.cwd() / "ivn_operation_times.json"
    operation_names = [
        "Extract Raw Text",
        "Suggest Source Title",
        "Extract Layout",
        "Identify Sections",
        "Identify Components",
        "Validate Components",
        "Format Inventory Rows",
        "Save Excel File"
    ]
    timings = OrderedDict()
    prev_timings = {}
    if timing_file.exists():
        try:
            with open(timing_file, "r") as f:
                prev_timings = json.load(f)
        except Exception:
            prev_timings = {}

    def eta_string(seconds):
        m, s = divmod(int(seconds), 60)
        return f"{m}m {s}s"

    total_ops = len(operation_names)
    completed_ops = 0
    remaining_ops = total_ops
    est_total = sum([prev_timings.get(op, 30) for op in operation_names])
    op_start = time.time()
    print(f"Starting IVN Extraction Pipeline...")
    for idx, op in enumerate(operation_names):
        print(f"Operation {idx+1}/{total_ops}: {op}")
        if op == "Extract Raw Text":
            op_start = time.time()
            raw_text = extract_text(pdf_path)
            op_end = time.time()
        elif op == "Suggest Source Title":
            op_start = time.time()
            if not source_name:
                source_name = suggest_source_document(pdf_path, raw_text)
            op_end = time.time()
        elif op == "Extract Layout":
            op_start = time.time()
            layout_elements = extract_text_with_layout(pdf_path)
            op_end = time.time()
        elif op == "Identify Sections":
            op_start = time.time()
            sections = identify_sections(layout_elements)
            op_end = time.time()
        elif op == "Identify Components":
            op_start = time.time()
            components = identify_components(sections)
            op_end = time.time()
        elif op == "Validate Components":
            op_start = time.time()
            valid_components = validate_data(components)
            op_end = time.time()
        elif op == "Format Inventory Rows":
            op_start = time.time()
            enabling_rows = format_inventory_rows(valid_components, source_name, component_url, "Enabling")
            dependent_rows = format_inventory_rows(valid_components, source_name, component_url, "Dependent")
            op_end = time.time()
        elif op == "Save Excel File":
            op_start = time.time()
            output_file = save_validated_excel(enabling_rows, dependent_rows, source_name)
            op_end = time.time()
        else:
            op_start = time.time()
            op_end = time.time()

        elapsed = op_end - op_start
        timings[op] = elapsed
        completed_ops += 1
        remaining_ops = total_ops - completed_ops
        avg_prev = prev_timings.get(op, 30)
        est_remaining = sum([prev_timings.get(operation_names[i], 30) for i in range(completed_ops, total_ops)])
        print(f"  - Elapsed: {eta_string(elapsed)}")
        print(f"  - Estimated Remaining: {eta_string(est_remaining)}")
        print(f"  - Operations Complete: {completed_ops}")
        print(f"  - Operations Remaining: {remaining_ops}")
        print(f"  - Estimated Time to Complete: {eta_string(est_remaining)}")
        print(f"  - Actual Time for Operation: {eta_string(elapsed)}")
        print()

    # Save timings for future runs
    try:
        with open(timing_file, "w") as f:
            json.dump({op: timings[op] for op in operation_names}, f, indent=2)
    except Exception as e:
        logger.error(f"❌ Error saving operation timings: {e}")

    print(f"🏁 Completed | Total elapsed: {eta_string(time.time() - overall_start)}")
    print(f"📊 Results saved to: {output_file}")

    df_enabling = pd.DataFrame(enabling_rows)
    df_dependent = pd.DataFrame(dependent_rows)
    return df_enabling, df_dependent, output_file

if __name__ == "__main__":
    import argparse
    import tkinter as tk
    from tkinter import filedialog
    parser = argparse.ArgumentParser(description="IVN PDF Extraction Pipeline")
    parser.add_argument("--pdf", type=str, help="Path to the PDF file")
    parser.add_argument("--url", type=str, help="Component URL (source document URL)")
    parser.add_argument("--title", type=str, default=None, help="Optional document title override")
    args = parser.parse_args()

    pdf_path = Path(args.pdf) if args.pdf else None
    component_url = args.url if args.url else ""
    source_name = args.title if args.title else None

    if not pdf_path or not pdf_path.exists():
        print("No PDF path provided. Please select a PDF file.")
        try:
            root = tk.Tk()
            root.withdraw()
            file_path = filedialog.askopenfilename(title="Select PDF file", filetypes=[("PDF files", "*.pdf")])
            root.destroy()
            if file_path:
                pdf_path = Path(file_path)
            else:
                print("❌ No PDF file selected. Exiting.")
                exit(1)
        except Exception as e:
            print(f"❌ Error opening file dialog: {e}")
            exit(1)

    if component_url == "":
        component_url = input("Paste the authoritative URL for the PDF (Component URL), or press Enter to leave blank in the output file: ").strip()
        # If user presses Enter, component_url remains blank and will be blank in the output file

    run_pipeline(pdf_path, component_url, source_name)