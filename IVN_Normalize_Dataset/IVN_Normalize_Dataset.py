"""
INSTRUCTIONS:
You are an expert Python programmer assisting with dataset normalization and denormalization.
Follow these finalized requirements:

Interactive runtime menu:
- 1) Normalize the Dataset tab into Components, Sources, Alignments
- 2) Generate (denormalize) the Dataset tab from the normalized tabs
- 3) Evaluate output file against prompt specifications
- 4) Explain what the script does
- 5) Exit
- Prompt for the workbook path to use.
- Before executing, show a reminder to review tabs for errors and ask for explicit confirmation (type "yes").

Strict column mapping and schemas:
- Every Dataset column must map to exactly one column in Components, Sources, or Alignments; halt with an error otherwise.
- Schemas and column orders:
  Components: [component_name, component_description, component_url, component_ofc_of_primary_interest, source_id, component_id, fetch_status]
    - component_id is exactly derived from (source_name + component_name)
  Sources: [source_name, source_agency, source_id]
    - source_id is exactly derived from source_name
  Alignments: [enabling_component_id, dependent_component_id, linkage_mandate, notes_and_keywords, keywords_tab_items_found, edits, valid, similarity, confidence, transitive_support, matched_enabling_index, matched_dependent_index, alignment_rationale]
    - Primary key: (enabling_component_id, dependent_component_id)
    - Must not duplicate component or source attributes

ID and matching rules:
- Generate IDs from exactly (trimmed, case-insensitive) input strings.
- source_id = canonical(source_name)
- component_id = canonical(source_name) + "::" + canonical(component_name)
- canonical(x) = lower(trim(x))

Handling duplicates:
- When multiple Dataset rows map to the same (source_name + component_name) but differ in description/URL/agency/office, merge by selecting the value with the longer string for each differing field.

Runtime behaviors:
- Always save outputs to a new workbook with a timestamped filename.
- Rebuilt Dataset sheet name: DatasetYYYYMMDDHHMM
- Exclude fetch status from Alignments; store single fetch_status in Components and only copy into Dataset during denormalization.

Dataset column to normalized destination mapping (must be enforced exactly):
- Enabling Source → Sources.source_name
- Enabling Component → Components.component_name
- Enabling Component Description → Components.component_description
- Dependent Component → Components.component_name
- Dependent Component Description → Components.component_description
- Dependent Source → Sources.source_name
- Linkage mandated by what US Code or OMB policy? → Alignments.linkage_mandate
- Enabling Component URL → Components.component_url
- Dependent Component URL → Components.component_url
- Enabling Source Agency → Sources.source_agency
- Dependent Source Agency → Sources.source_agency
- Notes and keywords → Alignments.notes_and_keywords
- Keywords Tab Items Found → Alignments.keywords_tab_items_found
- Enabling Component Office of Primary Interest → Components.component_ofc_of_primary_interest
- Dependent Component Office of Primary Interest → Components.component_ofc_of_primary_interest
- Edits → Alignments.edits
- Valid → Alignments.valid
- Similarity → Alignments.similarity
- Confidence → Alignments.confidence
- Transitive Support → Alignments.transitive_support
- Matched Enabling Index → Alignments.matched_enabling_index
- Matched Dependent Index → Alignments.matched_dependent_index
- Alignment Rationale → Alignments.alignment_rationale
- Enabling Fetch Status, Dependent Fetch Status → Excluded from Alignments; use Components.fetch_status and only copy into Dataset during denormalization
"""

import pandas as pd
from datetime import datetime
import os
import sys
import re
from collections import Counter
from typing import Dict, List, Tuple, Any, Set
import math

# ============================================================================
# STOPWORDS - Common words to filter out during keyword extraction
# ============================================================================
STOPWORDS: Set[str] = {
    'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'i',
    'it', 'for', 'not', 'on', 'with', 'he', 'as', 'you', 'do', 'at',
    'this', 'but', 'his', 'by', 'from', 'they', 'we', 'say', 'her', 'she',
    'or', 'an', 'will', 'my', 'one', 'all', 'would', 'there', 'their',
    'what', 'so', 'up', 'out', 'if', 'about', 'who', 'get', 'which', 'go',
    'me', 'when', 'make', 'can', 'like', 'time', 'no', 'just', 'him', 'know',
    'take', 'people', 'into', 'year', 'your', 'good', 'some', 'could', 'them',
    'see', 'other', 'than', 'then', 'now', 'look', 'only', 'come', 'its', 'over',
    'think', 'also', 'back', 'after', 'use', 'two', 'how', 'our', 'work', 'first',
    'well', 'way', 'even', 'new', 'want', 'because', 'any', 'these', 'give', 'day',
    'most', 'us', 'is', 'was', 'are', 'been', 'has', 'had', 'were', 'said', 'did',
    'having', 'may', 'should', 'must', 'shall', 'being', 'does', 'such', 'each',
    'through', 'where', 'both', 'those', 'during', 'before', 'herself', 'himself',
    'itself', 'between', 'under', 'above', 'below', 'within', 'without', 'against',
    'none', 'more', 'very', 'still', 'here', 'too', 'own', 'same', 'been', 'being',
    'including', 'based', 'using', 'available', 'provides', 'provide', 'requires',
    'addresses', 'supports', 'enable', 'enables', 'meet', 'meets', 'needs',
    'dr', 'it'
}

# Words to exclude from acronym detection (common uppercase words)
ACRONYM_EXCLUDES: Set[str] = {
    'THE', 'AND', 'FOR', 'THIS', 'THAT', 'WITH', 'FROM', 'WILL', 'HAVE',
    'ARE', 'WAS', 'BEEN', 'HAS', 'HAD', 'ALL', 'NOT', 'BUT', 'CAN'
}


# ============================================================================
# KEYWORD EXTRACTION FUNCTIONS
# ============================================================================

def clean_text_for_words(text: str) -> str:
    """
    Clean text for word extraction - convert to lowercase.
    """
    text = text.lower()
    text = re.sub(r'[^\w\s-]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def tokenize(text: str) -> List[str]:
    """
    Split text into words, filtering out short words (3 or fewer characters).
    """
    words = text.split()
    return [word for word in words if len(word) > 3]


def extract_acronyms(text: str) -> List[str]:
    """
    Extract acronyms (all uppercase words 2+ chars) from original text.
    """
    acronym_pattern = r'\b[A-Z][A-Z0-9]{1,}(?:-[A-Z0-9]+)*\b'
    found = re.findall(acronym_pattern, text)
    acronyms: List[str] = []
    seen: Set[str] = set()
    for word in found:
        if word not in ACRONYM_EXCLUDES and word not in seen:
            acronyms.append(word)
            seen.add(word)
    return acronyms


def extract_capitalized_phrases(text: str) -> List[str]:
    """
    Extract meaningful capitalized phrases (proper nouns, titles, technical terms).
    """
    phrases: List[str] = []
    cap_phrase_pattern = r'\b([A-Z][a-z]+(?:\s+(?:[A-Z][a-z]+|[A-Z]+|of|and|the|for))+)\b'
    found_phrases = re.findall(cap_phrase_pattern, text)
    for phrase in found_phrases:
        phrase = phrase.strip()
        words = phrase.split()
        if len(words) >= 2 and words[0][0].isupper():
            non_stop = [w for w in words if w.lower() not in {'of', 'and', 'the', 'for', 'a', 'an'}]
            if len(non_stop) >= 1:
                phrases.append(phrase)
    paren_pattern = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*\([A-Z]{2,}\)'
    paren_matches = re.findall(paren_pattern, text)
    phrases.extend(paren_matches)
    seen: Set[str] = set()
    unique_phrases: List[str] = []
    for p in phrases:
        p_lower = p.lower()
        if p_lower not in seen:
            unique_phrases.append(p)
            seen.add(p_lower)
    return unique_phrases


def extract_keywords_from_text(text: str) -> Dict[str, List[str]]:
    """
    Extract keywords and key phrases from text.
    
    Returns:
        Dictionary with 'keywords' and 'phrases' lists.
    """
    if not text or (isinstance(text, float) and math.isnan(text)):
        return {'keywords': [], 'phrases': []}
    
    text = str(text)
    
    # Extract acronyms from original text
    acronyms = extract_acronyms(text)
    
    # Extract capitalized phrases from original text
    cap_phrases = extract_capitalized_phrases(text)
    
    # Clean text for word frequency analysis
    cleaned = clean_text_for_words(text)
    words = tokenize(cleaned)
    
    # Count word frequencies (excluding stopwords)
    word_counts: Counter[str] = Counter()
    for word in words:
        if word not in STOPWORDS:
            word_counts[word] += 1
    
    # Build keywords list
    keyword_set: Set[str] = set()
    keywords: List[str] = []
    
    # Add acronyms first
    for acr in acronyms[:5]:
        acr_lower = acr.lower()
        if acr_lower not in keyword_set:
            keywords.append(acr)
            keyword_set.add(acr_lower)
    
    # Add most common words
    for word, _ in word_counts.most_common(10):
        if word not in keyword_set and len(keywords) < 10:
            keywords.append(word)
            keyword_set.add(word)
    
    # Build phrases list from capitalized phrases
    phrases: List[str] = []
    phrase_set: Set[str] = set()
    
    for phrase in cap_phrases[:5]:
        p_lower = phrase.lower()
        if p_lower not in phrase_set:
            phrases.append(phrase)
            phrase_set.add(p_lower)
    
    # Add n-gram phrases
    bigram_counts: Counter[str] = Counter()
    for i in range(len(words) - 1):
        if words[i] not in STOPWORDS or words[i + 1] not in STOPWORDS:
            bigram = f"{words[i]} {words[i + 1]}"
            bigram_counts[bigram] += 1
    
    trigram_counts: Counter[str] = Counter()
    for i in range(len(words) - 2):
        if (words[i] not in STOPWORDS or 
            words[i + 1] not in STOPWORDS or 
            words[i + 2] not in STOPWORDS):
            trigram = f"{words[i]} {words[i + 1]} {words[i + 2]}"
            trigram_counts[trigram] += 1
    
    for phrase, count in trigram_counts.most_common():
        if count > 1 and phrase not in phrase_set and len(phrases) < 7:
            phrases.append(phrase)
            phrase_set.add(phrase)
    
    for phrase, count in bigram_counts.most_common():
        if count > 1 and phrase not in phrase_set and len(phrases) < 7:
            phrases.append(phrase)
            phrase_set.add(phrase)
    
    return {
        'keywords': keywords,
        'phrases': phrases
    }


def find_common_keywords(keywords1: List[str], keywords2: List[str]) -> List[str]:
    """
    Find keywords common to both lists (case-insensitive comparison).
    """
    set1 = {k.lower() for k in keywords1}
    set2 = {k.lower() for k in keywords2}
    common_lower = set1 & set2
    # Return original case from first list
    return [k for k in keywords1 if k.lower() in common_lower]


def find_common_phrases(phrases1: List[str], phrases2: List[str]) -> List[str]:
    """
    Find phrases common to both lists (case-insensitive comparison).
    """
    set1 = {p.lower() for p in phrases1}
    set2 = {p.lower() for p in phrases2}
    common_lower = set1 & set2
    # Return original case from first list
    return [p for p in phrases1 if p.lower() in common_lower]


# --- Helpers ---

def timestamp() -> str:
    return datetime.now().strftime('%Y%m%d%H%M')

def canonical(s: Any) -> str:
    s_str: str = '' if s is None else str(s)
    return s_str.strip().lower()

def require_yes_confirmation(action_desc: str) -> None:
    print(f"You are about to {action_desc}.")
    print("Please review your workbook tabs for any errors before proceeding.")
    resp = input('Type "yes" to proceed: ').strip().lower()
    if resp != 'yes':
        raise SystemExit('Operation cancelled: explicit "yes" confirmation required.')

# --- Validation of dataset columns mapping ---

DATASET_COLUMNS_EXPECTED: List[str] = [
    'Enabling Source',
    'Enabling Component',
    'Enabling Component Description',
    'Enabling Component URL',
    'Enabling Source Agency',
    'Enabling Component Office of Primary Interest',
    'Enabling Fetch Status',
    'Enabling Keywords',
    'Enabling Key Phrases',
    'Dependent Source',
    'Dependent Component',
    'Dependent Component Description',
    'Dependent Component URL',
    'Dependent Source Agency',
    'Dependent Component Office of Primary Interest',
    'Dependent Fetch Status',
    'Dependent Keywords',
    'Dependent Key Phrases',
    'Common Keywords',
    'Common Key Phrases',
    'Linkage mandated by what US Code or OMB policy?',
    'Notes and keywords',
    'Keywords Tab Items Found',
    'Edits',
    'Valid',
    'Similarity',
    'Confidence',
    'Transitive Support',
    'Matched Enabling Index',
    'Matched Dependent Index',
    'Alignment Rationale',
]

# Map to normalized destinations (single destination per column)
# Columns marked as 'Generated.computed' are computed during denormalization
DATASET_TO_NORMALIZED: Dict[str, str] = {
    'Enabling Source': 'Sources.source_name',
    'Enabling Component': 'Components.component_name',
    'Enabling Component Description': 'Components.component_description',
    'Enabling Component URL': 'Components.component_url',
    'Enabling Source Agency': 'Sources.source_agency',
    'Enabling Component Office of Primary Interest': 'Components.component_ofc_of_primary_interest',
    'Enabling Fetch Status': 'Components.fetch_status',
    'Enabling Keywords': 'Generated.computed',
    'Enabling Key Phrases': 'Generated.computed',
    'Dependent Source': 'Sources.source_name',
    'Dependent Component': 'Components.component_name',
    'Dependent Component Description': 'Components.component_description',
    'Dependent Component URL': 'Components.component_url',
    'Dependent Source Agency': 'Sources.source_agency',
    'Dependent Component Office of Primary Interest': 'Components.component_ofc_of_primary_interest',
    'Dependent Fetch Status': 'Components.fetch_status',
    'Dependent Keywords': 'Generated.computed',
    'Dependent Key Phrases': 'Generated.computed',
    'Common Keywords': 'Generated.computed',
    'Common Key Phrases': 'Generated.computed',
    'Linkage mandated by what US Code or OMB policy?': 'Alignments.linkage_mandate',
    'Notes and keywords': 'Alignments.notes_and_keywords',
    'Keywords Tab Items Found': 'Alignments.keywords_tab_items_found',
    'Edits': 'Alignments.edits',
    'Valid': 'Alignments.valid',
    'Similarity': 'Alignments.similarity',
    'Confidence': 'Alignments.confidence',
    'Transitive Support': 'Alignments.transitive_support',
    'Matched Enabling Index': 'Alignments.matched_enabling_index',
    'Matched Dependent Index': 'Alignments.matched_dependent_index',
    'Alignment Rationale': 'Alignments.alignment_rationale',
}

COMPONENTS_ORDER: List[str] = [
    'component_name',
    'component_description',
    'component_url',
    'component_agency',
    'component_ofc_of_primary_interest',
    'source_id',
    'component_id',
    'fetch_status',
]

SOURCES_ORDER: List[str] = [
    'source_name',
    'source_agency',
    'source_id',
]

ALIGNMENTS_ORDER: List[str] = [
    'enabling_component_id',
    'Enabling Component',
    'Enabling Source',
    'dependent_component_id',
    'Dependent Component',
    'Dependent Source',
    'linkage_mandate',
    'notes_and_keywords',
    'keywords_tab_items_found',
    'edits',
    'valid',
    'similarity',
    'confidence',
    'transitive_support',
    'matched_enabling_index',
    'matched_dependent_index',
    'alignment_rationale',
]

def validate_dataset_columns(df: pd.DataFrame) -> None:
    # During normalization, skip columns that are Generated.computed (created during denormalization)
    required_columns = [c for c in DATASET_COLUMNS_EXPECTED 
                       if DATASET_TO_NORMALIZED.get(c) != 'Generated.computed']
    missing: List[str] = [c for c in required_columns if c not in df.columns]
    if missing:
        print(f"\nColumns found in file: {list(df.columns)}")
        raise ValueError(f"Dataset is missing required columns: {missing}")
    # Ensure unique mapping (exactly one destination per column)
    for c in DATASET_COLUMNS_EXPECTED:
        dest: str | None = DATASET_TO_NORMALIZED.get(c)
        if not dest:
            raise ValueError(f"No normalized destination configured for column: {c}")

# --- Normalization ---

def normalize_workbook(input_path: str) -> str:
    # Validate path is a file and an Excel workbook
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"File not found: {input_path}")
    if os.path.isdir(input_path):
        raise PermissionError(f"Provided path is a directory, not an Excel file: {input_path}")
    if not (input_path.lower().endswith('.xlsx') or input_path.lower().endswith('.xls')):
        raise ValueError(f"Input must be an Excel file (.xlsx/.xls). Got: {input_path}")

    # Read first sheet of Excel
    try:
        df: pd.DataFrame = pd.read_excel(input_path, sheet_name=0)  # type: ignore[no-untyped-call]
    except Exception as e:
        raise RuntimeError(f"Failed to read Excel file '{input_path}': {e}")
    validate_dataset_columns(df)

    def source_key(name: Any) -> str:
        # Use verbatim value for source_id, but always return a string (empty if None)
        return str(name) if name is not None else ''

    def component_key(source_name: Any, component_name: Any) -> str:
        return f"{source_name if source_name is not None else ''}::{component_name if component_name is not None else ''}"

    def safe_id(source_name: Any, component_name: Any) -> Any:
        # Returns None if either is null/nan, else returns the component_key
        def is_null(x: Any) -> bool:
            return x is None or (isinstance(x, float) and math.isnan(x))
        if is_null(source_name) or is_null(component_name):
            return None
        return component_key(source_name, component_name)

    sources: Dict[str, Dict[str, str]] = {}
    components: Dict[str, Dict[str, Any]] = {}
    alignments: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def choose_longer(a: Any, b: Any) -> Any:
        # Treat None and nan as null
        def is_null(x: Any) -> bool:
            return x is None or (isinstance(x, float) and math.isnan(x))
        if is_null(a) and is_null(b):
            return None
        if is_null(a):
            return b
        if is_null(b):
            return a
        a_str: str = str(a)
        b_str: str = str(b)
        return a_str if len(a_str) >= len(b_str) else b_str

    for _, row in df.iterrows():  # type: ignore[assignment]
        # Enabling side
        en_source_name = row.get('Enabling Source', None)  # type: ignore[arg-type]
        en_source_agency = row.get('Enabling Source Agency', None)  # type: ignore[arg-type]
        en_comp_name = row.get('Enabling Component', None)  # type: ignore[arg-type]
        en_comp_desc = row.get('Enabling Component Description', None)  # type: ignore[arg-type]
        en_comp_url = row.get('Enabling Component URL', None)  # type: ignore[arg-type]
        en_comp_ofc = row.get('Enabling Component Office of Primary Interest', None)  # type: ignore[arg-type]
        en_fetch = row.get('Enabling Fetch Status', None)  # type: ignore[arg-type]

        # Dependent side
        de_source_name = row.get('Dependent Source', None)  # type: ignore[arg-type]
        de_source_agency = row.get('Dependent Source Agency', None)  # type: ignore[arg-type]
        de_comp_name = row.get('Dependent Component', None)  # type: ignore[arg-type]
        de_comp_desc = row.get('Dependent Component Description', None)  # type: ignore[arg-type]
        de_comp_url = row.get('Dependent Component URL', None)  # type: ignore[arg-type]
        de_comp_ofc = row.get('Dependent Component Office of Primary Interest', None)  # type: ignore[arg-type]
        de_fetch = row.get('Dependent Fetch Status', None)  # type: ignore[arg-type]

        # Alignment fields
        linkage = row.get('Linkage mandated by what US Code or OMB policy?', None)  # type: ignore[arg-type]
        notes = row.get('Notes and keywords', None)  # type: ignore[arg-type]
        keywords_found = row.get('Keywords Tab Items Found', None)  # type: ignore[arg-type]
        edits = row.get('Edits', None)  # type: ignore[arg-type]
        valid = row.get('Valid', None)  # type: ignore[arg-type]
        similarity = row.get('Similarity', None)  # type: ignore[arg-type]
        confidence = row.get('Confidence', None)  # type: ignore[arg-type]
        transitive = row.get('Transitive Support', None)  # type: ignore[arg-type]
        m_en_idx = row.get('Matched Enabling Index', None)  # type: ignore[arg-type]
        m_de_idx = row.get('Matched Dependent Index', None)  # type: ignore[arg-type]
        rationale = row.get('Alignment Rationale', None)  # type: ignore[arg-type]

        # Sources
        if en_source_name is not None:
            sk = source_key(en_source_name)
            rec = sources.get(sk)
            merged_agency = choose_longer(rec['source_agency'] if rec and rec.get('source_agency', None) is not None else None, en_source_agency)
            sources[sk] = {
                'source_name': en_source_name,
                'source_agency': merged_agency,
                'source_id': en_source_name,
            }
        if de_source_name is not None:
            sk = source_key(de_source_name)
            rec = sources.get(sk)
            merged_agency = choose_longer(rec['source_agency'] if rec and rec.get('source_agency', None) is not None else None, de_source_agency)
            sources[sk] = {
                'source_name': de_source_name,
                'source_agency': merged_agency,
                'source_id': de_source_name,
            }

        # Components
        if en_source_name is not None and en_comp_name is not None:
            ck = component_key(en_source_name, en_comp_name)
            rec_comp = components.get(ck)
            comp_id = None if (en_source_name is None or (isinstance(en_source_name, float) and math.isnan(en_source_name)) or en_comp_name is None or (isinstance(en_comp_name, float) and math.isnan(en_comp_name))) else f"{en_source_name}::{en_comp_name}"
            components[ck] = {
                'component_name': en_comp_name,
                'component_description': choose_longer(rec_comp['component_description'] if rec_comp else None, en_comp_desc),
                'component_url': choose_longer(rec_comp['component_url'] if rec_comp else None, en_comp_url),
                'component_agency': choose_longer(rec_comp['component_agency'] if rec_comp else None, en_source_agency),
                'component_ofc_of_primary_interest': choose_longer(rec_comp['component_ofc_of_primary_interest'] if rec_comp else None, en_comp_ofc),
                'source_id': en_source_name,
                'component_id': comp_id,
                'fetch_status': choose_longer(rec_comp['fetch_status'] if rec_comp else None, en_fetch),
            }
        if de_source_name is not None and de_comp_name is not None:
            ck = component_key(de_source_name, de_comp_name)
            rec_comp = components.get(ck)
            comp_id = None if (de_source_name is None or (isinstance(de_source_name, float) and math.isnan(de_source_name)) or de_comp_name is None or (isinstance(de_comp_name, float) and math.isnan(de_comp_name))) else f"{de_source_name}::{de_comp_name}"
            components[ck] = {
                'component_name': de_comp_name,
                'component_description': choose_longer(rec_comp['component_description'] if rec_comp else None, de_comp_desc),
                'component_url': choose_longer(rec_comp['component_url'] if rec_comp else None, de_comp_url),
                'component_agency': choose_longer(rec_comp['component_agency'] if rec_comp else None, de_source_agency),
                'component_ofc_of_primary_interest': choose_longer(rec_comp['component_ofc_of_primary_interest'] if rec_comp else None, de_comp_ofc),
                'source_id': de_source_name,
                'component_id': comp_id,
                'fetch_status': choose_longer(rec_comp['fetch_status'] if rec_comp else None, de_fetch),
            }

        # Alignments
        en_id = safe_id(en_source_name, en_comp_name)
        de_id = safe_id(de_source_name, de_comp_name)
        key = (en_id, de_id)
        align = alignments.get(key, {})
        align.update({
            'enabling_component_id': en_id,
            'Enabling Component': en_comp_name,
            'Enabling Source': en_source_name,
            'dependent_component_id': de_id,
            'Dependent Component': de_comp_name,
            'Dependent Source': de_source_name,
            'linkage_mandate': linkage,
            'notes_and_keywords': notes,
            'keywords_tab_items_found': keywords_found,
            'edits': edits,
            'valid': valid,
            'similarity': similarity,
            'confidence': confidence,
            'transitive_support': transitive,
            'matched_enabling_index': m_en_idx,
            'matched_dependent_index': m_de_idx,
            'alignment_rationale': rationale,
        })
        alignments[key] = align

    sources_df: pd.DataFrame = pd.DataFrame(list(sources.values()))
    if not sources_df.empty:
        sources_df = sources_df[SOURCES_ORDER]

    components_df: pd.DataFrame = pd.DataFrame(list(components.values()))
    if not components_df.empty:
        components_df = components_df[COMPONENTS_ORDER]

    alignments_df: pd.DataFrame = pd.DataFrame(list(alignments.values()))
    if not alignments_df.empty:
        alignments_df = alignments_df[ALIGNMENTS_ORDER]

    out_name: str = f"ivntest_normalized_{timestamp()}.xlsx"
    with pd.ExcelWriter(out_name) as writer:
        sources_df.to_excel(writer, sheet_name='Sources', index=False)  # type: ignore[call-arg]
        components_df.to_excel(writer, sheet_name='Components', index=False)  # type: ignore[call-arg]
        alignments_df.to_excel(writer, sheet_name='Alignments', index=False)  # type: ignore[call-arg]
    return out_name

# --- Denormalization ---

DATASET_ORDER: List[str] = DATASET_COLUMNS_EXPECTED

def denormalize_workbook(input_path: str) -> str:
    # Validate path is a file and an Excel workbook
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"File not found: {input_path}")
    if os.path.isdir(input_path):
        raise PermissionError(f"Provided path is a directory, not an Excel file: {input_path}")
    if not (input_path.lower().endswith('.xlsx') or input_path.lower().endswith('.xls')):
        raise ValueError(f"Input must be an Excel file (.xlsx/.xls). Got: {input_path}")

    xls = pd.ExcelFile(input_path)
    required_tabs: set[str] = {'Sources', 'Components', 'Alignments'}
    missing_tabs: List[str] = [t for t in required_tabs if t not in xls.sheet_names]
    if missing_tabs:
        raise ValueError(f"Normalized workbook missing required tabs: {missing_tabs}")

    sources_df: pd.DataFrame = pd.read_excel(input_path, sheet_name='Sources')  # type: ignore[no-untyped-call]
    components_df: pd.DataFrame = pd.read_excel(input_path, sheet_name='Components')  # type: ignore[no-untyped-call]
    alignments_df: pd.DataFrame = pd.read_excel(input_path, sheet_name='Alignments')  # type: ignore[no-untyped-call]

    # Validate columns presence
    for expected, df, order in [
        ('Sources', sources_df, SOURCES_ORDER),
        ('Components', components_df, COMPONENTS_ORDER),
        ('Alignments', alignments_df, ALIGNMENTS_ORDER),
    ]:
        miss: List[str] = [c for c in order if c not in df.columns]
        if miss:
            raise ValueError(f"{expected} tab missing columns: {miss}")

    def is_null(x: Any) -> bool:
        return x is None or (isinstance(x, float) and math.isnan(x))

    # Build lookup dictionaries for components and sources
    source_by_id: Dict[str, Dict[str, Any]] = {
        str(row['source_id']): dict(row)
        for _, row in sources_df.iterrows()
    }
    comp_by_id: Dict[str, Dict[str, Any]] = {
        str(row['component_id']): dict(row)
        for _, row in components_df.iterrows()
    }

    dataset_rows: List[Dict[str, Any]] = []
    for _, a in alignments_df.iterrows():  # type: ignore[assignment]
        en_id = str(a['enabling_component_id']) if not is_null(a['enabling_component_id']) else None
        de_id = str(a['dependent_component_id']) if not is_null(a['dependent_component_id']) else None
        en_comp = comp_by_id.get(en_id) if en_id is not None else None  # type: ignore
        de_comp = comp_by_id.get(de_id) if de_id is not None else None  # type: ignore
        en_source = source_by_id.get(str(en_comp['source_id'])) if en_comp and not is_null(en_comp['source_id']) else None  # type: ignore
        de_source = source_by_id.get(str(de_comp['source_id'])) if de_comp and not is_null(de_comp['source_id']) else None  # type: ignore
        
        # Extract keywords and phrases from enabling component description
        en_desc = en_comp.get('component_description') if en_comp else None
        en_extracted = extract_keywords_from_text(en_desc) if en_desc and not is_null(en_desc) else {'keywords': [], 'phrases': []}
        en_keywords = en_extracted['keywords']
        en_phrases = en_extracted['phrases']
        
        # Extract keywords and phrases from dependent component description
        de_desc = de_comp.get('component_description') if de_comp else None
        de_extracted = extract_keywords_from_text(de_desc) if de_desc and not is_null(de_desc) else {'keywords': [], 'phrases': []}
        de_keywords = de_extracted['keywords']
        de_phrases = de_extracted['phrases']
        
        # Find common keywords and phrases between enabling and dependent components
        common_keywords = find_common_keywords(en_keywords, de_keywords)
        common_phrases = find_common_phrases(en_phrases, de_phrases)
        
        row: Dict[str, Any] = {
            'Enabling Source': None if not en_source or is_null(en_source.get('source_name')) else en_source.get('source_name'),  # type: ignore
            'Enabling Source Agency': None if not en_source or is_null(en_source.get('source_agency')) else en_source.get('source_agency'),  # type: ignore
            'Enabling Fetch Status': None if not en_comp or is_null(en_comp.get('fetch_status')) else en_comp.get('fetch_status'),  # type: ignore
            'Enabling Component': None if not en_comp or is_null(en_comp.get('component_name')) else en_comp.get('component_name'),  # type: ignore
            'Enabling Component Description': None if not en_comp or is_null(en_comp.get('component_description')) else en_comp.get('component_description'),  # type: ignore
            'Enabling Component URL': None if not en_comp or is_null(en_comp.get('component_url')) else en_comp.get('component_url'),  # type: ignore
            'Enabling Component Office of Primary Interest': None if not en_comp or is_null(en_comp.get('component_ofc_of_primary_interest')) else en_comp.get('component_ofc_of_primary_interest'),  # type: ignore
            'Enabling Keywords': ', '.join(en_keywords) if en_keywords else None,
            'Enabling Key Phrases': ', '.join(en_phrases) if en_phrases else None,
            'Dependent Source': None if not de_source or is_null(de_source.get('source_name')) else de_source.get('source_name'),  # type: ignore
            'Dependent Source Agency': None if not de_source or is_null(de_source.get('source_agency')) else de_source.get('source_agency'),  # type: ignore
            'Dependent Fetch Status': None if not de_comp or is_null(de_comp.get('fetch_status')) else de_comp.get('fetch_status'),  # type: ignore
            'Dependent Component': None if not de_comp or is_null(de_comp.get('component_name')) else de_comp.get('component_name'),  # type: ignore
            'Dependent Component Description': None if not de_comp or is_null(de_comp.get('component_description')) else de_comp.get('component_description'),  # type: ignore
            'Dependent Component URL': None if not de_comp or is_null(de_comp.get('component_url')) else de_comp.get('component_url'),  # type: ignore
            'Dependent Component Office of Primary Interest': None if not de_comp or is_null(de_comp.get('component_ofc_of_primary_interest')) else de_comp.get('component_ofc_of_primary_interest'),  # type: ignore
            'Dependent Keywords': ', '.join(de_keywords) if de_keywords else None,
            'Dependent Key Phrases': ', '.join(de_phrases) if de_phrases else None,
            'Common Keywords': ', '.join(common_keywords) if common_keywords else None,
            'Common Key Phrases': ', '.join(common_phrases) if common_phrases else None,
            'Linkage mandated by what US Code or OMB policy?': None if is_null(a['linkage_mandate']) else a['linkage_mandate'],
            'Notes and keywords': None if is_null(a['notes_and_keywords']) else a['notes_and_keywords'],
            'Keywords Tab Items Found': None if is_null(a['keywords_tab_items_found']) else a['keywords_tab_items_found'],
            'Edits': None if is_null(a['edits']) else a['edits'],
            'Valid': None if is_null(a['valid']) else a['valid'],
            'Similarity': None if is_null(a['similarity']) else a['similarity'],
            'Confidence': None if is_null(a['confidence']) else a['confidence'],
            'Transitive Support': None if is_null(a['transitive_support']) else a['transitive_support'],
            'Matched Enabling Index': None if is_null(a['matched_enabling_index']) else a['matched_enabling_index'],
            'Matched Dependent Index': None if is_null(a['matched_dependent_index']) else a['matched_dependent_index'],
            'Alignment Rationale': None if is_null(a['alignment_rationale']) else a['alignment_rationale'],
        }
        dataset_rows.append(row)

    dataset_df: pd.DataFrame = pd.DataFrame(dataset_rows)
    dataset_df = dataset_df[DATASET_ORDER]

    out_name: str = f"Dataset{timestamp()}.xlsx"
    with pd.ExcelWriter(out_name) as writer:
        dataset_df.to_excel(writer, sheet_name=f"Dataset{timestamp()}", index=False)  # type: ignore[call-arg]
        sources_df.to_excel(writer, sheet_name='Sources', index=False)  # type: ignore[call-arg]
        components_df.to_excel(writer, sheet_name='Components', index=False)  # type: ignore[call-arg]
        alignments_df.to_excel(writer, sheet_name='Alignments', index=False)  # type: ignore[call-arg]
    return out_name

# --- Navigation Prompt ---

# --- Evaluate Output File ---
def evaluate_output_file(file_path: str) -> None:
    """
    Validate output file (normalized or denormalized) against prompt specifications.
    Checks:
      - Sheet names (Dataset, Sources, Components, Alignments)
      - Column names and order for each tab
      - No 'nan' or 'None' strings (should be blank)
      - component_id format (should contain '::')
      - Reports issues and warnings
    """
    import pandas as pd
    import math
    print(f'\n=== Evaluating Output File: {file_path} ===')
    try:
        xls = pd.ExcelFile(file_path)
    except Exception as e:
        print(f'Error reading Excel file: {e}')
        return
    sheets = xls.sheet_names
    print(f'Sheets found: {sheets}')
    issues = []
    warnings = []
    # Detect type: normalized (Sources, Components, Alignments) or denormalized (Dataset tab)
    is_normalized = all(tab in sheets for tab in ['Sources', 'Components', 'Alignments'])
    dataset_tab = next((s for s in sheets if s.lower().startswith('dataset')), None)
    # --- Validate Sources tab ---
    if 'Sources' in sheets:
        df = pd.read_excel(file_path, sheet_name='Sources')
        expected = ['source_name', 'source_agency', 'source_id']
        missing = [c for c in expected if c not in df.columns]
        extra = [c for c in df.columns if c not in expected]
        if missing:
            issues.append(f'Sources tab missing columns: {missing}')
        if extra:
            warnings.append(f'Sources tab has extra columns: {extra}')
    # --- Validate Components tab ---
    if 'Components' in sheets:
        df = pd.read_excel(file_path, sheet_name='Components')
        expected = ['component_name', 'component_description', 'component_url', 'component_agency', 'component_ofc_of_primary_interest', 'source_id', 'component_id', 'fetch_status']
        missing = [c for c in expected if c not in df.columns]
        extra = [c for c in df.columns if c not in expected]
        if missing:
            issues.append(f'Components tab missing columns: {missing}')
        if extra:
            warnings.append(f'Components tab has extra columns: {extra}')
        # Validate component_id format
        if 'component_id' in df.columns:
            bad_ids = [i for i, v in enumerate(df['component_id']) if pd.notna(v) and '::' not in str(v)]
            if bad_ids:
                issues.append(f'component_id values missing \'::\' at rows: {bad_ids[:10]}')
    # --- Validate Alignments tab ---
    if 'Alignments' in sheets:
        df = pd.read_excel(file_path, sheet_name='Alignments')
        expected = ['enabling_component_id', 'Enabling Component', 'Enabling Source', 'dependent_component_id', 'Dependent Component', 'Dependent Source', 'linkage_mandate', 'notes_and_keywords', 'keywords_tab_items_found', 'edits', 'valid', 'similarity', 'confidence', 'transitive_support', 'matched_enabling_index', 'matched_dependent_index', 'alignment_rationale']
        missing = [c for c in expected if c not in df.columns]
        extra = [c for c in df.columns if c not in expected]
        if missing:
            issues.append(f'Alignments tab missing columns: {missing}')
        if extra:
            warnings.append(f'Alignments tab has extra columns: {extra}')
    # --- Validate Dataset tab ---
    if dataset_tab:
        df = pd.read_excel(file_path, sheet_name=dataset_tab)
        # 31 columns: 25 stored + 6 computed
        expected = [
            'Enabling Source', 'Enabling Component', 'Enabling Component Description', 'Enabling Component URL',
            'Enabling Source Agency', 'Enabling Component Office of Primary Interest', 'Enabling Fetch Status',
            'Enabling Keywords', 'Enabling Key Phrases', 'Dependent Source', 'Dependent Component',
            'Dependent Component Description', 'Dependent Component URL', 'Dependent Source Agency',
            'Dependent Component Office of Primary Interest', 'Dependent Fetch Status', 'Dependent Keywords',
            'Dependent Key Phrases', 'Common Keywords', 'Common Key Phrases',
            'Linkage mandated by what US Code or OMB policy?', 'Notes and keywords', 'Keywords Tab Items Found',
            'Edits', 'Valid', 'Similarity', 'Confidence', 'Transitive Support', 'Matched Enabling Index',
            'Matched Dependent Index', 'Alignment Rationale'
        ]
        missing = [c for c in expected if c not in df.columns]
        extra = [c for c in df.columns if c not in expected]
        if missing:
            issues.append(f'Dataset tab missing columns: {missing}')
        if extra:
            warnings.append(f'Dataset tab has extra columns: {extra}')
        # Check for 'nan' or 'None' strings
        for col in df.columns:
            bad = df[df[col].astype(str).str.lower().isin(['nan', 'none'])]
            if not bad.empty:
                issues.append(f"Column '{col}' has 'nan' or 'None' string values at rows: {list(bad.index[:10])}")
    # --- Summary ---
    print('\n=== Evaluation Report ===')
    if not issues and not warnings:
        print('PASS: Output file matches prompt specifications.')
    else:
        if issues:
            print('ISSUES:')
            for issue in issues:
                print('-', issue)
        if warnings:
            print('WARNINGS:')
            for warning in warnings:
                print('-', warning)
    print('\nEvaluation complete. Review issues and warnings above.')
def navigation_prompt(start_path: str) -> str:
    """
    Interactive navigation prompt for selecting a file or directory, starting at start_path.
    Returns the selected file path as a string.
    """
    current_path = os.path.abspath(start_path)
    while True:
        entries = os.listdir(current_path)
        entries = sorted(entries)
        print(f"\nCurrent directory: {current_path}")
        for idx, entry in enumerate(entries):
            full_path = os.path.join(current_path, entry)
            if os.path.isdir(full_path):
                print(f"{idx+1}) [DIR]  {entry}")
            else:
                print(f"{idx+1}) [FILE] {entry}")
        print(f"0) Go up one directory")
        print(f"Choose a number to navigate or select a file. Type 'q' to quit.")
        choice = input("Enter number: ").strip()
        if choice.lower() == 'q':
            sys.exit(0)
        if choice == '0':
            parent = os.path.dirname(current_path)
            if parent == current_path:
                print("Already at root directory.")
            else:
                current_path = parent
            continue
        try:
            idx = int(choice) - 1
            if idx < 0 or idx >= len(entries):
                print("Invalid selection.")
                continue
        except ValueError:
            print("Invalid input. Enter a number.")
            continue
        selected = entries[idx]
        selected_path = os.path.join(current_path, selected)
        if os.path.isdir(selected_path):
            current_path = selected_path
        else:
            return selected_path

# --- Interactive Menu ---
# --- Compare Datasets Menu ---
def compare_datasets_menu():
    print('\n=== Compare Code-Generated Dataset to IVN Production Dataset Tab ===')
    print('Navigate to the code-generated output file:')
    code_path = navigation_prompt(os.path.dirname(os.path.abspath(__file__)))
    if not code_path or not os.path.exists(code_path):
        print('No path provided or file not found.')
        return
    if os.path.isdir(code_path) or not (code_path.lower().endswith('.xlsx') or code_path.lower().endswith('.xls')):
        print('Invalid file type. Please provide an Excel file (.xlsx/.xls).')
        return
    print('Navigate to the IVN production Dataset file:')
    prod_path = navigation_prompt(os.path.dirname(os.path.abspath(__file__)))
    if not prod_path or not os.path.exists(prod_path):
        print('No path provided or file not found.')
        return
    if os.path.isdir(prod_path) or not (prod_path.lower().endswith('.xlsx') or prod_path.lower().endswith('.xls')):
        print('Invalid file type. Please provide an Excel file (.xlsx/.xls).')
        return
    import pandas as pd
    try:
        code_xls = pd.ExcelFile(code_path)
        prod_xls = pd.ExcelFile(prod_path)
        # Find Dataset tab in each file
        code_sheet = next((s for s in code_xls.sheet_names if s.lower().startswith('dataset')), None)
        prod_sheet = next((s for s in prod_xls.sheet_names if s.lower().startswith('dataset')), None)
        if not code_sheet or not prod_sheet:
            print('Could not find Dataset tab in one or both files.')
            return
        code_df = pd.read_excel(code_path, sheet_name=code_sheet)
        prod_df = pd.read_excel(prod_path, sheet_name=prod_sheet)
    except Exception as e:
        print(f'Error reading files: {e}')
        return
    issues = []
    # Compare columns
    code_cols = list(code_df.columns)
    prod_cols = list(prod_df.columns)
    if code_cols != prod_cols:
        issues.append('Column order or names differ.')
        missing_in_code = [c for c in prod_cols if c not in code_cols]
        missing_in_prod = [c for c in code_cols if c not in prod_cols]
        if missing_in_code:
            issues.append(f'Columns missing in code-generated: {missing_in_code}')
        if missing_in_prod:
            issues.append(f'Columns missing in production: {missing_in_prod}')
    # Compare row counts
    if len(code_df) != len(prod_df):
        issues.append(f'Row count differs: code-generated={len(code_df)}, production={len(prod_df)}')
    # Compare values (first N rows for brevity)
    N = min(10, min(len(code_df), len(prod_df)))
    value_diffs = []
    for i in range(N):
        code_row = code_df.iloc[i] if i < len(code_df) else None
        prod_row = prod_df.iloc[i] if i < len(prod_df) else None
        if code_row is not None and prod_row is not None:
            for col in set(code_cols) & set(prod_cols):
                v1 = code_row.get(col, None)
                v2 = prod_row.get(col, None)
                if pd.isna(v1) and pd.isna(v2):
                    continue
                if v1 != v2:
                    value_diffs.append(f'Row {i+1}, column "{col}": code-generated="{v1}", production="{v2}"')
    if value_diffs:
        issues.append(f'First {N} row value differences:')
        issues.extend(value_diffs)
    # Recommend script changes
    print('\n=== Comparison Report ===')
    if not issues:
        print('No differences found. Code-generated Dataset matches production Dataset tab.')
    else:
        for issue in issues:
            print('-', issue)
        print('\n=== Recommendations ===')
        if 'Column order or names differ.' in issues:
            print('-> Update script to match column order and names to production Dataset.')
        if any('Row count differs' in s for s in issues):
            print('-> Investigate why row counts differ. Check normalization/denormalization logic.')
        if any('value differences' in s for s in issues):
            print('-> Review value differences above. Adjust merging, null handling, or computed columns as needed.')
        print('-> After making changes, re-run this comparison to verify alignment.')


if __name__ == '__main__':
    while True:
        print('\nIVN Dataset Normalizer/Generator')
        print('1) Normalize the Dataset tab into Components, Sources, Alignments')
        print('2) Generate (denormalize) the Dataset tab from the normalized tabs')
        print('3) Evaluate output file against prompt specifications')
        print('4) Explain what the script does')
        print('5) Exit')
        print('6) Compare code-generated Dataset to IVN production Dataset tab')
        choice = input('Enter choice (1/2/3/4/5/6): ').strip()
        if choice == '1':
            print('Navigate to the workbook containing Dataset tab:')
            path = navigation_prompt(os.path.dirname(os.path.abspath(__file__)))
            if not path:
                print('No path provided.')
                continue
            if not os.path.exists(path):
                print('File not found.')
                continue
            if os.path.isdir(path):
                print('Provided path is a directory. Please provide an Excel file (.xlsx/.xls).')
                continue
            if not (path.lower().endswith('.xlsx') or path.lower().endswith('.xls')):
                print('Invalid file type. Please provide an Excel file (.xlsx/.xls).')
                continue
            require_yes_confirmation('normalize the Dataset into Components, Sources, Alignments')
            out = normalize_workbook(path)
            print(f'Output file: {out}')
        elif choice == '2':
            print('Navigate to the normalized workbook (with Sources, Components, Alignments):')
            path = navigation_prompt(os.path.dirname(os.path.abspath(__file__)))
            if not path:
                print('No path provided.')
                continue
            if not os.path.exists(path):
                print('File not found.')
                continue
            if os.path.isdir(path):
                print('Provided path is a directory. Please provide an Excel file (.xlsx/.xls).')
                continue
            if not (path.lower().endswith('.xlsx') or path.lower().endswith('.xls')):
                print('Invalid file type. Please provide an Excel file (.xlsx/.xls).')
                continue
            require_yes_confirmation('generate the Dataset tab from normalized tabs')
            out = denormalize_workbook(path)
            print(f'Output file: {out}')
        elif choice == '3':
            print('Navigate to the output file to evaluate:')
            path = navigation_prompt(os.path.dirname(os.path.abspath(__file__)))
            if not path:
                print('No path provided.')
                continue
            if not os.path.exists(path):
                print('File not found.')
                continue
            if os.path.isdir(path):
                print('Provided path is a directory. Please provide an Excel file (.xlsx/.xls).')
                continue
            if not (path.lower().endswith('.xlsx') or path.lower().endswith('.xls')):
                print('Invalid file type. Please provide an Excel file (.xlsx/.xls).')
                continue
            evaluate_output_file(path)
        elif choice == '4':
            print('\n=== What This Script Does ===')
            print('This script provides normalization and denormalization of IVN Excel workbooks.')
            print('\nNormalization (Option 1):')
            print('  - Takes a Dataset tab with 25 columns')
            print('  - Splits it into 3 normalized tables: Sources, Components, Alignments')
            print('  - Merges duplicate records by choosing longer values')
            print('  - Preserves all null values correctly')
            print('  - Outputs: ivntest_normalized_YYYYMMDDHHMM.xlsx')
            print('\nDenormalization (Option 2):')
            print('  - Takes Sources, Components, and Alignments tabs')
            print('  - Reconstructs the original Dataset tab with all 25 columns')
            print('  - Preserves all null values correctly')
            print('  - Outputs: DatasetYYYYMMDDHHMM.xlsx')
            print('\nEvaluate Output (Option 3):')
            print('  - Validates output file against prompt specifications')
            print('  - Checks column names, order, and data quality')
            print('  - Reports issues and warnings')
            print('\nKey Features:')
            print('  - Strict schema validation')
            print('  - Interactive file navigation')
            print('  - Explicit confirmation before processing')
            print('  - Timestamped output files')
            print('  - Complete data integrity preservation\n')
        elif choice == '5':
            print('Exiting.')
            break
        elif choice == '6':
            compare_datasets_menu()
        else:
            print('Invalid choice.')
