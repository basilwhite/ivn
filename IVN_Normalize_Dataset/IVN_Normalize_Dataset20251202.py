"""
INSTRUCTIONS:
You are an expert Python programmer assisting with dataset normalization and denormalization.
Follow these finalized requirements:

Interactive runtime menu:
- 1) Normalize the Dataset tab into Components, Sources, Alignments
- 2) Generate (denormalize) the Dataset tab from the normalized tabs
- 3) Exit
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
from typing import Dict, List, Tuple, Any
import math

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
    'Dependent Source',
    'Dependent Component',
    'Dependent Component Description',
    'Dependent Component URL',
    'Dependent Source Agency',
    'Dependent Component Office of Primary Interest',
    'Dependent Fetch Status',
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
DATASET_TO_NORMALIZED: Dict[str, str] = {
    'Enabling Source': 'Sources.source_name',
    'Enabling Component': 'Components.component_name',
    'Enabling Component Description': 'Components.component_description',
    'Enabling Component URL': 'Components.component_url',
    'Enabling Source Agency': 'Sources.source_agency',
    'Enabling Component Office of Primary Interest': 'Components.component_ofc_of_primary_interest',
    'Enabling Fetch Status': 'Components.fetch_status',
    'Dependent Source': 'Sources.source_name',
    'Dependent Component': 'Components.component_name',
    'Dependent Component Description': 'Components.component_description',
    'Dependent Component URL': 'Components.component_url',
    'Dependent Source Agency': 'Sources.source_agency',
    'Dependent Component Office of Primary Interest': 'Components.component_ofc_of_primary_interest',
    'Dependent Fetch Status': 'Components.fetch_status',
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
    'dependent_component_id',
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
    missing: List[str] = [c for c in DATASET_COLUMNS_EXPECTED if c not in df.columns]
    if missing:
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
            'dependent_component_id': de_id,
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
        row: Dict[str, Any] = {
            'Enabling Source': None if not en_source or is_null(en_source.get('source_name')) else en_source.get('source_name'),  # type: ignore
            'Enabling Source Agency': None if not en_source or is_null(en_source.get('source_agency')) else en_source.get('source_agency'),  # type: ignore
            'Enabling Fetch Status': None if not en_comp or is_null(en_comp.get('fetch_status')) else en_comp.get('fetch_status'),  # type: ignore
            'Enabling Component': None if not en_comp or is_null(en_comp.get('component_name')) else en_comp.get('component_name'),  # type: ignore
            'Enabling Component Description': None if not en_comp or is_null(en_comp.get('component_description')) else en_comp.get('component_description'),  # type: ignore
            'Enabling Component URL': None if not en_comp or is_null(en_comp.get('component_url')) else en_comp.get('component_url'),  # type: ignore
            'Enabling Component Office of Primary Interest': None if not en_comp or is_null(en_comp.get('component_ofc_of_primary_interest')) else en_comp.get('component_ofc_of_primary_interest'),  # type: ignore
            'Dependent Source': None if not de_source or is_null(de_source.get('source_name')) else de_source.get('source_name'),  # type: ignore
            'Dependent Source Agency': None if not de_source or is_null(de_source.get('source_agency')) else de_source.get('source_agency'),  # type: ignore
            'Dependent Fetch Status': None if not de_comp or is_null(de_comp.get('fetch_status')) else de_comp.get('fetch_status'),  # type: ignore
            'Dependent Component': None if not de_comp or is_null(de_comp.get('component_name')) else de_comp.get('component_name'),  # type: ignore
            'Dependent Component Description': None if not de_comp or is_null(de_comp.get('component_description')) else de_comp.get('component_description'),  # type: ignore
            'Dependent Component URL': None if not de_comp or is_null(de_comp.get('component_url')) else de_comp.get('component_url'),  # type: ignore
            'Dependent Component Office of Primary Interest': None if not de_comp or is_null(de_comp.get('component_ofc_of_primary_interest')) else de_comp.get('component_ofc_of_primary_interest'),  # type: ignore
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

def interactive_menu() -> None:
    print('IVN Dataset Normalizer/Generator')
    print('1) Normalize the Dataset tab into Components, Sources, Alignments')
    print('2) Generate (denormalize) the Dataset tab from the normalized tabs')
    print('3) Exit')
    choice = input('Enter choice (1/2/3): ').strip()
    if choice == '1':
        print('Navigate to the workbook containing Dataset tab:')
        path = navigation_prompt(os.path.dirname(os.path.abspath(__file__)))
        if not path:
            print('No path provided.')
            return
        if not os.path.exists(path):
            print('File not found.')
            return
        if os.path.isdir(path):
            print('Provided path is a directory. Please provide an Excel file (.xlsx/.xls).')
            return
        if not (path.lower().endswith('.xlsx') or path.lower().endswith('.xls')):
            print('Invalid file type. Please provide an Excel file (.xlsx/.xls).')
            return
        require_yes_confirmation('normalize the Dataset into Components, Sources, Alignments')
        out = normalize_workbook(path)
        print(f'Output file: {out}')
    elif choice == '2':
        print('Navigate to the normalized workbook (with Sources, Components, Alignments):')
        path = navigation_prompt(os.path.dirname(os.path.abspath(__file__)))
        if not path:
            print('No path provided.')
            return
        if not os.path.exists(path):
            print('File not found.')
            return
        if os.path.isdir(path):
            print('Provided path is a directory. Please provide an Excel file (.xlsx/.xls).')
            return
        if not (path.lower().endswith('.xlsx') or path.lower().endswith('.xls')):
            print('Invalid file type. Please provide an Excel file (.xlsx/.xls).')
            return
        require_yes_confirmation('generate the Dataset tab from normalized tabs')
        out = denormalize_workbook(path)
        print(f'Output file: {out}')
    elif choice == '3':
        print('Exiting.')
    else:
        print('Invalid choice.')

if __name__ == '__main__':
    interactive_menu()

