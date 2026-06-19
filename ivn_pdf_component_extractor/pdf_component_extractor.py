# pdf_component_extractor.py is a command-line tool for extracting structured components from PDF documents and saving them as a TSV file. It is designed to run on Android (or other platforms) using pure Python.

#!/usr/bin/env python3
# PDF Component Extractor (pdf_component_extractor.py)
# Pure Python solution for Android with text-based file browser
# Usage: python pdf_component_extractor.py

import re
import os
import sys
from pathlib import Path
import fitz  # PyMuPDF
from datetime import datetime
import openpyxl
import hashlib
import tkinter as tk
from tkinter import filedialog
import joblib

MAX_FILENAME_BASE_LEN = 120  # to avoid Windows MAX_PATH issues


def clear_screen():
    """Clear terminal screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def get_pdf_title(pdf_path):
    doc = fitz.open(pdf_path)
    title = doc.metadata.get("title", Path(pdf_path).stem)
    return title.strip()

def normalize_text(text):
    """Clean and normalize text content with all required replacements and enhancements"""
    replacements = [
        (r'—', '--'),
        (r'‘', "'"),
        (r'’', "'"),
        (r'â€™', "'"),
        (r'“', '"'),
        (r'”', '"'),
        (r'\t', ' '),
        (r'•\s*', '- '),
        (r'\s*-\s+', '-'),
    ]
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text)
    text = re.sub(r'\n+', '\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def extract_components(pdf_path: Path, page_header_pattern: str | None = None):
    """Extract structured components from a PDF using generalized heading rules and atomic splitting."""
    doc = fitz.open(pdf_path)

    # Build full text per page and keep line-level info for regex-based detection
    pages_text: list[str] = []
    for i in range(doc.page_count):
        page_text = doc.load_page(i).get_text("text") or ""
        page_text = page_text.replace("\t", " ")
        pages_text.append(page_text)

    full_text = "\n".join(pages_text)

    # Regex-based heading detection in plain text
    heading_patterns = [
        r"^(FC\s+\d+\.\d+.*)$",                 # FC 1.000 Scope
        r"^(Part\s+\d+\b.*)$",                   # Part 5 – Publicizing ...
        r"^(\d+\.\s+.+)$",                       # 3. Citation System
        r"^([A-Z][A-Z\s,&\-]{8,})$",              # ALL-CAPS headings, min length 8
        r"^(Appendix\s+[A-Z0-9]+.*)$",              # Appendix sections
        r"^(Immediate Agency Actions.*)$",           # Initiative phrase headings
        r"^(Federal HR 2.0.*)$",                    # Initiative phrase headings
        r"^(Core HCM.*)$",                          # Initiative phrase headings
        r"^(Advisory Board.*)$",                    # Initiative phrase headings
        r"^(Transition.*)$",                        # Initiative phrase headings
        r"^(Platform.*)$",                          # Initiative phrase headings
        r"^(Initiative.*)$",                        # Initiative phrase headings
        r"^(Agency Actions.*)$",                    # Initiative phrase headings
        r"^(Agency Transitions.*)$",                # Initiative phrase headings
    ]
    combined_heading_regex = re.compile("|".join(f"({p})" for p in heading_patterns), re.MULTILINE)

    matches = list(combined_heading_regex.finditer(full_text))
    components: list[dict[str, str]] = []

    # Load atomic extraction algorithm
    extractor = joblib.load(str(Path(__file__).parent / 'component_extraction_algorithm.joblib'))

    for idx, m in enumerate(matches):
        # Find which group matched and get its text
        heading_line = next(g for g in m.groups() if g)
        start = m.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(full_text)
        raw_content = full_text[start:end].strip()

        if page_header_pattern:
            # Remove lines that match the repeating page header pattern
            ph_re = re.compile(page_header_pattern)
            filtered_lines = []
            for line in raw_content.splitlines():
                if ph_re.search(line.strip()):
                    continue
                filtered_lines.append(line)
            raw_content = "\n".join(filtered_lines)

        cleaned = normalize_text(raw_content)

        # Use atomic extractor to split cleaned content into atomic components
        atomic_contents = extractor.extract(cleaned)
        for atomic in atomic_contents:
            components.append({"heading": heading_line.strip(), "content": atomic})

    return components

def safe_filename(stem: str, suffix: str, timestamp: str) -> str:
    stem_clean = "".join(c if c in "-_.()[]{}abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" else "_" for c in stem)
    if len(stem_clean) > MAX_FILENAME_BASE_LEN:
        h = hashlib.sha1(stem_clean.encode("utf-8")).hexdigest()[:10]
        stem_clean = f"{stem_clean[:60]}_{h}_{stem_clean[-30:]}"
    return f"{stem_clean}_{timestamp}{suffix}"

def main():
    clear_screen()
    print("\n📄 PDF Component Extractor")
    print("=" * 50)
    print("This tool extracts sections from PDFs and saves them as XLSX")

    # --- PDF selection via GUI dialog ---
    root = tk.Tk()
    root.withdraw()
    pdf_file = filedialog.askopenfilename(
        title="Select PDF file",
        filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
    )
    root.destroy()

    if not pdf_file:
        print("\nNo file selected. Exiting.")
        input("Press Enter to exit...")
        return

    pdf_path = Path(pdf_file)
    print(f"\nSelected PDF: {pdf_path}")

    # --- Build output path (same directory as this script) ---
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    suggested_name = safe_filename(pdf_path.stem, ".xlsx", timestamp)
    script_dir = Path(__file__).resolve().parent
    output_xlsx = script_dir / suggested_name

    print(f"\nOutput file will be saved as:\n{output_xlsx}")

    # Get source URL (optional)
    url = input("\nEnter source URL (press Enter to skip): ").strip()

    # Get optional page-header pattern for cleaning (generalizable across PDFs)
    header_pattern = input(
        "\nIf the PDF has a repeating page header you want removed from descriptions, "
        "enter a regex pattern that matches that header (or press Enter to skip): "
    ).strip() or None

    # Prompt for Office of Primary Interest (OPI) for all components
    opi = input(
        "\nIf there is an Office of Primary Interest (OPI) for every component in this source, "
        "enter that OPI here, or press Enter to leave the OPI blank and populate each component manually: "
    ).strip()

    try:
        print("\nExtracting structured sections from PDF...")
        # Determine source name from PDF metadata, with a manual override option
        default_source_name = get_pdf_title(pdf_path)
        override_source = input(
            f"\nDetected source title is: '{default_source_name}'.\n"
            "Press Enter to accept, or type a different Source value to use: "
        ).strip()
        source_name = override_source or default_source_name

        sections = extract_components(pdf_path, header_pattern)
        print(f"Found {len(sections)} components")
        print(f"Saving to: {output_xlsx}")

        output_xlsx.parent.mkdir(parents=True, exist_ok=True)

        wb = openpyxl.Workbook()
        safe_title = "".join(c if c not in r"[]:*?/\'\"" else "_" for c in source_name)[:31]
        ws = wb.active
        ws.title = safe_title
        headers = [
            "Source",
            "Component Name",
            "Component Description",
            "Component URL",
            "Component Office of Primary Interest",
        ]
        ws.append(headers)

        for section in sections:
            heading = section["heading"]
            content = section["content"]
            component_name = f"{source_name}: {heading}" if heading else source_name
            ws.append([
                source_name,
                component_name,
                content,
                url,
                opi,
            ])

        try:
            wb.save(output_xlsx)
        except FileNotFoundError:
            print("Path may be too long. Retrying with shortened name...")
            short_name = safe_filename("output", ".xlsx", timestamp)
            fallback_path = output_xlsx.parent / short_name
            wb.save(fallback_path)
            output_xlsx = fallback_path
        except OSError as e:
            print(f"Initial save failed: {e}")
            short_name = f"out_{timestamp}.xlsx"
            fallback_path = output_xlsx.parent / short_name
            wb.save(fallback_path)
            output_xlsx = fallback_path

        print("\n✅ Processing completed successfully!")
        print(f"Output saved to: {output_xlsx}")
        input("\nPress Enter to exit...")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        input("Press Enter to exit...")
        sys.exit(1)

if __name__ == "__main__":
    main()
