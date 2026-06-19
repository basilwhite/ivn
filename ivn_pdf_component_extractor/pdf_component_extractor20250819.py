# pdf_component_extractor.py is a command-line tool for extracting structured components from PDF documents and saving them as a CSV file. It is designed to run on Android (or other platforms) using pure Python.

#!/usr/bin/env python3
# PDF Component Extractor (pdf_component_extractor.py)
# Pure Python solution for Android with text-based file browser
# Usage: python pdf_component_extractor.py

import re
import csv
import os
import sys
from pathlib import Path
import PyPDF2
import string
from datetime import datetime

def clear_screen():
    """Clear terminal screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def show_menu(title, options, back=True):
    """Display a menu and get user selection"""
    clear_screen()
    print(f"\n{title}")
    print("=" * 40)
    
    for i, option in enumerate(options, 1):
        print(f"{i}. {option}")
    
    if back:
        print("0. Back")
    
    return input("\nEnter your choice: ").strip()

def browse_files(start_dir):
    """Text-based file browser for directory navigation"""
    current_dir = Path(start_dir)
    
    while True:
        # Get directory contents
        dirs = []
        files = []
        
        try:
            for item in current_dir.iterdir():
                if item.is_dir():
                    dirs.append(f"{item.name}/")
                elif item.suffix.lower() == '.pdf':
                    files.append(item.name)
        except Exception as e:
            return None, f"Error accessing directory: {str(e)}"
        
        # Sort directories and files
        dirs.sort()
        files.sort()
        
        # Create menu options
        options = dirs + files
        if not options:
            options = ["No PDF files found"]
        
        # Show menu
        choice = show_menu(f"Current Directory: {current_dir}", options, current_dir != start_dir)
        
        # Handle back command
        if choice == '0' and current_dir != start_dir:
            current_dir = current_dir.parent
            continue
        elif choice == '0':
            return None, "Operation cancelled"
        
        # Handle selection
        try:
            choice_index = int(choice) - 1
            if 0 <= choice_index < len(options):
                selected = options[choice_index]
                selected_path = current_dir / selected.rstrip('/')
                
                if selected.endswith('/'):  # Directory
                    current_dir = selected_path
                else:  # File
                    return selected_path, None
            else:
                input("\nInvalid choice. Press Enter to try again...")
        except ValueError:
            input("\nPlease enter a number. Press Enter to continue...")

def extract_pdf_text(pdf_path):
    """Extract text from PDF using PyPDF2"""
    text_pages = []
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page_num in range(len(reader.pages)):
                page = reader.pages[page_num]
                page_text = page.extract_text()
                text_pages.append(page_text if page_text else "")
    except Exception as e:
        raise RuntimeError(f"Failed to extract text from PDF: {str(e)}")
    
    return text_pages

def get_pdf_title(pdf_path):
    """Extract the Title field from PDF document properties."""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            info = reader.metadata
            title = info.title if info and info.title else Path(pdf_path).stem
            return title.strip()
    except Exception:
        return Path(pdf_path).stem

def get_bookmarks(reader):
    """Flatten all bookmarks (including nested) into a list of (title, page_number) tuples."""
    bookmarks = []
    def walk(outlines):
        for item in outlines:
            if isinstance(item, list):
                walk(item)
            else:
                try:
                    title = item.title if hasattr(item, 'title') else str(item)
                    page_num = reader.get_destination_page_number(item)
                    bookmarks.append((title.strip(), page_num))
                except Exception:
                    continue
    try:
        walk(reader.outline)
    except Exception:
        try:
            walk(reader.outlines)
        except Exception:
            pass
    # Remove duplicates and sort by page number, but keep all bookmarks
    seen = set()
    unique_bookmarks = []
    for title, page_num in bookmarks:
        key = (title, page_num)
        if key not in seen:
            unique_bookmarks.append((title, page_num))
            seen.add(key)
    # Sort by page number, but preserve original order for bookmarks on the same page
    unique_bookmarks.sort(key=lambda x: (x[1], bookmarks.index(x)))
    return unique_bookmarks

def extract_sections_by_bookmarks(pdf_path):
    """Extract sections based on PDF bookmarks."""
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        text_pages = []
        for page_num in range(len(reader.pages)):
            page = reader.pages[page_num]
            page_text = page.extract_text()
            # Replace all tabs with spaces immediately after extraction
            if page_text:
                page_text = page_text.replace('\t', ' ')
            text_pages.append(page_text if page_text else "")
        bookmarks = get_bookmarks(reader)
        sections = []
        for i, (title, start_page) in enumerate(bookmarks):
            end_page = bookmarks[i+1][1] if i+1 < len(bookmarks) else len(text_pages)
            section_text = "\n".join(text_pages[start_page:end_page]).strip()
            sections.append({
                "heading": title,
                "content": section_text
            })
        return sections

def normalize_heading(heading):
    """Trim, remove control characters, and title-case the heading."""
    # Remove non-printable/control characters
    heading = ''.join(ch for ch in heading if ch in string.printable)
    heading = heading.strip()
    # Optionally, title-case headings (comment out if not wanted)
    heading = heading.title()
    return heading

def normalize_text(text):
    """Clean and normalize text content with all required replacements and enhancements"""
    # Remove non-printable/control characters
    text = ''.join(ch for ch in text if ch in string.printable or ch in '\n\r')
    
    # Remove common headers/footers/page numbers (basic patterns, adjust as needed)
    # Example: Remove lines that are just numbers (page numbers)
    text = re.sub(r'^\s*\d+\s*$', '', text, flags=re.MULTILINE)
    # Example: Remove lines that match "Page X" or "Page X of Y"
    text = re.sub(r'^\s*Page\s+\d+(\s+of\s+\d+)?\s*$', '', text, flags=re.MULTILINE)
    
    replacements = [
        (r'—', '--'),          # Em-dash to two en-dashes
        (r'‘', "'"),           # Left single smart quote
        (r'’', "'"),           # Right single smart quote
        (r'â€™', "'"),         # Fix encoding: right single quote
        (r'“', '"'),           # Left double smart quote
        (r'”', '"'),           # Right double smart quote
        (r'\t', ' '),          # Tabs to spaces
        (r'•\s*', '- '),       # Bullets to dashes
        (r'\s*-\s+', '-'),     # Fix hyphenated words
    ]
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text)
    
    # Consistent line break handling: replace multiple line breaks with a single one, then flatten to single space
    text = re.sub(r'\n+', '\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def main():
    clear_screen()
    print("\n📄 PDF Component Extractor for Android")
    print("=" * 50)
    print("This tool extracts sections from PDFs and saves them as CSV")
    print("with text normalization for compatibility.")
    
    # Start with common Android directories
    start_dir = "/sdcard"
    if not Path(start_dir).exists():
        start_dir = "/storage/emulated/0"
    if not Path(start_dir).exists():
        start_dir = os.getcwd()
    
    # Select PDF file
    pdf_path, error = browse_files(start_dir)
    if error:
        print(f"\nError: {error}")
        input("Press Enter to exit...")
        return
    
    print(f"\nSelected PDF: {pdf_path}")
    
    # Get output path
    output_tsv = pdf_path.with_suffix('.tsv')  # <-- Change .csv to .tsv
    # --- Add timestamp suffix to output filename ---
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    output_tsv = output_tsv.with_name(f"{output_tsv.stem}_{timestamp}{output_tsv.suffix}")
    # ------------------------------------------------
    output_path, error = browse_files(output_tsv.parent)
    if error:
        print(f"\nError: {error}")
        input("Press Enter to exit...")
        return
    
    # Confirm output filename
    if output_path.suffix.lower() != '.tsv':  # <-- Change .csv to .tsv
        output_tsv = output_path.with_suffix('.tsv')  # <-- Change .csv to .tsv
    else:
        output_tsv = output_path

    # --- Ensure timestamp is appended if user changes filename ---
    if not output_tsv.stem.endswith(timestamp):
        output_tsv = output_tsv.with_name(f"{output_tsv.stem}_{timestamp}{output_tsv.suffix}")
    # ------------------------------------------------------------

    # Get URL
    url = input("\nEnter source URL (press Enter to skip): ").strip()
    
    try:
        # Extract sections by bookmarks from PDF
        print(f"\nExtracting sections by bookmarks from PDF...")
        sections = extract_sections_by_bookmarks(pdf_path)
        
        # Prepare TSV output
        source_name = get_pdf_title(pdf_path)
        print(f"Found {len(sections)} components")
        print(f"Saving to: {output_tsv}")
        
        with open(output_tsv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "Source Name",
                    "Component Name",
                    "Component Description",
                    "Component URL"
                ],
                delimiter='\t'  # Use tab as delimiter
            )
            writer.writeheader()
            
            for section in sections:
                writer.writerow({
                    "Source Name": source_name,
                    "Component Name": section["heading"],
                    "Component Description": normalize_text(section["content"]),
                    "Component URL": url
                })
        
        print("\n✅ Processing completed successfully!")
        print(f"Output saved to: {output_tsv}")
        input("\nPress Enter to exit...")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        input("Press Enter to exit...")
        sys.exit(1)

if __name__ == "__main__":
    # Install PyPDF2 if missing
    try:
        import PyPDF2
    except ImportError:
        print("Installing required PyPDF2 library...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "PyPDF2"])
        import PyPDF2
        
    main()
