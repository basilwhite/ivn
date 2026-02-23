# COMPLETE WORKING VERSION - IVN .gov Domain Scraper
# Version 1.7 - Extracts ACTUAL DOCUMENT TEXT CONTENT for descriptions
import os
import sys
import time
import json
import uuid
import argparse
import threading
import requests
import random
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, parse_qs
from datetime import datetime
import re
from io import BytesIO

# Optional PDF text extraction
try:
    from pypdf import PdfReader
    PDF_SUPPORT = True
except ImportError:
    try:
        from PyPDF2 import PdfReader
        PDF_SUPPORT = True
    except ImportError:
        PDF_SUPPORT = False
        print("[WARN] pypdf/PyPDF2 not installed - PDF text extraction disabled")

# ---- CONSTANTS ---------------------------------------------------------
INPUT_FILE = "ivntest.xlsx"
OUTPUT_FILE = "ivn_discovered_alignments.xlsx"
LOG_FILE = "ivn_scraper_log.json"
VALID_EXTENSIONS = [".pdf", ".html", ".xml", ".txt", ".docx", ".json", ".htm", ".doc"]
REQUEST_TIMEOUT = 30
MAX_RETRIES = 2
RETRY_DELAY = 2  # seconds

# ---- GLOBAL STATE ------------------------------------------------------
pause_event = threading.Event()
processed_urls = 0
total_urls = 0
start_time = None
domains_to_skip = set()
domains_to_scrape = set()
failed_domains = set()  # Track domains that consistently fail

def _is_debugging():
    """Return True if running under a debugger."""
    return sys.gettrace() is not None

def _debug_pause(reason=""):
    """Pause when debugging so terminal stays open."""
    if _is_debugging():
        try:
            print(f"\n[DEBUG PAUSE] {reason}")
            input("Press Enter to exit...")
        except (EOFError, KeyboardInterrupt):
            pass

def beep_alert():
    """Play beep to alert user - DISABLED."""
    pass  # Beep disabled per user request

def load_or_create_log(log_path, auto_choice="ask"):
    """Load existing log or create new."""
    if auto_choice == "ask" and _is_debugging():
        auto_choice = "yes"  # Auto-load when debugging
    
    if os.path.exists(log_path):
        if auto_choice == "yes":
            with open(log_path, 'r', encoding='utf-8') as f:
                log_data = json.load(f)
            print(f"[Log] Auto-loaded (start={log_data.get('start_time', 'unknown')})")
            return log_data
        elif auto_choice == "no":
            print("[Log] Starting fresh (ignoring existing log)")
        else:
            beep_alert()
            choice = input(f"\nLoad existing log '{log_path}'? (Y/N): ").strip().upper()
            if choice == 'Y':
                with open(log_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
    
    return {
        "run_id": str(uuid.uuid4()),
        "start_time": datetime.now().isoformat(),
        "domains": {},
        "visited_urls": [],
        "statistics": {"urls_scraped": 0, "docs_found": 0, "errors": 0}
    }

def save_log(log_data, log_path):
    """Save log to file."""
    with open(log_path, 'w', encoding='utf-8') as f:
        json.dump(log_data, f, indent=2)
    print(f"[Log saved: {log_path}]")

def load_excel(file_path):
    """Load Excel file."""
    if not os.path.exists(file_path):
        print(f"ERROR: File not found: {file_path}")
        return {}
    try:
        return pd.read_excel(file_path, sheet_name=None, engine='openpyxl')
    except Exception as e:
        print(f"ERROR loading Excel: {e}")
        return {}

def is_document_url(url):
    """Check if URL points to a document (case-insensitive, handles query strings)."""
    # Parse URL and get path without query string
    parsed = urlparse(url.lower())
    path = parsed.path
    
    # Check if path ends with document extension
    for ext in VALID_EXTENSIONS:
        if path.endswith(ext):
            return True
    
    # Also check query string for file parameters
    query = parse_qs(parsed.query)
    for key in ['file', 'document', 'download', 'attachment']:
        if key in query:
            val = str(query[key][0]).lower()
            if any(val.endswith(ext) for ext in VALID_EXTENSIONS):
                return True
    
    return False

def extract_document_info(link_tag, doc_url, page_soup, source_url):
    """
    Extract document name and description from link context and page content.
    Uses multiple strategies to get meaningful content like Components.xlsx has.
    """
    domain = urlparse(doc_url).netloc
    doc_path = urlparse(doc_url).path
    filename = os.path.basename(doc_path) if doc_path else ""
    
    # Strategy 1: Get link text (often the document title)
    link_text = ""
    if link_tag:
        link_text = link_tag.get_text(strip=True)
        # Also check for title attribute
        if not link_text:
            link_text = link_tag.get('title', '')
    
    # Strategy 2: Look for surrounding context (parent elements often have descriptions)
    description = ""
    if link_tag:
        # Check parent elements for descriptive text
        parent = link_tag.parent
        for _ in range(3):  # Check up to 3 levels up
            if parent:
                # Look for nearby paragraph or description
                desc_elem = parent.find(['p', 'dd', 'span', 'div'], class_=re.compile(r'desc|summary|abstract', re.I))
                if desc_elem:
                    description = desc_elem.get_text(strip=True)[:500]
                    break
                # Check siblings
                sibling = link_tag.find_next_sibling(['p', 'span', 'div'])
                if sibling:
                    text = sibling.get_text(strip=True)
                    if len(text) > 20 and len(text) < 1000:
                        description = text[:500]
                        break
                parent = parent.parent
    
    # Strategy 3: For the source page, get meta description
    if not description and page_soup:
        meta_desc = page_soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            description = meta_desc.get('content', '')[:500]
    
    # Strategy 4: Clean up filename as fallback name
    if not link_text:
        # Convert filename to readable name
        name_from_file = os.path.splitext(filename)[0]
        name_from_file = re.sub(r'[-_]+', ' ', name_from_file)
        name_from_file = re.sub(r'([a-z])([A-Z])', r'\1 \2', name_from_file)  # CamelCase
        link_text = name_from_file.strip().title()
    
    # Strategy 5: Generate description from URL path if still empty
    if not description:
        # Create description from URL structure
        path_parts = [p for p in doc_path.split('/') if p and p != filename]
        if path_parts:
            description = f"Document from {domain}: {' > '.join(path_parts[-3:])}"
        else:
            description = f"Document hosted on {domain}"
      return {
        'name': link_text[:200] if link_text else filename,
        'description': description[:1000] if description else f"Document from {domain}",
        'filename': filename
    }

def fetch_document_content(doc_url, session, headers, timeout=15):
    """
    Fetch the ACTUAL TEXT CONTENT from a document URL.
    This extracts real document text like Components.xlsx has.
    
    For HTML: Extracts main body text, removing nav/footer/scripts
    For PDF: Extracts text from first few pages
    For XML: Extracts text content
    
    Returns tuple: (title, content_text) where content_text is actual document text
    """
    title = ""
    content = ""
    
    try:
        parsed = urlparse(doc_url)
        path_lower = parsed.path.lower()
        
        # Determine document type
        is_pdf = path_lower.endswith('.pdf')
        is_html = path_lower.endswith(('.html', '.htm')) or not any(path_lower.endswith(ext) for ext in ['.pdf', '.xml', '.json', '.txt', '.docx'])
        is_xml = path_lower.endswith('.xml')
        
        response = session.get(doc_url, timeout=timeout, headers=headers, stream=is_pdf)
        response.raise_for_status()
        
        if is_pdf and PDF_SUPPORT:
            # Extract text from PDF
            try:
                pdf_bytes = BytesIO(response.content)
                reader = PdfReader(pdf_bytes)
                
                # Get title from PDF metadata
                if reader.metadata:
                    title = reader.metadata.get('/Title', '') or ''
                    if isinstance(title, bytes):
                        title = title.decode('utf-8', errors='ignore')
                
                # Extract text from first 3 pages (usually contains abstract/intro)
                text_parts = []
                for i, page in enumerate(reader.pages[:3]):
                    try:
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(page_text)
                    except:
                        pass
                
                content = ' '.join(text_parts)
                # Clean up PDF text
                content = re.sub(r'\s+', ' ', content)
                content = content.strip()
                
            except Exception as e:
                print(f"    [!] PDF extraction failed: {str(e)[:50]}")
        
        elif is_html or response.headers.get('content-type', '').startswith('text/html'):
            # Extract text from HTML - focus on main content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Get title
            if soup.title:
                title = soup.title.get_text(strip=True)
            
            # Remove non-content elements
            for tag in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 
                                       'aside', 'form', 'noscript', 'iframe']):
                tag.decompose()
            
            # Try to find main content area
            main_content = None
            for selector in ['main', 'article', '[role="main"]', '.content', '#content', 
                           '.main-content', '#main-content', '.post-content', '.entry-content']:
                main_content = soup.select_one(selector)
                if main_content:
                    break
            
            if main_content:
                content = main_content.get_text(separator=' ', strip=True)
            else:
                # Fall back to body
                body = soup.find('body')
                if body:
                    content = body.get_text(separator=' ', strip=True)
            
            # Clean up HTML text
            content = re.sub(r'\s+', ' ', content)
            content = content.strip()
        
        elif is_xml:
            # Extract text from XML
            soup = BeautifulSoup(response.text, 'xml')
            content = soup.get_text(separator=' ', strip=True)
            content = re.sub(r'\s+', ' ', content)
        
        else:
            # Plain text or unknown - just get text
            content = response.text[:5000]
            content = re.sub(r'\s+', ' ', content)
        
    except requests.Timeout:
        print(f"    [!] Timeout fetching content from {doc_url[:50]}")
    except requests.RequestException as e:
        print(f"    [!] Error fetching content: {str(e)[:50]}")
    except Exception as e:
        print(f"    [!] Content extraction error: {str(e)[:50]}")
    
    # Ensure we have something
    if not title:
        # Use filename as title fallback
        filename = os.path.basename(parsed.path)
        title = os.path.splitext(filename)[0].replace('-', ' ').replace('_', ' ').title()
    
    # Truncate content to reasonable length (like Components.xlsx)
    if content:
        # Take first 800 chars of actual content
        content = content[:800]
        # Try to end at a sentence boundary
        last_period = content.rfind('.')
        if last_period > 400:
            content = content[:last_period + 1]
    
    return title, content

# Pool of realistic User-Agents to rotate
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

# Create a persistent session for cookies and connection pooling
_session = None

def get_session():
    """Get or create a persistent requests session with realistic browser behavior."""
    global _session
    if _session is None:
        _session = requests.Session()
        # Set default headers that persist across requests
        _session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        })
    return _session

def get_random_headers(referer=None):
    """Generate realistic browser headers with rotation."""
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin' if referer else 'none',
        'Sec-Fetch-User': '?1',
        'Sec-CH-UA': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'Sec-CH-UA-Mobile': '?0',
        'Sec-CH-UA-Platform': '"Windows"',
    }
    if referer:
        headers['Referer'] = referer
    return headers

def scrape_domain(url, visited, log_data, timeout=REQUEST_TIMEOUT):
    """Scrape a .gov domain for document links with advanced anti-bot bypass. Returns list of document dicts with metadata."""
    domain = urlparse(url).netloc
    documents = []
    session = get_session()
    
    # Mark this URL as visited BEFORE attempting (to avoid retrying failed URLs)
    visited.add(url)
    
    # Retry logic
    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            if attempt > 0:
                print(f"  [RETRY {attempt}] Waiting {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            
            # Generate fresh headers for each attempt (rotate User-Agent on retries)
            headers = get_random_headers(referer=None)
            
            # Add random delay to appear more human-like (0.5-2 seconds)
            time.sleep(random.uniform(0.5, 2.0))
            
            print(f"\n[SCRAPING] {url}")
            
            # Use session for connection pooling and cookie persistence
            response = session.get(url, timeout=timeout, allow_redirects=True, headers=headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Get page title for context
            page_title = ""
            if soup.title:
                page_title = soup.title.get_text(strip=True)
            
            links = soup.find_all("a", href=True)
            
            for link in links:
                href = link["href"].strip()
                if not href or href.startswith('#') or href.startswith('javascript:'):
                    continue
                    
                full_url = urljoin(url, href)
                
                # Normalize URL (remove fragments)
                parsed = urlparse(full_url)
                normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                if parsed.query:
                    normalized_url += f"?{parsed.query}"
                if normalized_url not in visited and is_document_url(normalized_url):
                    visited.add(normalized_url)
                    
                    # Extract basic info from link context
                    doc_info = extract_document_info(link, normalized_url, soup, url)
                    
                    # NOW FETCH ACTUAL DOCUMENT CONTENT
                    # This is what makes descriptions unique like Components.xlsx
                    print(f"    [FETCHING] {normalized_url[:60]}...")
                    doc_title, doc_content = fetch_document_content(
                        normalized_url, session, headers, timeout=15
                    )
                    
                    # Use fetched content if available, otherwise use link context
                    final_name = doc_title if doc_title else doc_info['name']
                    final_description = doc_content if doc_content else doc_info['description']
                    
                    documents.append({
                        'url': normalized_url,
                        'name': final_name,
                        'description': final_description,
                        'filename': doc_info['filename']
                    })
                    
                    content_preview = final_description[:80] + "..." if len(final_description) > 80 else final_description
                    print(f"  [+] {final_name[:40]} | Content: {content_preview}")
            
            print(f"  Summary: {len(documents)} documents found")
            
            if domain in log_data['domains']:
                log_data['domains'][domain]['last_scraped'] = datetime.now().isoformat()
                log_data['domains'][domain]['docs_found'] = log_data['domains'][domain].get('docs_found', 0) + len(documents)
            
            # Success - break retry loop
            break
            
        except requests.Timeout:
            last_error = f"Timeout ({timeout}s)"
            if attempt == MAX_RETRIES:
                print(f"  [X] {last_error} - giving up after {MAX_RETRIES + 1} attempts")
                log_data['statistics']['errors'] += 1
                failed_domains.add(domain)
        except requests.RequestException as e:
            last_error = str(e)[:100]
            if attempt == MAX_RETRIES:
                print(f"  [X] Error: {last_error}")
                log_data['statistics']['errors'] += 1
        except Exception as e:
            print(f"  [X] Unexpected error: {str(e)[:100]}")
            log_data['statistics']['errors'] += 1
            break  # Don't retry unexpected errors
    
    return documents

def monitor_keyboard():
    """Monitor for Enter key to pause."""
    global pause_event
    if sys.platform == 'win32':
        import msvcrt
        while True:
            if msvcrt.kbhit():
                key = msvcrt.getch()
                if key in (b'\r', b'\n'):
                    pause_event.set()
                    print("\n[!] PAUSE REQUESTED - will pause after current URL")
            time.sleep(0.1)

def handle_pause(log_data, all_docs):
    """Handle pause menu."""
    beep_alert()
    print("\n" + "="*70)
    print("SCRIPT PAUSED")
    print(f"Documents collected: {len(all_docs)}")
    print(f"URLs processed: {processed_urls}/{total_urls}")
    print("="*70)
    
    choice = input("\n(C)ontinue, (S)top & save, (B)reak? ").strip().upper()
    
    if choice == 'S':
        return 'save'
    elif choice == 'B':
        return 'break'
    else:
        pause_event.clear()
        return 'continue'

def parse_args():
    p = argparse.ArgumentParser(description="IVN .gov scraper")
    p.add_argument("--auto-load-log", choices=["yes", "no", "ask"], default="ask")
    p.add_argument("--unattended", action="store_true")
    p.add_argument("--no-debug-pause", action="store_true")
    p.add_argument("--stay-open", action="store_true")
    return p.parse_args()

def main():
    global processed_urls, total_urls, start_time, domains_to_skip, domains_to_scrape
    
    args = parse_args()
    
    print("="*70)
    print("IVN .GOV DOMAIN SCRAPER v1.6")
    print("="*70)
    print(f"Working dir: {os.getcwd()}")
    print(f"Input: {INPUT_FILE}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Log: {LOG_FILE}")
    print(f"Debug mode: {_is_debugging()}")
    print(f"Timeout: {REQUEST_TIMEOUT}s, Max retries: {MAX_RETRIES}")
    print("="*70)
    
    # Load log
    log_data = load_or_create_log(LOG_FILE, auto_choice=args.auto_load_log)
    
    # Mode selection
    unattended_mode = args.unattended
    if unattended_mode:
        print("\n[OK] UNATTENDED MODE (no prompts)")
    else:
        print("\n[OK] INTERACTIVE MODE")
    
    # Start keyboard monitor
    kb_thread = threading.Thread(target=monitor_keyboard, daemon=True)
    kb_thread.start()
    print("[INFO] Press Enter to pause\n")
    
    # Load Excel
    print("Loading Excel...")
    data = load_excel(INPUT_FILE)
    if not data:
        if not args.no_debug_pause:
            _debug_pause("Failed to load Excel")
        return
    
    components_df = data.get('Components', pd.DataFrame())
    print(f"Loaded {len(components_df)} components")
    
    if components_df.empty:
        if not args.no_debug_pause:
            _debug_pause("No Components sheet found")
        return
    
    # Find URL column and component identifier columns
    url_column = None
    for col in ['component_url', 'URL', 'url', 'Link']:
        if col in components_df.columns:
            url_column = col
            print(f"Found URL column: {url_column}")
            break
    
    # Find component name/ID column
    component_col = None
    for col in ['component_name', 'component_id', 'name', 'id', 'Name', 'ID', 'Component']:
        if col in components_df.columns:
            component_col = col
            print(f"Found component column: {component_col}")
            break
    
    if not url_column:
        print(f"ERROR: No URL column found. Available: {list(components_df.columns)}")
        if not args.no_debug_pause:
            _debug_pause("No URL column")
        return
    
    # Filter .gov URLs
    valid_df = components_df[components_df[url_column].notna()].copy()
    gov_mask = valid_df[url_column].astype(str).str.contains(r'\.gov', case=False, na=False)
    valid_df = valid_df[gov_mask]
    
    print(f"Found {len(valid_df)} .gov URLs\n")
    
    if valid_df.empty:
        if not args.no_debug_pause:
            _debug_pause("No .gov URLs found")
        return
    
    # Build URL to component mapping
    url_to_component = {}
    if component_col:
        for _, row in valid_df.iterrows():
            url = str(row[url_column])
            comp = str(row[component_col]) if pd.notna(row[component_col]) else "Unknown"
            if url not in url_to_component:
                url_to_component[url] = comp
    
    # Process URLs - deduplicate input list
    url_list_raw = valid_df[url_column].astype(str).tolist()
    url_list = list(dict.fromkeys(url_list_raw))  # Preserve order, remove duplicates
    print(f"Deduplicated: {len(url_list_raw)} -> {len(url_list)} unique URLs")
    
    # Filter out direct document URLs (PDFs, etc.) - only scrape web pages
    doc_extensions = ['.pdf', '.docx', '.xlsx', '.pptx', '.doc', '.xls', '.ppt', '.zip', '.csv']
    scrapeable_urls = [u for u in url_list if not any(u.lower().endswith(ext) for ext in doc_extensions)]
    skipped_docs = len(url_list) - len(scrapeable_urls)
    if skipped_docs > 0:
        print(f"[INFO] Skipping {skipped_docs} direct document URLs (PDFs, etc.)")
    url_list = scrapeable_urls
    print(f"URLs to scrape: {len(url_list)}\n")
    
    total_urls = len(url_list)
    processed_urls = 0
    start_time = time.time()
    
    visited_urls = set(log_data.get('visited_urls', []))
    
    # Also skip URLs already in visited_urls from previous runs
    urls_to_process = [u for u in url_list if u not in visited_urls]
    skipped_from_log = len(url_list) - len(urls_to_process)
    if skipped_from_log > 0:
        print(f"[INFO] Skipping {skipped_from_log} URLs already visited in previous runs")
    url_list = urls_to_process
    total_urls = len(url_list)
    all_documents = []
    
    # Track statistics by domain
    domain_stats = {}
    
    for idx, url in enumerate(url_list, 1):
        # Check pause
        if pause_event.is_set():
            action = handle_pause(log_data, all_documents)
            if action == 'save':
                break
            elif action == 'break':
                return
        
        # Progress indicator
        pct = (idx / total_urls) * 100 if total_urls > 0 else 0
        elapsed = time.time() - start_time
        rate = idx / elapsed if elapsed > 0 else 0
        remaining = (total_urls - idx) / rate if rate > 0 else 0
        print(f"\n[{idx}/{total_urls}] {pct:.1f}% | ~{remaining/60:.1f}m remaining", end="")
        
        # Process URL
        domain = urlparse(url).netloc
        
        # Check if we already decided to skip this domain
        if domain in domains_to_skip:
            processed_urls += 1
            print(f" [SKIP] domain {domain} previously declined")
            continue
        
        # Check if domain has failed too many times
        if domain in failed_domains:
            processed_urls += 1
            print(f" [SKIP] domain {domain} has connection issues")
            continue
        
        # Check if should scrape (in unattended mode, always scrape; if already approved, scrape)
        if not unattended_mode and domain not in domains_to_scrape and domain in log_data.get('domains', {}):
            last = log_data['domains'][domain].get('last_scraped', 'unknown')
            beep_alert()
            choice = input(f"\n{domain} last scraped {last}. Scrape again? (Y/N): ").strip().upper()
            if choice == 'Y':
                domains_to_scrape.add(domain)
            else:
                domains_to_skip.add(domain)
                processed_urls += 1
                print(f"[SKIP] Skipping all URLs from {domain}")
                continue
        
        # Initialize domain in log
        if domain not in log_data['domains']:
            log_data['domains'][domain] = {
                'last_scraped': datetime.now().isoformat(),
                'docs_found': 0
            }
        
        # Track domain stats
        if domain not in domain_stats:
            domain_stats[domain] = {'urls': 0, 'docs': 0}
        domain_stats[domain]['urls'] += 1
        
        # Scrape - now returns document dicts with metadata
        docs = scrape_domain(url, visited_urls, log_data)
        component_name = url_to_component.get(url, "Unknown")
        
        # Generate output in Components.xlsx format with REAL extracted content
        for doc in docs:
            all_documents.append({
                "component_name": doc['name'],
                "component_description": doc['description'],
                "component_url": doc['url'],
                "component_ofc_of_primary_interest": urlparse(doc['url']).netloc,
                "source_id": component_name,
                "component_id": f"{component_name}::{doc['name']}",
                "fetch_status": "DISCOVERED"
            })
        
        domain_stats[domain]['docs'] += len(docs)
        
        processed_urls += 1
        log_data['statistics']['urls_scraped'] = processed_urls
        
        # Save checkpoint every 10 URLs
        if processed_urls % 10 == 0:
            log_data['visited_urls'] = list(visited_urls)
            save_log(log_data, LOG_FILE)
            print(f"\n[CHECKPOINT] {processed_urls}/{total_urls} URLs, {len(all_documents)} docs\n")
    
    # Save results
    print(f"\n{'='*70}")
    print(f"COMPLETE: {len(all_documents)} documents from {processed_urls} URLs")
    print(f"{'='*70}")
    
    # Print domain summary
    if domain_stats:
        print("\nDomain Summary:")
        for dom, stats in sorted(domain_stats.items(), key=lambda x: x[1]['docs'], reverse=True)[:10]:
            print(f"  {dom}: {stats['docs']} docs from {stats['urls']} URLs")
    
    if all_documents:
        df = pd.DataFrame(all_documents)
        df.to_excel(OUTPUT_FILE, index=False)
        print(f"\n[OK] Results saved: {OUTPUT_FILE}")
        print(f"    Columns: {list(df.columns)}")
        print(f"    Rows: {len(df)}")
    else:
        print("[!] No documents collected")
    
    # Save final log
    log_data['visited_urls'] = list(visited_urls)
    log_data['end_time'] = datetime.now().isoformat()
    log_data['statistics']['docs_found'] = len(all_documents)
    save_log(log_data, LOG_FILE)
    
    # Debug pause
    if _is_debugging() and not args.no_debug_pause:
        _debug_pause("Run complete")
    
    # Stay open if requested
    if args.stay_open and _is_debugging():
        print("\n[DEBUG] --stay-open active. Press Ctrl+C to exit.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("[Exiting]")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        _debug_pause("Fatal error")
