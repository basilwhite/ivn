#!/usr/bin/env python3
"""
IVN Transformation Engine
Extracts rows from ivntest.xlsx, sends to Transformation Engine, stages responses.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
import openpyxl
import anthropic
from jsonschema import validate, ValidationError
import time
import math
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import pdfplumber
import io

load_dotenv()

# Try to import transformers for local AI
try:
    from transformers import pipeline
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

# Try to import NLTK for extractive summarization
try:
    import nltk
    from nltk.tokenize import sent_tokenize, word_tokenize
    from nltk.corpus import stopwords
    from nltk.probability import FreqDist
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False

load_dotenv()

# Initialize local summarizer if needed
summarizer = None

def initialize_summarizer():
    global summarizer
    if summarizer is None and TRANSFORMERS_AVAILABLE:
        logger.info("Initializing local summarization model...")
        try:
            summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")
            logger.info("Local model loaded successfully.")
        except Exception as e:
            logger.warning(f"Failed to load local model: {e}. Using mock fallback.")
            summarizer = None
    elif not TRANSFORMERS_AVAILABLE:
        logger.warning("Transformers library not available, local AI will use NLTK extractive summarization fallback.")

def extractive_summarize(text, num_sentences=3):
    """Perform extractive summarization using NLTK."""
    if not NLTK_AVAILABLE:
        logger.warning("NLTK not available for summarization.")
        return text[:200] + "..."
    try:
        sentences = sent_tokenize(text)
        stop_words = set(stopwords.words('english'))
        words = word_tokenize(text.lower())
        words = [word for word in words if word.isalnum() and word not in stop_words]
        freq_dist = FreqDist(words)
        sentence_scores = {}
        for sentence in sentences:
            sentence_words = word_tokenize(sentence.lower())
            sentence_words = [word for word in sentence_words if word.isalnum() and word not in stop_words]
            score = sum(freq_dist.get(word, 0) for word in sentence_words)
            sentence_scores[sentence] = score
        top_sentences = sorted(sentence_scores, key=sentence_scores.get, reverse=True)[:num_sentences]
        summary = ' '.join(top_sentences)
        return summary
    except Exception as e:
        logger.warning(f"Extractive summarization failed: {e}")
        return text[:200] + "..."

# Configuration
DATA_DIR = Path(__file__).parent.parent / "data"
SCHEMA_DIR = Path(__file__).parent.parent / "schemas"
LOG_DIR = Path(__file__).parent.parent / "logs"
EXCEL_FILE = DATA_DIR / "ivntest.xlsx"
STAGED_FILE = DATA_DIR / "updates_staged.jsonl"
BACKUP_DIR = DATA_DIR / "backups"
TIMINGS_FILE = DATA_DIR / "operation_timings.json"

# API Configuration
API_KEY = os.getenv("ANTHROPIC_API_KEY")
MODEL = "claude-sonnet-4-20250514"

# Logging
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "ivn_transformation_engine.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load schemas
with open(SCHEMA_DIR / "request.schema.json") as f:
    REQUEST_SCHEMA = json.load(f)
with open(SCHEMA_DIR / "response.schema.json") as f:
    RESPONSE_SCHEMA = json.load(f)

# Utility to load and save operation timings
def load_timings():
    if TIMINGS_FILE.exists():
        with open(TIMINGS_FILE, "r") as f:
            return json.load(f)
    return {}

def save_timings(timings):
    with open(TIMINGS_FILE, "w") as f:
        json.dump(timings, f, indent=2)

def format_time(seconds):
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s"

# Wrap major operations for timing and progress reporting
def timed_operation(name, op_idx, total_ops, timings, func, *args, **kwargs):
    print(f"\n[Operation {op_idx}/{total_ops}] Starting: {name}")
    prev = timings.get(name, None)
    start = time.time()
    if prev:
        print(f"Estimated time: {format_time(prev['avg'])}")
    else:
        print("Estimated time: unknown")
    result = func(*args, **kwargs)
    elapsed = time.time() - start
    print(f"Completed {name} in {format_time(elapsed)}")
    # Update timings
    if name not in timings:
        timings[name] = {"runs": 0, "total": 0.0, "avg": 0.0}
    timings[name]["runs"] += 1
    timings[name]["total"] += elapsed
    timings[name]["avg"] = timings[name]["total"] / timings[name]["runs"]
    save_timings(timings)
    return result, elapsed

# Extract component_id, current_text, source_url from Excel.
def extract_rows(sheet_name="Dataset", max_rows=None):
    """
    Extract component_id, current_text, source_url from Excel.
    
    Column mapping for 'Dataset' sheet:
    - Column A (1): Enabling Source
    - Column B (2): Enabling Component (component_id)
    - Column C (3): Enabling Component Description (current_text)
    - Column I (9): Enabling Component URL (source_url)
    """
    wb = openpyxl.load_workbook(EXCEL_FILE, read_only=True)
    ws = wb[sheet_name]
    
    rows_extracted = []
    for idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        if max_rows and len(rows_extracted) >= max_rows:
            break
            
        component_id = row[1]  # Column B
        current_text = row[2]  # Column C
        source_url = row[8]    # Column I
        
        # Skip rows with missing required fields
        if not all([component_id, current_text, source_url]):
            logger.warning(f"Row {idx}: Missing required fields, skipping")
            continue
            
        # Skip non-URL source_url (e.g., single space)
        if not source_url.startswith("http"):
            logger.debug(f"Row {idx}: No valid URL, skipping")
            continue
        
        rows_extracted.append({
            "row_number": idx,
            "component_id": component_id,
            "current_text": current_text,
            "source_url": source_url
        })
    
    wb.close()
    logger.info(f"Extracted {len(rows_extracted)} rows from sheet '{sheet_name}'")
    return rows_extracted


def call_transformation_engine(request_data):
    """Send request to Transformation Engine and return validated response."""
    # Validate request
    try:
        validate(instance=request_data, schema=REQUEST_SCHEMA)
    except ValidationError as e:
        logger.error(f"Request validation failed: {e.message}")
        raise
    
    # First, try API if key is available
    if API_KEY:
        try:
            client = anthropic.Anthropic(api_key=API_KEY)
            
            system_prompt = """You are a deterministic transformation engine. You receive row data from an Excel workbook and return structured JSON updates.

You must return ONLY a JSON object with exactly these three keys:
- updated_text: transformed content based on source_url
- lineage: structured explanation of your transformation logic
- delta: description of changes from current_text to updated_text

Do not include markdown, code fences, commentary, or any text outside the JSON object."""

            user_prompt = f"""Transform this component:

component_id: {request_data['component_id']}
current_text: {request_data['current_text']}
source_url: {request_data['source_url']}

Return only the JSON object with updated_text, lineage, and delta."""

            message = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}]
            )
            
            response_text = message.content[0].text.strip()
            
            # Remove markdown code fences if present
            if response_text.startswith("```"):
                lines = response_text.split("\n")
                response_text = "\n".join(lines[1:-1])
            
            response_json = json.loads(response_text)
            
            # Validate response
            validate(instance=response_json, schema=RESPONSE_SCHEMA)
            
            return response_json
            
        except Exception as e:
            logger.warning(f"API call failed: {e}. Falling back to local AI.")
    
    # Fallback to local AI with URL fetching
    try:
        initialize_summarizer()
        
        # Fetch content from source_url
        logger.info(f"Fetching content from {request_data['source_url']}")
        response = requests.get(request_data['source_url'], timeout=10)
        response.raise_for_status()
        
        if request_data['source_url'].endswith('.pdf'):
            # Extract text from PDF
            with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                text = ''
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + ' '
        else:
            # Extract text from HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            # Extract text from paragraphs
            text = ' '.join([p.get_text() for p in soup.find_all('p')])
            if not text:
                text = soup.get_text()  # Fallback
        
        # Clean and limit text
        text = ' '.join(text.split())  # Remove extra whitespace
        text = text[:2000]  # Limit for model
        if len(text) < 50:
            raise ValueError("Insufficient content extracted from source")
        
        if summarizer:
            # Generate summary using local model
            summary_result = summarizer(text, max_length=200, min_length=50, do_sample=False)
            summary = summary_result[0]['summary_text']
            updated_text = f"Transformed content for {request_data['component_id']}: {summary}"
            lineage = f"{{\"source_authority\": \"{request_data['component_id']}\", \"method\": \"local AI model summarization of fetched source URL content\", \"original_length\": {len(request_data['current_text'])}, \"updated_length\": {len(updated_text)}, \"source_url_fetched\": true, \"model\": \"sshleifer/distilbart-cnn-12-6\"}}"
            delta = f"Replaced original text with AI-generated summary derived from source URL content. Original length: {len(request_data['current_text'])} characters. New length: {len(updated_text)} characters."
        else:
            # Extractive summarization if transformers not available
            summary = extractive_summarize(text)
            updated_text = f"Transformed content for {request_data['component_id']}: {summary}"
            lineage = f"{{\"source_authority\": \"{request_data['component_id']}\", \"method\": \"extractive summarization of fetched source URL content using NLTK\", \"original_length\": {len(request_data['current_text'])}, \"updated_length\": {len(updated_text)}, \"source_url_fetched\": true}}"
            delta = f"Replaced original text with extractive summary derived from source URL content. Original length: {len(request_data['current_text'])} characters. New length: {len(updated_text)} characters."
        
        response = {
            "updated_text": updated_text,
            "lineage": lineage,
            "delta": delta
        }
        # Validate response
        validate(instance=response, schema=RESPONSE_SCHEMA)
        return response
        
    except Exception as e:
        logger.warning(f"Local AI transformation failed: {e}. Falling back to simple mock.")
        # Fallback mock
        mock_text = f"Mock transformation of: {request_data['current_text'][:100]}... (Source URL processing failed: {str(e)})"
        mock_response = {
            "updated_text": mock_text,
            "lineage": f'{{"source_authority": "{request_data["component_id"]}", "method": "fallback mock due to source processing failure", "original_length": {len(request_data["current_text"])}, "updated_length": {len(mock_text)}}}',
            "delta": f"Fallback mock applied due to source processing error. Original length: {len(request_data['current_text'])} characters."
        }
        validate(instance=mock_response, schema=RESPONSE_SCHEMA)
        return mock_response


def stage_update(row_data, response_data):
    """Append validated update to staging file."""
    update_record = {
        "timestamp": datetime.now().isoformat(),
        "row_number": row_data["row_number"],
        "component_id": row_data["component_id"],
        "request": {
            "current_text": row_data["current_text"],
            "source_url": row_data["source_url"]
        },
        "response": response_data,
        "applied": False
    }
    
    with open(STAGED_FILE, "a") as f:
        f.write(json.dumps(update_record) + "\n")
    
    logger.info(f"Staged update for row {row_data['row_number']}")


def create_backup():
    """Create timestamped backup of Excel file."""
    BACKUP_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"ivntest_{timestamp}.xlsx"
    import shutil
    shutil.copy2(EXCEL_FILE, backup_path)
    logger.info(f"Backup created: {backup_path}")
    print(f"Output file created: {backup_path}")


def main():
    """Main orchestration loop."""
    logger.info("=== IVN Transformation Engine Started ===")
    timings = load_timings()
    op_names = [
        "Create Backup",
        "Extract Rows",
        "Process Rows"
    ]
    total_ops = len(op_names)
    completed_ops = 0
    elapsed_ops = []
    start_all = time.time()

    # 1. Create backup
    _, elapsed = timed_operation(
        op_names[0], 1, total_ops, timings, create_backup)
    completed_ops += 1
    elapsed_ops.append(elapsed)
    print(f"Progress: {completed_ops}/{total_ops} operations complete.")

    # 2. Extract rows
    rows, elapsed = timed_operation(
        op_names[1], 2, total_ops, timings, extract_rows, sheet_name="Dataset", max_rows=10)
    completed_ops += 1
    elapsed_ops.append(elapsed)
    print(f"Progress: {completed_ops}/{total_ops} operations complete.")

    # 3. Process each row (count as one operation for ETA)
    def process_all_rows(rows):
        processed = 0
        total = len(rows)
        row_times = []
        for idx, row_data in enumerate(rows, 1):
            row_start = time.time()
            print(f"\nProcessing row {idx}/{total}: {row_data['component_id']}")
            request = {
                "component_id": row_data["component_id"],
                "current_text": row_data["current_text"],
                "source_url": row_data["source_url"]
            }
            try:
                response = call_transformation_engine(request)
                stage_update(row_data, response)
            except Exception as e:
                logger.error(f"Failed to process row {row_data['row_number']}: {e}")
                continue
            processed += 1
            elapsed_row = time.time() - row_start
            row_times.append(elapsed_row)
            avg_row = sum(row_times) / len(row_times)
            remaining = total - idx
            est_remaining = avg_row * remaining
            print(f"Row {idx} complete. Elapsed: {format_time(elapsed_row)}. Remaining: {format_time(est_remaining)}.")
        return processed

    processed_count, elapsed = timed_operation(
        op_names[2], 3, total_ops, timings, process_all_rows, rows)
    completed_ops += 1
    elapsed_ops.append(elapsed)
    print(f"Progress: {completed_ops}/{total_ops} operations complete.")

    total_elapsed = time.time() - start_all
    print(f"All operations complete in {format_time(total_elapsed)}.")
    # Estimate for next run
    if completed_ops == total_ops:
        est_total = sum(timings[n]["avg"] for n in op_names if n in timings)
        print(f"Estimated time for future runs: {format_time(est_total)}.")
    
    print(f"Output file: {STAGED_FILE}")

if __name__ == "__main__":
    main()
