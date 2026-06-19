


import argparse
import re
import os
import sys
import pandas as pd
import requests
from datetime import datetime
import time
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    ZoneInfo = None

# Output file and sheet name

# ---------------------------
# Helpers
# ---------------------------

def load_excel_prefer_components(path: str) -> pd.DataFrame:
    """Load Excel; prefer 'Components' sheet if present."""
    xl = pd.ExcelFile(path, engine="openpyxl")
    sheet = "Components" if "Components" in xl.sheet_names else xl.sheet_names[0]
    return pd.read_excel(path, sheet_name=sheet, engine="openpyxl"), sheet

def validate_cols(df: pd.DataFrame):
    required = {"component_name", "component_id", "component_url"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")



def generate_url(name: str, comp_id) -> str | None:
    text_name = "" if pd.isna(name) else str(name)
    text_id   = "" if pd.isna(comp_id) else str(comp_id)
    text = f"{text_name} {text_id}".strip()

    # Synonyms/abbreviations normalization
    text = re.sub(r"\bEO\b", "Executive Order", text, flags=re.IGNORECASE)
    text = re.sub(r"\bSP\s*800\b", "NIST SP 800", text, flags=re.IGNORECASE)

    # USC
    m = re.search(r"(\d+)\s*USC\s*([0-9A-Za-z\-]+)", text, flags=re.IGNORECASE)
    if m:
        title, section = m.groups()
        section = re.sub(r"[^0-9A-Za-z\-]+", "", section)
        return f"https://uscode.house.gov/view.xhtml?req=(title:{title}%20section:{section})"

    # CFR
    m = re.search(r"(\d+)\s*CFR\s*([0-9]+(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)
    if m:
        title, part_or_section = m.groups()
        if "." in part_or_section:
            return f"https://www.ecfr.gov/current/title-{title}/section-{part_or_section}"
        return f"https://www.ecfr.gov/current/title-{title}/part-{part_or_section}"

    # FAR
    m = re.search(r"\bFAR\s*([0-9]+(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)
    if m:
        part = m.group(1).split(".")[0]
        return f"https://www.acquisition.gov/far/part-{part}"

    # DFARS
    m = re.search(r"\bDFARS\s*([0-9]+(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)
    if m:
        part = m.group(1).split(".")[0]
        return f"https://www.acquisition.gov/dfars/part-{part}"

    # Executive Orders
    m = re.search(r"\bExecutive Order\s*(\d+)\b", text, flags=re.IGNORECASE)
    if m:
        eo = m.group(1)
        return f"https://www.archives.gov/federal-register/executive-orders/{eo}.html"

    # OMB Circulars (A-XX)
    m = re.search(r"OMB\s*Circular\s*A-?(\d+)", text, flags=re.IGNORECASE)
    if m:
        return "https://www.whitehouse.gov/omb/information-for-agencies/circulars/"

    # OMB Memoranda (M-YY-XX; special case M-26-03)
    m = re.search(r"\bM-\d{2}-\d{2}\b", text, flags=re.IGNORECASE)
    if m:
        memo = m.group(0).upper()
        if memo == "M-26-03":
            return "https://www.whitehouse.gov/wp-content/uploads/2025/12/M-26-03-Presidents-Management-Agenda.pdf"
        return "https://www.whitehouse.gov/omb/information-resources/guidance/memoranda/"

    # NIST SP 800 exacts (expanded)
    sp_map = {
        "800-53":  "https://csrc.nist.gov/publications/detail/sp/800-53/rev-5/final",
        "800-171": "https://csrc.nist.gov/publications/detail/sp/800-171/rev-2/final",
        "800-37":  "https://csrc.nist.gov/publications/detail/sp/800-37/rev-2/final",
        "800-30":  "https://csrc.nist.gov/publications/detail/sp/800-30/rev-1/final",
        "800-39":  "https://csrc.nist.gov/publications/detail/sp/800-39/final",
        "800-61":  "https://csrc.nist.gov/publications/detail/sp/800-61/rev-2/final",
        "800-115": "https://csrc.nist.gov/publications/detail/sp/800-115/final",
        "800-82":  "https://csrc.nist.gov/publications/detail/sp/800-82/rev-2/final",
        "800-88":  "https://csrc.nist.gov/publications/detail/sp/800-88/rev-1/final",
        "800-34":  "https://csrc.nist.gov/publications/detail/sp/800-34/rev-1/final",
        "800-122": "https://csrc.nist.gov/publications/detail/sp/800-122/final",
        "800-207": "https://csrc.nist.gov/publications/detail/sp/800-207/final",
        "800-53A": "https://csrc.nist.gov/publications/detail/sp/800-53a/rev-5/final",
        "800-63":  "https://csrc.nist.gov/publications/detail/sp/800-63/rev-3/final",
        "800-63A": "https://csrc.nist.gov/publications/detail/sp/800-63a/rev-3/final",
        "800-63B": "https://csrc.nist.gov/publications/detail/sp/800-63b/rev-3/final",
        "800-63C": "https://csrc.nist.gov/publications/detail/sp/800-63c/rev-3/final",
        "800-218": "https://csrc.nist.gov/publications/detail/sp/800-218/final",
        "800-160": "https://csrc.nist.gov/publications/detail/sp/800-160/rev-1/final",
        "800-184": "https://csrc.nist.gov/publications/detail/sp/800-184/final",
        "800-144": "https://csrc.nist.gov/publications/detail/sp/800-144/final",
        "800-137": "https://csrc.nist.gov/publications/detail/sp/800-137/final",
        "800-55":  "https://csrc.nist.gov/publications/detail/sp/800-55/rev-1/final",
        "800-53B": "https://csrc.nist.gov/publications/detail/sp/800-53b/final",
        "800-125": "https://csrc.nist.gov/publications/detail/sp/800-125/final",
        "800-124": "https://csrc.nist.gov/publications/detail/sp/800-124/rev-2/final",
        "800-82r3": "https://csrc.nist.gov/publications/detail/sp/800-82/rev-3/final",
    }
    m = re.search(r"SP\s*800-([0-9]+[A-Za-z]?)", text, flags=re.IGNORECASE)
    if m:
        spn = f"800-{m.group(1)}"
        if spn in sp_map:
            return sp_map[spn]
        # Try revisioned forms (e.g., 800-82r3)
        spn_r = spn.replace("r", "r")
        if spn_r in sp_map:
            return sp_map[spn_r]
        return "https://csrc.nist.gov/publications/sp800"

    # FIPS 199/200
    m = re.search(r"\bFIPS\s*(199|200)\b", text, flags=re.IGNORECASE)
    if m:
        return ("https://csrc.nist.gov/publications/detail/fips/199/final"
                if m.group(1) == "199"
                else "https://csrc.nist.gov/publications/detail/fips/200/final")

    # FISMA
    if re.search(r"FISMA", text, flags=re.IGNORECASE):
        return "https://www.cisa.gov/topics/risk-management/federal-information-security-modernization-act"

    # HIPAA
    if re.search(r"HIPAA", text, flags=re.IGNORECASE):
        return "https://www.hhs.gov/hipaa/index.html"

    # FERPA
    if re.search(r"FERPA", text, flags=re.IGNORECASE):
        return "https://www2.ed.gov/policy/gen/guid/fpco/ferpa/index.html"

    # Privacy Act
    if re.search(r"Privacy Act", text, flags=re.IGNORECASE):
        return "https://www.justice.gov/opcl/privacy-act-1974"

    # Paperwork Reduction Act
    if re.search(r"Paperwork Reduction Act", text, flags=re.IGNORECASE):
        return "https://www.whitehouse.gov/omb/information-regulatory-affairs/paperwork-reduction-act/"

    # Clinger-Cohen Act
    if re.search(r"Clinger[- ]Cohen", text, flags=re.IGNORECASE):
        return "https://www.gsa.gov/policy-regulations/policy/information-integrity-and-access/clingercohen-act"

    # Federal Register
    if re.search(r"Federal Register", text, flags=re.IGNORECASE):
        return "https://www.federalregister.gov/"

    # NARA Bulletin
    m = re.search(r"NARA Bulletin\s*(\d{4}-\d{2})", text, flags=re.IGNORECASE)
    if m:
        return f"https://www.archives.gov/records-mgmt/bulletins/{m.group(1)}.html"

    # GAO Report
    m = re.search(r"GAO Report\s*(GAO-\d+-\d+)", text, flags=re.IGNORECASE)
    if m:
        return f"https://www.gao.gov/products/{m.group(1)}"

    # OMB Bulletin
    m = re.search(r"OMB Bulletin\s*(\d{4}-\d{2})", text, flags=re.IGNORECASE)
    if m:
        return f"https://www.whitehouse.gov/omb/information-for-agencies/bulletins/{m.group(1)}"

    # DHS Directive
    m = re.search(r"DHS Directive\s*(\d{4}-\d{2})", text, flags=re.IGNORECASE)
    if m:
        return f"https://www.dhs.gov/publication/dhs-directive-{m.group(1)}"

    # DoD Instruction
    m = re.search(r"DoD Instruction\s*(\d+\.\d+)", text, flags=re.IGNORECASE)
    if m:
        return f"https://www.esd.whs.mil/Portals/54/Documents/DD/issuances/dodi/{m.group(1)}.pdf"

    # EEOC Title VII
    if re.search(r"\bTitle\s*VII\b|\bCivil\s*Rights\s*Act\b", text, flags=re.IGNORECASE):
        return "https://www.eeoc.gov/statutes/title-vii-civil-rights-act-1964"

    # FSIS Notices / Directives
    m = re.search(r"\bFSIS\s*Notice\s*(\d{4}-\d{2})\b", text, flags=re.IGNORECASE)
    if m:
        return f"https://www.fsis.usda.gov/policy/fsis-notices/{m.group(1)}"
    m = re.search(r"\bFSIS\s*(?:Directive)?\s*(\d{4,}\.?[0-9]*)\b", text, flags=re.IGNORECASE)
    if m:
        return f"https://www.fsis.usda.gov/policy/fsis-directives/{m.group(1)}"

    # AMS Administrative Issuances
    if re.search(r"\bAMS Administrative Issuances\b", text, flags=re.IGNORECASE):
        return "https://www.ams.usda.gov/about-ams/policies/ams-issuances"

    # APHIS ASF & Directives
    if re.search(r"\bAfrican\s*Swine\s*Fever\b|\bASF\b", text, flags=re.IGNORECASE):
        return "https://www.aphis.usda.gov/animal-emergencies/asf"
    if re.search(r"\bAPHIS\s*Directive\b", text, flags=re.IGNORECASE):
        return "https://www.aphis.usda.gov/aphis/ourfocus/business-services/directives"

    # USDA Farmers First Agenda
    if re.search(r"\bFarmers\s*First\b", text, flags=re.IGNORECASE):
        return "https://www.usda.gov/sites/default/files/documents/farmers-first-small-family-farms-policy-agenda.pdf"

    # Fallback: Google .gov site search
    if text.strip():
        q = re.sub(r'\s+', '+', text.strip())
        return f"https://www.google.com/search?q=site:.gov+{q}"

    return None


def url_is_valid(url):
    try:
        resp = requests.head(url, allow_redirects=True, timeout=3)
        if resp.status_code >= 200 and resp.status_code < 400:
            return True
        if resp.status_code >= 400 or resp.status_code < 100:
            return False
        resp = requests.get(url, allow_redirects=True, timeout=3)
        return resp.status_code >= 200 and resp.status_code < 400
    except (requests.Timeout, requests.ConnectionError):
        return False
    except Exception:
        return False



def main():
    ap = argparse.ArgumentParser(description="Populate component URLs (timestamped output).")
    ap.add_argument("--in", dest="input_path", default="ivntest.xlsx",
                    help="Path to input Excel (default: ivntest.xlsx)")
    ap.add_argument("--outbase", dest="outbase", default="ivntest_with_urls",
                    help="Base name for the output file (default: ivntest_with_urls)")
    args = ap.parse_args()

    # Load and validate
    df, sheet = load_excel_prefer_components(args.input_path)
    validate_cols(df)

    # Prepare for progress reporting
    blank_indices = [idx for idx, row in df.iterrows() if pd.isna(row["component_url"]) or str(row["component_url"]).strip() == ""]
    total_ops = len(blank_indices)
    if total_ops == 0:
        print("No blank component_url fields to populate.")
        # Timezone: America/Chicago
        if ZoneInfo is not None:
            now_ct = datetime.now(ZoneInfo("America/Chicago"))
        else:
            now_ct = datetime.now()
        stamp = now_ct.strftime("%Y-%m-%d-%H%M")
        out_name = f"{args.outbase}_{stamp}.xlsx"
        df.to_excel(out_name, index=False)
        print(f"Sheet: {sheet}")
        print(f"Total rows: {len(df)}")
        print(f"New URLs populated this run: 0")
        print(f"Saved: {out_name}")
        return

    print(f"Starting URL population for {total_ops} blank entries...")
    start_time = time.time()
    updated = 0
    for op_num, idx in enumerate(blank_indices, 1):
        op_start = time.time()
        row = df.loc[idx]
        op_name = f"Populating URL for row {idx+1} (component_name: {row['component_name']}, component_id: {row['component_id']})"
        print(f"\nOperation {op_num}/{total_ops}: {op_name}")
        url = generate_url(row["component_name"], row["component_id"])
        op_elapsed = time.time() - op_start
        if url and url_is_valid(url):
            df.at[idx, "component_url"] = url
            updated += 1
            op_status = "[SUCCESS]"
        else:
            op_status = "[SKIPPED]"
        # Timing and ETA reporting
        elapsed = time.time() - start_time
        avg_time = elapsed / op_num
        remaining_ops = total_ops - op_num
        est_remaining = avg_time * remaining_ops
        eta = elapsed + est_remaining
        mins, secs = divmod(int(op_elapsed), 60)
        elapsed_mins, elapsed_secs = divmod(int(elapsed), 60)
        rem_mins, rem_secs = divmod(int(est_remaining), 60)
        eta_mins, eta_secs = divmod(int(eta), 60)
        print(f"  {op_status} | Elapsed: {mins}m {secs}s | Total elapsed: {elapsed_mins}m {elapsed_secs}s | Remaining: {rem_mins}m {rem_secs}s | Completed: {op_num} | Remaining: {remaining_ops} | ETA: {eta_mins}m {eta_secs}s")

    # Timezone: America/Chicago
    if ZoneInfo is not None:
        now_ct = datetime.now(ZoneInfo("America/Chicago"))
    else:
        now_ct = datetime.now()
    stamp = now_ct.strftime("%Y-%m-%d-%H%M")
    out_name = f"{args.outbase}_{stamp}.xlsx"
    df.to_excel(out_name, index=False)
    print(f"\nSheet: {sheet}")
    print(f"Total rows: {len(df)}")
    print(f"New URLs populated this run: {updated}")
    print(f"Saved: {out_name}")

if __name__ == "__main__":
    main()
