from pypdf import PdfReader
from pyxlsb import open_workbook
import re
from pathlib import Path

pdf_path = Path('crosswalk.pdf')
xlsb_path = Path('IVN-dataset.xlsb')

print('=== PDF ANALYSIS ===')
reader = PdfReader(str(pdf_path))
meta = reader.metadata or {}
title = None
if isinstance(meta, dict):
    title = meta.get('/Title')
else:
    title = getattr(meta, 'title', None)

texts = []
for p in reader.pages:
    try:
        texts.append(p.extract_text() or '')
    except Exception:
        texts.append('')
full_text = '\n'.join(texts)
print(f'Title: {title if title else "<not available>"}')
print(f'FullTextLength: {len(full_text)}')
print('First3000:')
print(full_text[:3000].replace('\x00',''))

print('\nRequirementLines:')
req_pat = re.compile(r'\b(must|shall|required|deadline|effective)\b', re.I)
req_lines = []
for line in full_text.splitlines():
    s=line.strip()
    if s and req_pat.search(s):
        req_lines.append(s)
uniq=[]
seen=set()
for l in req_lines:
    key=l.lower()
    if key not in seen:
        seen.add(key)
        uniq.append(l)
for l in uniq[:40]:
    print('- ' + l)
if len(uniq)>40:
    print(f'... ({len(uniq)-40} more lines)')

print('\nExplicitCitations:')
patterns = {
    'USC': re.compile(r'\b\d+\s*U\.?\s*S\.?\s*C\.?\s*(?:Sec\.?|section)?\s*[\w\-\.()]*', re.I),
    'EO': re.compile(r'\bE\.?\s*O\.?\s*\d{4,6}\b|\bExecutive\s+Order\s+\d{4,6}\b', re.I),
    'OMB': re.compile(r'\bOMB\b[^\n;,.]*', re.I),
}
for label, pat in patterns.items():
    found=[]
    found_l=set()
    for m in pat.finditer(full_text):
        t=' '.join(m.group(0).split())
        tl=t.lower()
        if t and tl not in found_l:
            found_l.add(tl)
            found.append(t)
    print(f'{label}: {len(found)}')
    for item in found[:20]:
        print('  - ' + item)
    if len(found)>20:
        print(f'  ... ({len(found)-20} more)')

print('\n=== XLSB ANALYSIS ===')
try:
    with open_workbook(str(xlsb_path)) as wb:
        sheets = list(wb.sheets)
        print('SheetCount:', len(sheets))
        for s in sheets:
            print(f'Sheet: {s}')
            try:
                with wb.get_sheet(s) as sh:
                    first=None
                    for row in sh.rows():
                        first=row
                        break
                    if first is None:
                        print('  Headers: <empty sheet>')
                    else:
                        vals=[]
                        for c in first:
                            v=c.v
                            vals.append('' if v is None else str(v))
                        while vals and vals[-1]=='':
                            vals.pop()
                        if vals:
                            print('  Headers: ' + ' | '.join(vals))
                        else:
                            print('  Headers: <no populated cells in first row>')
            except Exception as e:
                print('  Headers: <unreadable> ' + str(e))
except Exception as e:
    print('Failed to open XLSB:', e)
