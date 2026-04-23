from pathlib import Path
import re
from pypdf import PdfReader

pdf_path = Path('crosswalk.pdf')
out_text = Path('extracted_document.txt')
out_req = Path('requirement_lines.txt')
out_cit = Path('citation_lines.txt')

reader = PdfReader(str(pdf_path))
texts = []
for p in reader.pages:
    try:
        texts.append(p.extract_text() or '')
    except Exception:
        texts.append('')
full_text = '\n'.join(texts)
out_text.write_text(full_text, encoding='utf-8')

lines = full_text.splitlines()
req_pat = re.compile(r'\b(must|shall|required|deadline|effective)\b', re.I)
req_lines = [ln for ln in lines if req_pat.search(ln)]
out_req.write_text('\n'.join(req_lines), encoding='utf-8')

cit_pat = re.compile(r'(\b\d+\s*U\.?\s*S\.?\s*C\.?\b|\bUnited\s+States\s+Code\b|\bExecutive\s+Order\b|\bE\.?\s*O\.?\s*\d{4,6}\b|\bOMB\b|\bM-\d{2}-\d{2}\b|\bCircular\b)', re.I)
cit_lines = [ln for ln in lines if cit_pat.search(ln)]
out_cit.write_text('\n'.join(cit_lines), encoding='utf-8')

print(f'Total text length: {len(full_text)}')
print(f'Number of lines: {len(lines)}')
print('First 120 lines of extracted_document.txt:')
for i, ln in enumerate(lines[:120], 1):
    print(f'{i:03d}: {ln}')
