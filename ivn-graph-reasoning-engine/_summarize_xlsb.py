from pyxlsb import open_workbook
from pathlib import Path

xlsb_path = Path('IVN-dataset.xlsb')
out_path = Path('dataset_summary.txt')

lines = []
lines.append(f"Workbook: {xlsb_path.name}")
lines.append("")

with open_workbook(str(xlsb_path)) as wb:
    sheet_names = list(wb.sheets)
    lines.append(f"Sheet count: {len(sheet_names)}")
    lines.append("")

    for idx, sheet_name in enumerate(sheet_names, 1):
        lines.append(f"Sheet {idx}: {sheet_name}")
        try:
            with wb.get_sheet(sheet_name) as sheet:
                row_iter = sheet.rows()
                first_row = next(row_iter, None)

                if first_row is None:
                    headers = []
                    row_count = 0
                else:
                    headers = [cell.v for cell in first_row]
                    row_count = 1
                    for _ in row_iter:
                        row_count += 1

                header_text = ', '.join('' if h is None else str(h) for h in headers)
                lines.append(f"Headers (row 1): {header_text}")
                lines.append(f"Row count estimate: {row_count}")
        except Exception as e:
            lines.append(f"Error reading sheet: {e}")

        lines.append("")

out_path.write_text('\n'.join(lines), encoding='utf-8')
print(f"Wrote {out_path}")
