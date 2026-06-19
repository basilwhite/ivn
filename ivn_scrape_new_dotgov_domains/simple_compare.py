import pandas as pd

c = pd.read_excel('Components.xlsx')
o = pd.read_excel('ivn_discovered_alignments.xlsx')

with open('comparison_output.txt', 'w', encoding='utf-8') as f:
    f.write(f"Components.xlsx rows: {len(c)}\n")
    f.write(f"Output rows: {len(o)}\n")
    f.write(f"\nComponents.xlsx columns: {list(c.columns)}\n")
    f.write(f"Output columns: {list(o.columns)}\n")
    f.write(f"\nComponents.xlsx sample row 0:\n")
    f.write(c.iloc[0].to_string() + "\n")
    f.write(f"\nOutput sample row 0:\n")
    f.write(o.iloc[0].to_string() + "\n")
    f.write(f"\nComponents.xlsx component_description[0]:\n")
    f.write(str(c['component_description'].iloc[0]) + "\n")
    f.write(f"\nOutput component_description[0]:\n")
    f.write(str(o['component_description'].iloc[0]) + "\n")

print("Done - see comparison_output.txt")
