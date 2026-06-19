# Check Alignments sheet columns
import pandas as pd

df = pd.read_excel('ivntest.xlsx', sheet_name='Nonaligned-Edge-Cases')
print('Nonaligned-Edge-Cases columns:')
for c in df.columns:
    print(f'  - "{c}"')
print(f'\nRows: {len(df)}')

# Show first row if exists
if len(df) > 0:
    print('\nFirst row sample:')
    for c in df.columns:
        print(f'  {c}: {df.iloc[0][c]}')
