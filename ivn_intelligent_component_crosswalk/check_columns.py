import pandas as pd

file = 'ivntest.xlsx'

for sheet in ['Components', 'ToBeCrosswalked']:
    df = pd.read_excel(file, sheet_name=sheet)
    print(f'\n{sheet} columns:')
    for col in df.columns:
        print(f'  - {col}')
    print(f'\nFirst row sample:')
    if len(df) > 0:
        first_row = df.iloc[0]
        for col in df.columns:
            print(f'  {col}: {first_row[col]}')
