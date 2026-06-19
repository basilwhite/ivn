import pandas as pd
print(pd.read_excel('ivntest.xlsx', sheet_name=0, engine='openpyxl').head())