
📘 Power BI Starter Guide for IVN Network

This folder includes:
- IVN_Network_Starter_Data.xlsx → Contains all Enabling/Dependent component linkages
- Instructions to create the IVN network graph

Step-by-step in Power BI Desktop:
1. Open Power BI Desktop
2. Click "Get Data" → Excel → Select IVN_Network_Starter_Data.xlsx
3. Load the table (usually named Sheet1 or Table1)
4. Click the “...” in Visualizations → “Get more visuals”
5. Search for and import: "Network Navigator Chart"
6. Drag these fields into the visual:
   - Source: enabling_component_id
   - Target: dependent_component_id
   - Tooltip: enabling_description, dependent_description
7. (Optional) Add slicers using enabling_description or dependent_description for filtering

Enjoy exploring the Integrated Value Network visually!
