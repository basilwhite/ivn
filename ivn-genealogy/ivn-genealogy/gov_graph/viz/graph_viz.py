"""Graph visualization using networkx and pyvis."""
import networkx as nx
from pyvis.network import Network
import os
import streamlit as st
import pandas as pd

from gov_graph.config import get_backend
from gov_graph.queries import ancestry, compliance, heatmap

VIZ_OUT_DIR = 'viz_out'

def _create_viz_dir():
    if not os.path.exists(VIZ_OUT_DIR):
        os.makedirs(VIZ_OUT_DIR)

def _query_to_graph(query, source_col, target_col, edge_label):
    backend = get_backend()
    results = backend.run_cypher(query)
    
    G = nx.DiGraph()
    for record in results:
        source = record[source_col]
        target = record[target_col]
        G.add_node(source, label=source, title=source)
        G.add_node(target, label=target, title=target)
        G.add_edge(source, target, label=edge_label)
    return G

def visualize_retirement_slice():
    """Generates a visualization of the retirement slice."""
    _create_viz_dir()
    query = """
    MATCH p = (usc:Provision)-[:IMPLEMENTED_BY_Provision]->(cfr:Provision)
    WHERE usc.id STARTS WITH 'USC_T5_CH8'
    RETURN usc.citation as usc_citation, cfr.citation as cfr_citation
    """
    G = _query_to_graph(query, 'usc_citation', 'cfr_citation', 'IMPLEMENTED_BY')
    
    net = Network(notebook=True, cdn_resources='in_line', height="750px", width="100%")
    net.from_nx(G)
    net.show(os.path.join(VIZ_OUT_DIR, 'retirement_slice.html'))

def visualize_m2610_lineage():
    """Generates a visualization of the M-26-10 lineage."""
    _create_viz_dir()
    query = """
    MATCH path = (prov:Provision)<-[*]-(memo:Document {id: 'OMB_M-26-10'})-[:IMPLEMENTED_BY_Doc]->(directive:Document)
    RETURN prov.citation as source, memo.id as target
    UNION
    MATCH path = (memo:Document {id: 'OMB_M-26-10'})-[:IMPLEMENTED_BY_Doc]->(directive:Document)
    RETURN memo.id as source, directive.id as target
    """
    backend = get_backend()
    results = backend.run_cypher(query)

    G = nx.DiGraph()
    for record in results:
        G.add_edge(record['source'], record['target'])

    net = Network(notebook=True, cdn_resources='in_line', height="750px", width="100%")
    net.from_nx(G)
    net.show(os.path.join(VIZ_OUT_DIR, 'm2610_lineage.html'))


# --- Streamlit App ---
def run_streamlit_app():
    st.set_page_config(layout="wide")
    st.title("Legal Genealogy Graph Explorer")

    st.sidebar.header("Queries")
    query_choice = st.sidebar.selectbox(
        "Choose a query to run:",
        ["M-26-10 Lineage", "USDA Compliance", "CIO Impact", "M-26-10 Heatmap"]
    )

    if st.sidebar.button("Run Query"):
        if query_choice == "M-26-10 Lineage":
            st.subheader("M-26-10 Full Lineage to USDA Directives")
            data = ancestry.full_lineage_memo_to_usda()
            st.dataframe(pd.DataFrame(data))
            visualize_m2610_lineage()
            st.subheader("Visualization")
            with open(os.path.join(VIZ_OUT_DIR, 'm2610_lineage.html'), 'r', encoding='utf-8') as f:
                st.components.v1.html(f.read(), height=800)

        elif query_choice == "USDA Compliance":
            st.subheader("USDA Compliance Surface")
            st.write("Provisions Binding USDA:")
            st.dataframe(pd.DataFrame(compliance.usda_bound_provisions()))
            st.write("Directives Issued by USDA:")
            st.dataframe(pd.DataFrame(compliance.usda_directives()))

        elif query_choice == "CIO Impact":
            st.subheader("CIO Impact and Compliance")
            st.write("CIO Compliance Surface (Directives):")
            st.dataframe(pd.DataFrame(compliance.cio_compliance_surface()))
            # The impact path query is complex, so we'll just show the table
            st.write("M-26-10 Impact Paths to CIO:")
            st.dataframe(pd.DataFrame(impact.cio_m2610_impact_paths()))

        elif query_choice == "M-26-10 Heatmap":
            st.subheader("M-26-10 Influence Heatmap")
            data = heatmap.get_m2610_influence_heatmap()
            df = pd.DataFrame(data)
            st.dataframe(df)
            
            st.bar_chart(df.set_index('directive')['influence_score'])

if __name__ == '__main__':
    # To run the streamlit app:
    # streamlit run gov_graph/viz/graph_viz.py
    run_streamlit_app()
