
# IVN Network Visualizer
# Created: 2025-06-29

import streamlit as st
import pandas as pd
import networkx as nx
import plotly.graph_objects as go

st.set_page_config(layout="wide")
st.title("Integrated Value Network (IVN) Visualizer")

@st.cache_data
def load_data():z
    components_df = pd.read_excel("IVN_Normalized.xlsx", sheet_name="Components")
    alignments_df = pd.read_excel("IVN_Normalized.xlsx", sheet_name="Alignments")
    sources_df = pd.read_excel("IVN_Normalized.xlsx", sheet_name="Sources")

    merged = alignments_df.merge(
        components_df[['component_id', 'source_id', 'description']],
        left_on='enabling_component_id',
        right_on='component_id',
        how='left'
    ).rename(columns={
        'source_id': 'enabling_source_id',
        'description': 'enabling_description'
    }).drop(columns='component_id')

    merged = merged.merge(
        components_df[['component_id', 'source_id', 'description']],
        left_on='dependent_component_id',
        right_on='component_id',
        how='left'
    ).rename(columns={
        'source_id': 'dependent_source_id',
        'description': 'dependent_description'
    }).drop(columns='component_id')

    merged = merged.merge(
        sources_df,
        left_on='enabling_source_id',
        right_on='source_id',
        how='left'
    ).rename(columns={'source': 'enabling_source'}).drop(columns='source_id')

    merged = merged.merge(
        sources_df,
        left_on='dependent_source_id',
        right_on='source_id',
        how='left'
    ).rename(columns={'source': 'dependent_source'}).drop(columns='source_id')

    return merged, components_df, sources_df

df, components_df, sources_df = load_data()

# Filters
st.sidebar.header("Filters")
enable_filter = st.sidebar.text_input("Enabling Filter (keywords)").strip().lower()
depend_filter = st.sidebar.text_input("Dependent Filter (keywords)").strip().lower()

filtered_df = df.dropna(subset=['enabling_component_id', 'dependent_component_id'])

if enable_filter:
    filtered_df = filtered_df[filtered_df['enabling_description'].str.lower().str.contains(enable_filter, na=False)]

if depend_filter:
    filtered_df = filtered_df[filtered_df['dependent_description'].str.lower().str.contains(depend_filter, na=False)]

tab1, tab2 = st.tabs(["Component Network", "Source Network"])

with tab1:
    st.subheader("Component-Level Network")
    G = nx.DiGraph()
    for _, row in filtered_df.iterrows():
        G.add_edge(row['enabling_component_id'], row['dependent_component_id'])

    pos = nx.spring_layout(G, seed=42)
    edge_x = []
    edge_y = []
    for u, v in G.edges():
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=1, color='gray'),
        hoverinfo='none',
        mode='lines'
    )

    node_x = []
    node_y = []
    hover_texts = []
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        desc = components_df.loc[components_df['component_id'] == node, 'description'].values
        hover_texts.append(desc[0][:150] if len(desc) > 0 else node)

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        text=[n[:6] for n in G.nodes()],
        textposition="top center",
        hoverinfo='text',
        hovertext=hover_texts,
        marker=dict(size=6, color='lightgreen', line_width=1)
    )

    fig = go.Figure(data=[edge_trace, node_trace],
                   layout=go.Layout(
                       margin=dict(b=20,l=5,r=5,t=40),
                       xaxis=dict(showgrid=False, zeroline=False),
                       yaxis=dict(showgrid=False, zeroline=False),
                       title='Component Network'
                   ))
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("Source-Level Network")
    Gs = nx.DiGraph()
    source_edges = filtered_df[['enabling_source', 'dependent_source']].dropna().drop_duplicates()
    for _, row in source_edges.iterrows():
        Gs.add_edge(row['enabling_source'], row['dependent_source'])

    pos = nx.spring_layout(Gs, seed=42)
    edge_xs = []
    edge_ys = []
    for u, v in Gs.edges():
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        edge_xs.extend([x0, x1, None])
        edge_ys.extend([y0, y1, None])

    edge_trace_s = go.Scatter(
        x=edge_xs, y=edge_ys,
        line=dict(width=1, color='black'),
        hoverinfo='none',
        mode='lines'
    )

    node_xs = []
    node_ys = []
    node_labels = []
    for node in Gs.nodes():
        x, y = pos[node]
        node_xs.append(x)
        node_ys.append(y)
        node_labels.append(node)

    node_trace_s = go.Scatter(
        x=node_xs, y=node_ys,
        mode='markers+text',
        text=node_labels,
        textposition="top center",
        hoverinfo='text',
        marker=dict(size=10, color='lightblue', line_width=1)
    )

    fig_s = go.Figure(data=[edge_trace_s, node_trace_s],
                     layout=go.Layout(
                         margin=dict(b=20,l=5,r=5,t=40),
                         xaxis=dict(showgrid=False, zeroline=False),
                         yaxis=dict(showgrid=False, zeroline=False),
                         title='Source Network'
                     ))
    st.plotly_chart(fig_s, use_container_width=True)
