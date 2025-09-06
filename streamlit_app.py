import streamlit as st
import pandas as pd
import plotly.express as px
import os
import logging
import re
import numpy as np
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from st_link_analysis import st_link_analysis, NodeStyle, EdgeStyle

from functionalities.link_prediction import bulk_link_prediction
from functionalities.similarity import similarity
from functionalities.community_detection import get_spark_df_communities


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _initialize_spark() -> SparkSession:
    """Create a Spark Session for Streamlit app"""
    spark = (
        SparkSession.builder.appName("StreamlitApp")
        .master(SPARK_MASTER_URL)
        .config(
            "spark.jars.packages", "neo4j-contrib:neo4j-spark-connector:5.3.1-s_2.12"
        )
        .config("neo4j.url", NEO4J_URI)
        .config("neo4j.authentication.basic.username", NEO4J_USERNAME)
        .config("neo4j.authentication.basic.password", NEO4J_PASSWORD)
        .config("neo4j.database", "neo4j")
        .getOrCreate()
    )
    return spark


NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:password@localhost:7687")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[*]")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
LINK_PREDICTION_THRESHOLD = os.getenv("LINK_PREDICTION_THRESHOLD", "1")
LINK_PREDICTION_THRESHOLD = int(LINK_PREDICTION_THRESHOLD)
NODE_ID = 230

if "spark" not in st.session_state:
    st.session_state.spark = _initialize_spark()


def execute_spark_query(query: str):
    """Execute a Cypher query using Spark and return results as a DataFrame"""
    df = (
        st.session_state.spark.read.format("org.neo4j.spark.DataSource")
        .option("query", query)
        .load()
    )
    return df.toPandas()


def get_paper_search(search_term, limit=25):
    df = (
        st.session_state.spark.read.format("org.neo4j.spark.DataSource")
        .option("labels", ":Paper")
        .load()
    )
    if search_term:
        escaped_search = search_term.replace("'", "\\'")
        df = (
            df.withColumn("title", F.lower(F.col("title")))
            .filter(F.col("title").contains(escaped_search))
            .orderBy(F.col("year").desc())
        )
    else:
        df = df.orderBy(F.col("year").desc())
    return df.limit(limit).toPandas()


def get_data_overview():
    """Get overview statistics of the data"""
    logging.info("Fetching data overview...")
    queries = {
        "papers": "MATCH (p:Paper) RETURN count(p) as count",
        "volumes": "MATCH (v:Volume) RETURN count(v) as count",
        "people": "MATCH (p:Person) RETURN count(p) as count",
        "keywords": "MATCH (k:Keyword) RETURN count(k) as count",
        "authorship_relations": "MATCH ()-[r:AUTHORED]->() RETURN count(r) as count",
        "editorship_relations": "MATCH ()-[r:EDITED]->() RETURN count(r) as count",
        "belongs_relations": "MATCH ()-[r:BELONGS_TO]->() RETURN count(r) as count",
    }

    overview = {}
    for key, query in queries.items():
        result = execute_spark_query(query)
        if not result.empty:
            overview[key] = result.iloc[0]["count"]
        else:
            overview[key] = 0

    return overview


def get_community():
    logger.info("Fetching community detection data...")
    return get_spark_df_communities(
        st.session_state.spark, "louvain", "graph"
    ).toPandas()


def get_similarity(_df):
    logging.info("Fetching similarity data...")
    return similarity(st.session_state.spark, "graph", _df).toPandas()


def sidebar():
    with st.sidebar:
        st.title("🔧 Controls")

        # Connection status
        st.success("✅ Connected to Neo4j")

        # Data overview
        st.subheader("📊 Data Overview")

        with st.spinner("Loading data overview..."):
            overview = get_data_overview()

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Papers", overview.get("papers", 0))
            st.metric("People", overview.get("people", 0))
            st.metric("Keywords", overview.get("keywords", 0))

        with col2:
            st.metric("Volumes", overview.get("volumes", 0))
            st.metric("Authorships", overview.get("authorship_relations", 0))
            st.metric("Editorships", overview.get("editorship_relations", 0))

        # Refresh button
        if st.button("🔄 Refresh Data"):
            st.cache_resource.clear()
            st.rerun()


def tab1_overlay():
    overview = get_data_overview()
    st.header("Dashboard")

    if overview.get("papers", 0) == 0:
        st.warning(
            "⚠️ No data found in the database. Please run your scraper first to populate the database."
        )
        st.info(
            "Make sure your scraper has successfully imported data into Neo4j before using this application."
        )
        return

    # Key metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Papers",
            overview.get("papers", 0),
            help="Total number of research papers in the database",
        )

    with col2:
        st.metric(
            "Total Authors",
            overview.get("people", 0),
            help="Total number of unique authors and editors",
        )

    with col3:
        st.metric(
            "Research Volumes",
            overview.get("volumes", 0),
            help="Total number of research volumes/collections",
        )

    with col4:
        avg_papers_per_volume = overview.get("papers", 0) / max(
            overview.get("volumes", 1), 1
        )
        st.metric(
            "Avg Papers/Volume",
            f"{avg_papers_per_volume:.1f}",
            help="Average number of papers per volume",
        )
    
    # Recent papers chart
    st.subheader("📈 Papers by Year")

    year_query = """
    MATCH (v:Volume)-[:CONTAINS]->(p:Paper)
    WHERE v.pubyear IS NOT NULL
    RETURN v.pubyear as year, count(p) as count
    ORDER BY year
    """

    year_data = execute_spark_query(year_query)

    if year_data.empty:
        st.info("No year data available for papers.")
    else:
        # Convert year to integer if it's a string
        year_data['year'] = pd.to_numeric(year_data['year'], errors='coerce')
        year_data = year_data.dropna(subset=['year'])
        year_data['year'] = year_data['year'].astype(int)
        
        fig = px.line(
            year_data,
            x='year',
            y='count',
            title='Number of Papers Published by Year',
            labels={'year': 'Year', 'count': 'Number of Papers'},
            markers=True
        )
        
        fig.update_layout(
            xaxis_title="Year",
            yaxis_title="Number of Papers",
            hovermode='x unified',
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if not year_data.empty:
                st.metric("Earliest Year", int(year_data['year'].min()))
        
        with col2:
            if not year_data.empty:
                st.metric("Latest Year", int(year_data['year'].max()))
        
        with col3:
            if not year_data.empty:
                avg_papers_per_year = year_data['count'].mean()
                st.metric("Avg Papers/Year", f"{avg_papers_per_year:.1f}")
    
    # Most common keywords chart
    st.subheader("🏷️ Most Common Keywords")
    
    keywords_query = """
    MATCH (p:Paper)-[:HAS_KEYWORD]->(k:Keyword)
    WITH k.name as keyword, count(p) as paper_count
    ORDER BY paper_count DESC
    LIMIT 20
    RETURN keyword, paper_count
    """
    
    keywords_data = execute_spark_query(keywords_query)
    
    if keywords_data.empty:
        st.info("No keyword data available.")
    else:
        # Create horizontal bar chart for better readability of keyword names
        fig_keywords = px.bar(
            keywords_data,
            x='paper_count',
            y='keyword',
            orientation='h',
            title='Top 20 Most Common Keywords in Papers',
            labels={'paper_count': 'Number of Papers', 'keyword': 'Keyword'},
            color='paper_count',
            color_continuous_scale='Blues',
            text='paper_count'
        )
        
        fig_keywords.update_layout(
            height=600,
            xaxis_title="Number of Papers",
            yaxis_title="Keywords",
            showlegend=False,
            yaxis={'categoryorder': 'total ascending'},
            coloraxis_showscale=False
        )
        
        fig_keywords.update_traces(
            texttemplate='%{text}',
            textposition='outside'
        )
        
        st.plotly_chart(fig_keywords, use_container_width=True)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_keywords_query = """
            MATCH (k:Keyword)
            RETURN count(DISTINCT k) as count
            """
            total_keywords = execute_spark_query(total_keywords_query)
            if not total_keywords.empty:
                st.metric("Total Unique Keywords", total_keywords.iloc[0]['count'])
        
        with col2:
            avg_keywords_query = """
            MATCH (p:Paper)-[:HAS_KEYWORD]->(k:Keyword)
            WITH p, count(k) as keyword_count
            RETURN avg(keyword_count) as avg_keywords
            """
            avg_keywords = execute_spark_query(avg_keywords_query)
            if not avg_keywords.empty and avg_keywords.iloc[0]['avg_keywords']:
                st.metric("Avg Keywords per Paper", f"{avg_keywords.iloc[0]['avg_keywords']:.1f}")
        
        with col3:
            if not keywords_data.empty:
                most_common = keywords_data.iloc[0]
                st.metric("Most Common Keyword", 
                         most_common['keyword'], 
                         f"({most_common['paper_count']} papers)")


def tab2_overlay():
    st.header("📄 Papers Explorer")
    
    # Load all papers data at once
    papers_query = """
    MATCH (v:Volume)-[:CONTAINS]->(p:Paper)
    WHERE v.pubyear IS NOT NULL
    RETURN DISTINCT
        p.title as title,
        p.url as url,
        p.abstract as abstract,
        p.pages as pages,
        v.pubyear as year,
        v.voltitle as volume_title,
        v.volnr as volume_id
    ORDER BY v.pubyear DESC, p.title
    """
    
    try:
        papers_df = (
            st.session_state.spark.read.format("org.neo4j.spark.DataSource")
            .option("query", papers_query)
            .load()
            .limit(100)  # Limit to 100 papers for performance
            .toPandas()
        )
    except Exception as e:
        st.error(f"Error loading papers: {str(e)}")
        papers_df = pd.DataFrame()
        return
    
    if papers_df.empty:
        st.warning("No papers found in the database.")
        return
    
    # Convert year to numeric
    papers_df['year'] = pd.to_numeric(papers_df['year'], errors='coerce')
    
    # Quick Statistics
    st.subheader("📊 Quick Statistics")
    
    stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
    
    with stat_col1:
        total_papers = len(papers_df)
        st.metric("Papers Shown", total_papers)
    
    with stat_col2:
        papers_with_abstract = papers_df['abstract'].notna().sum()
        st.metric("Papers with Abstract", papers_with_abstract)
    
    with stat_col3:
        unique_volumes = papers_df['volume_title'].nunique()
        st.metric("Unique Volumes", unique_volumes)
    
    with stat_col4:
        if not papers_df.empty and 'year' in papers_df.columns:
            year_range = f"{int(papers_df['year'].min())}-{int(papers_df['year'].max())}"
            st.metric("Year Range", year_range)
        else:
            st.metric("Year Range", "N/A")
    
    # Display sample papers
    st.subheader("📄 Recent Papers Sample")
    st.info("Showing a sample of recent papers from the database")
    
    # Get the 20 most recent papers
    recent_papers = papers_df.nlargest(20, 'year')
    
    # Display papers
    for idx, row in recent_papers.iterrows():
        paper_title = row.get('title', 'Untitled')
        paper_year = row.get('year', 'N/A')
        volume_title = row.get('volume_title', 'Unknown Volume')
        
        with st.expander(f"📄 {paper_title} ({paper_year})"):
            col_left, col_right = st.columns([3, 1])
            
            with col_left:
                st.markdown("**Abstract:**")
                if row.get('abstract'):
                    # Truncate abstract
                    abstract_text = row['abstract']
                    if len(abstract_text) > 1000:
                        abstract_text = abstract_text[:1000] + "..."
                    st.write(abstract_text)
                else:
                    st.write("*No abstract available*")
            
            with col_right:
                st.markdown("**Details:**")
                st.write(f"📅 Year: {paper_year}")
                st.write(f"📚 Volume: {volume_title}")
                
                if row.get('pages'):
                    st.write(f"📄 Pages: {row['pages']}")
                
                if row.get('url'):
                    st.markdown(f"[🔗 View Paper]({row['url']})")
    
def tab3_overlay():
    st.header("👥 People Network")

    # Top authors by publication count
    st.subheader("📊 Most Prolific Authors")

    authors_query = """
    MATCH (person:Person)-[:AUTHORED]->(paper:Paper)
    WITH person, count(paper) as paper_count
    ORDER BY paper_count DESC
    WITH person.name as name, paper_count
    RETURN name, paper_count
    """

    authors_data = execute_spark_query(authors_query)

    if not authors_data.empty:
        df_authors = pd.DataFrame(authors_data)
        
        # Sort descending to ensure largest first
        df_authors_sorted = df_authors.sort_values(by="paper_count", ascending=False)
        
        fig_authors = px.bar(
            df_authors_sorted.head(10),
            x="paper_count",
            y="name",
            orientation="h",
            title="Top 10 Authors by Publication Count",
        )
        
        # Make largest bar appear on top
        fig_authors.update_layout(
            height=500,
            yaxis={'categoryorder':'total ascending'}
        )
        
        st.plotly_chart(fig_authors, use_container_width=True)

def tab4_overlay():
    # Style node & edge groups
    node_styles = [
        NodeStyle("Volume", "#0E12F3", "volnr", "description"),
        NodeStyle("Paper", "#04D10E", "title", "description"),
        NodeStyle("Person", "#0EEDF9", "name", "person"),
        NodeStyle("Keyword", "#FF7F3E", "name", "key"),
    ]

    edge_styles = [
        EdgeStyle("AUTHORED", caption="label", directed=False),
        EdgeStyle("CONTAINS", caption="label", directed=False),
        EdgeStyle("EDITED", caption="label", directed=False),
        EdgeStyle("HAS_KEYWORD", caption="label", directed=False),
        EdgeStyle(
            label="POSSIBLY_RELATED", color="#ff0000", caption="label", directed=False
        ),
        EdgeStyle(label="SIMILAR", color="#0000ff", caption="label", directed=False),
    ]
    
    query = f"""
        MATCH (n)-[r]-(m)
        WHERE id(n) = {NODE_ID}
        RETURN n, r, m
        """
    df = execute_spark_query(query).head(50)
    df = st.session_state.spark.createDataFrame(df)

    elements = transform_df_to_graph_elements(df)

    # Render the component
    st.markdown("## Node overview")
    st_link_analysis(elements, "cose", node_styles, edge_styles)

    df_community, community_elements = display_community(node_styles, edge_styles)

    display_link_prediction(node_styles, edge_styles, community_elements)

    st.markdown("## Similarity")
    try:
        df_similarity = get_similarity(st.session_state.spark.createDataFrame(df_community))
        
        if df_similarity is not None and not df_similarity.empty:
            edges = []
            if community_elements.get("edges") and len(community_elements["edges"]) > 0:
                maxx = max(community_elements["edges"], key=lambda x: x["data"]["id"])["data"]["id"]
            else:
                maxx = 0
                
            for index, row in df_similarity.iterrows():
                maxx += 1
                edge = {}
                edge["id"] = maxx
                edge["label"] = "SIMILAR"
                # Check which column names are actually in the dataframe
                if "node1" in row:
                    edge["source"] = row["node1"]
                    edge["target"] = row["node2"]
                    edge["similarity"] = 1.0 - row["distCol"]
                elif "nodeId1" in row:
                    edge["source"] = row["nodeId1"]
                    edge["target"] = row["nodeId2"]
                    edge["similarity"] = 1.0 - row["features_diff_sum"]
                else:
                    # Debug: show what columns are actually available
                    st.warning(f"Unexpected column names in similarity data: {list(row.index)}")
                    continue
                    
                edges.append({"data": edge})
            
            if edges:
                community_elements["edges"].extend(edges)
            st_link_analysis(community_elements, "cose", node_styles, edge_styles, key="similarity")
        else:
            st.info("No similarity data available for this community")
            st_link_analysis(community_elements, "cose", node_styles, edge_styles, key="similarity_empty")
    except Exception as e:
        st.error(f"Error in similarity analysis: {str(e)}")
        st.link_analysis(community_elements, "cose", node_styles, edge_styles, key="similarity_error")


def display_community(node_styles, edge_styles):
    df_community = get_community_detection_df_graph(NODE_ID)
    community_elements = transform_df_to_graph_elements(df_community)
    st.markdown("## Community")
    st_link_analysis(community_elements, "cose", node_styles, edge_styles)
    
    st.markdown("### 📊 Community Analytics")
    
    # Get all communities
    all_communities_df = get_community()
    
    if all_communities_df is None or all_communities_df.empty:
        st.warning("No community data available")
        return df_community, community_elements
    
    # Find which community the current node belongs to
    current_community_id = None
    node_ids_list = [row['nodeIds'] for _, row in all_communities_df.iterrows()]
    for i, nodes in enumerate(node_ids_list):
        if NODE_ID in nodes:
            current_community_id = i
            break
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_communities = len(all_communities_df)
        st.metric("Total Communities", total_communities)
    
    with col2:
        if current_community_id is not None:
            current_community_size = all_communities_df.iloc[current_community_id]['item_count']
            st.metric("Current Community Size", current_community_size)
        else:
            st.metric("Current Community Size", "N/A")
    
    with col3:
        avg_community_size = all_communities_df['item_count'].mean()
        st.metric("Avg Community Size", f"{avg_community_size:.1f}")
    
    with col4:
        largest_community = all_communities_df['item_count'].max()
        st.metric("Largest Community", largest_community)

    viz_col1, viz_col2 = st.columns(2)
    
    with viz_col1:
        # Community size distribution
        st.subheader("📈 Community Size Distribution")
        fig_dist = px.histogram(
            all_communities_df.head(20),
            x='item_count',
            nbins=20,
            title="Distribution of Community Sizes (Top 20)",
            labels={'item_count': 'Number of Nodes', 'count': 'Number of Communities'},
            color_discrete_sequence=['#1f77b4']
        )
        fig_dist.update_layout(
            height=350,
            showlegend=False,
            bargap=0.1
        )
        
        fig_dist.update_xaxes(dtick=100)

        st.plotly_chart(fig_dist, use_container_width=True)
    
    with viz_col2:
        # Top communities
        st.subheader("🏆 Top 10 Largest Communities")
        top_communities = all_communities_df.head(10).copy()
        top_communities['community_label'] = ['Community ' + str(i) for i in range(len(top_communities))]
        
        colors = ['#FF7F3E' if i == current_community_id else '#1f77b4' 
                  for i in range(len(top_communities))]
        
        fig_top = px.bar(
            top_communities,
            x='community_label',
            y='item_count',
            title="Largest Communities by Node Count",
            labels={'item_count': 'Number of Nodes', 'community_label': 'Community'},
            color_discrete_sequence=colors
        )
        fig_top.update_layout(
            height=350,
            showlegend=False,
            xaxis_tickangle=-45
        )
        st.plotly_chart(fig_top, use_container_width=True)
    
    # Node type composition in current community
    if current_community_id is not None and len(community_elements['nodes']) > 0:
        st.subheader("🎯 Current Community Composition")
        
        node_types = {}
        for node in community_elements['nodes']:
            node_label = node['data']['label']
            node_types[node_label] = node_types.get(node_label, 0) + 1
        
        comp_col1, comp_col2 = st.columns(2)
        
        with comp_col1:
            # Chart of node types
            if node_types:
                fig_pie = px.pie(
                    values=list(node_types.values()),
                    names=list(node_types.keys()),
                    title=f"Node Types in Community {current_community_id}",
                    color_discrete_map={
                        'Volume': '#0E12F3',
                        'Paper': '#04D10E',
                        'Person': '#0EEDF9',
                        'Keyword': '#FF7F3E'
                    }
                )
                fig_pie.update_traces(
                    textposition='inside',
                    textinfo='percent+label'
                )
                fig_pie.update_layout(height=300)
                st.plotly_chart(fig_pie, use_container_width=True)
        
        with comp_col2:
            # Table of node type counts
            if node_types:
                node_types_df = pd.DataFrame(
                    list(node_types.items()),
                    columns=['Node Type', 'Count']
                ).sort_values('Count', ascending=False)
                
                st.markdown("**Node Type Breakdown:**")
                st.dataframe(node_types_df, use_container_width=True, hide_index=True)
                
                total_nodes = sum(node_types.values())
                total_edges = len(community_elements['edges'])
                density = (2 * total_edges) / (total_nodes * (total_nodes - 1)) if total_nodes > 1 else 0
                
                st.markdown("**Community Metrics:**")
                st.write(f"• **Total Edges:** {total_edges}")
                st.write(f"• **Graph Density:** {density:.3f}")
                st.write(f"• **Avg Degree:** {(2 * total_edges / total_nodes):.2f}" if total_nodes > 0 else "N/A")
    
    return df_community, community_elements


def display_link_prediction(node_styles, edge_styles, community_elements):
    s = list(
        {
            e["data"]["id"]
            for e in community_elements["nodes"]
            if e["data"]["label"] == "Person"
        }
    )

    predictions = bulk_link_prediction(st.session_state.spark, "Person", s)
    predictions = predictions.where(predictions["score"] > LINK_PREDICTION_THRESHOLD)

    # Collect predictions for analytics
    predictions_list = predictions.collect()
    predicted_edges = []
    
    maxx = max(community_elements["edges"], key=lambda x: x["data"]["id"])["data"]["id"]
    for p in predictions_list:
        if p["p1"]["<id>"] != p["p2"]["<id>"] and p["p1"]["<id>"] > p["p2"]["<id>"]:
            maxx += 1
            edge = {}
            edge["id"] = maxx
            edge["label"] = "POSSIBLY_RELATED"
            edge["source"] = p["p1"]["<id>"]
            edge["target"] = p["p2"]["<id>"]
            edge["score"] = float(p["score"])  # Store the score for analytics
            community_elements["edges"].append({"data": edge})
            predicted_edges.append(edge)
            
    st.markdown("## Link Prediction")
    st_link_analysis(
        community_elements, "cose", node_styles, edge_styles, key="POSSIBLY_RELATED"
    )
    
    # Add Link Prediction Analytics
    st.markdown("### 🔮 Link Prediction Analytics")
    
    if len(predicted_edges) > 0:
        # Basic metrics
        col1, col2, col3, col4 = st.columns(4)
        
        scores = [e['score'] for e in predicted_edges]
        
        with col1:
            st.metric("Predicted Links", len(predicted_edges))
        
        with col2:
            avg_score = sum(scores) / len(scores) if scores else 0
            st.metric("Avg Prediction Score", f"{avg_score:.2f}")
        
        with col3:
            max_score = max(scores) if scores else 0
            st.metric("Highest Score", f"{max_score:.2f}")
        
        with col4:
            st.metric("Threshold Used", LINK_PREDICTION_THRESHOLD)
        
        # Visualizations
        viz_col1, viz_col2 = st.columns(2)

        st.subheader("📊 Score Distribution")
        if scores:
            scores_df = pd.DataFrame({'score': scores})
            fig_hist = px.histogram(
                scores_df,
                x='score',
                nbins=20,
                title="Distribution of Prediction Scores",
                labels={'score': 'Prediction Score', 'count': 'Number of Predictions'},
                color_discrete_sequence=['#FF6B6B']
            )
            fig_hist.update_layout(
                height=350,
                showlegend=False
            )
            st.plotly_chart(fig_hist, use_container_width=True)
        
        # Connectivity Analysis
        st.subheader("🌐 Predicted Connectivity Analysis")
        
        # Count predictions per node
        node_predictions = {}
        for edge in predicted_edges:
            source = edge['source']
            target = edge['target']
            node_predictions[source] = node_predictions.get(source, 0) + 1
            node_predictions[target] = node_predictions.get(target, 0) + 1
        
        if node_predictions:
            connectivity_df = pd.DataFrame(
                list(node_predictions.items()),
                columns=['Node ID', 'Predicted Connections']
            ).sort_values('Predicted Connections', ascending=False)
            
            conn_col1, conn_col2 = st.columns(2)
            
            with conn_col1:
                # Most connected nodes
                if not connectivity_df.empty:
                    top_connected = connectivity_df.head(10)
                    fig_connected = px.bar(
                        top_connected,
                        x='Predicted Connections',
                        y='Node ID',
                        orientation='h',
                        title="Most Connected Nodes (by Predictions)",
                        labels={'Predicted Connections': 'Number of Predicted Links', 'Node ID': 'Node'},
                        color='Predicted Connections',
                        color_continuous_scale='Viridis'
                    )
                    fig_connected.update_layout(
                        height=350,
                        showlegend=False,
                        coloraxis_showscale=False,
                        yaxis={'type': 'category'}
                    )
                    st.plotly_chart(fig_connected, use_container_width=True)
            
            with conn_col2:
                # Statistics
                st.markdown("**Connectivity Statistics:**")
                
                avg_conn = connectivity_df['Predicted Connections'].mean()
                max_conn = connectivity_df['Predicted Connections'].max()
                min_conn = connectivity_df['Predicted Connections'].min()
                std_conn = connectivity_df['Predicted Connections'].std()
                
                stats_df = pd.DataFrame({
                    'Metric': ['Average', 'Maximum', 'Minimum', 'Std Dev', 'Total Nodes'],
                    'Value': [f"{avg_conn:.2f}", 
                             f"{max_conn:.0f}", 
                             f"{min_conn:.0f}", 
                             f"{std_conn:.2f}",
                             f"{len(connectivity_df)}"]
                })
                st.table(stats_df)
                
                # Additional insights
                st.markdown("**Insights:**")
                high_connected = connectivity_df[connectivity_df['Predicted Connections'] >= avg_conn + std_conn]
                st.write(f"• {len(high_connected)} nodes with above-average predictions")
                st.write(f"• {len(connectivity_df)} total nodes involved in predictions")


def get_community_detection_df_graph(node_id):
    df_community = get_community()
    if isinstance(df_community, pd.DataFrame):
        df_community = st.session_state.spark.createDataFrame(df_community)

    comm = 0
    node_ids_list = [
        row.nodeIds for row in df_community.select("nodeIds").collect()
    ]  # extract as list

    for i, n in enumerate(node_ids_list):
        if node_id in n:
            comm = i
            break

    query = f"""
    WITH {node_ids_list[comm]} AS ids
    MATCH (n)-[r]->(m)
    WHERE id(n) IN ids AND id(m) IN ids
    RETURN n, r, m
    """
    df_community = execute_spark_query(query)
    return df_community


def transform_df_to_graph_elements(df):
    if isinstance(df, pd.DataFrame):
        df = st.session_state.spark.createDataFrame(df)

    elements = [row.asDict() for row in df.collect()]
    edges = []
    nodes = []

    for e in elements:
        node = e["n"].asDict()
        node["id"] = node.pop("<id>")
        node["label"] = node.pop("<labels>")[0]
        nodes.append({"data": node})

        node = e["m"].asDict()
        node["id"] = node.pop("<id>")
        node["label"] = node.pop("<labels>")[0]
        nodes.append({"data": node})

        edge = e["r"].asDict()
        edge["id"] = edge.pop("<rel.id>")
        edge["label"] = edge.pop("<rel.type>")
        edge["source"] = edge.pop("<source.id>")
        edge["target"] = edge.pop("<target.id>")

        edges.append({"data": edge})

    elements = {
        "nodes": nodes,
        "edges": edges,
    }

    return elements

if NODE_ID == -1:
    query = """MATCH (n:Paper) RETURN n LIMIT 1"""
    execute_spark_query(query)
    execute_spark_query(query)

# Header
st.title("📚 Research Paper Network Explorer")

# Sidebar
sidebar()

# # Main content tabs
tab1, tab2, tab3, tab4 = st.tabs(
    ["🏠 Dashboard", "📄 Papers", "👥 People", "🌐 Networks"]
)

with tab1:
    tab1_overlay()

with tab2:
    tab2_overlay()

with tab3:
    tab3_overlay()

with tab4:
    tab4_overlay()