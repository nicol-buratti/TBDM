#!/usr/bin/env python3
"""
Research Paper Network Explorer - Streamlit Application
Interactive web interface for exploring research paper networks stored in Neo4j
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from pyvis.network import Network
import tempfile
import os
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NEO4J_URI = os.getenv("NEO4J_URI")


# # Page configuration
# st.set_page_config(
#     page_title="Research Paper Network Explorer",
#     page_icon="📚",
#     layout="wide",
#     initial_sidebar_state="expanded",
# )

# # Custom CSS for better styling
# st.markdown(
#     """
# <style>
#     .main-header {
#         font-size: 3rem;
#         color: #1f77b4;
#         text-align: center;
#         margin-bottom: 2rem;
#     }
#     .metric-card {
#         background-color: #f0f2f6;
#         padding: 1rem;
#         border-radius: 0.5rem;
#         margin: 0.5rem 0;
#     }
#     .stTabs [data-baseweb="tab-list"] {
#         gap: 2px;
#     }
#     .stTabs [data-baseweb="tab"] {
#         height: 50px;
#         padding-left: 20px;
#         padding-right: 20px;
#     }
# </style>
# """,
#     unsafe_allow_html=True,
# )

spark = (
    SparkSession.builder.appName("StreamlitApp")
    .master("spark://spark:7077")
    .config("spark.jars.packages", "neo4j-contrib:neo4j-spark-connector:5.3.1-s_2.12")
    .config("neo4j.url", NEO4J_URI)
    .config("neo4j.authentication.basic.username", "neo4j")
    .config("neo4j.authentication.basic.password", "password")
    .config("neo4j.database", "neo4j")
    .getOrCreate()
)


def execute_spark_query(query: str) -> pd.DataFrame:
    """Execute a Cypher query using Spark and return results as a DataFrame"""
    df = spark.read.format("org.neo4j.spark.DataSource").option("query", query).load()
    return df.toPandas()


def get_data_overview():
    """Get overview statistics of the data"""
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
        overview[key] = result[0]["count"] if result else 0

    return overview


def create_network_visualization(nodes, edges, height=600):
    """Create an interactive network visualization using pyvis"""
    net = Network(
        height=f"{height}px", width="100%", bgcolor="#ffffff", font_color="#000000"
    )

    # Add nodes
    for node in nodes:
        net.add_node(
            node["id"],
            label=node["label"],
            title=node.get("title", node["label"]),
            color=node.get("color", "#1f77b4"),
            size=node.get("size", 20),
        )

    # Add edges
    for edge in edges:
        net.add_edge(
            edge["source"],
            edge["target"],
            label=edge.get("label", ""),
            width=edge.get("width", 1),
        )

    # Configure physics
    net.set_options(
        """
    {
        "physics": {
            "enabled": true,
            "stabilization": {"iterations": 100}
        }
    }
    """
    )

    # Generate HTML
    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
        net.save_graph(tmp_file.name)
        with open(tmp_file.name, "r", encoding="utf-8") as f:
            html_content = f.read()
        os.unlink(tmp_file.name)

    return html_content


def main():
    """Main Streamlit application"""

    # Header
    st.markdown(
        '<h1 class="main-header">📚 Research Paper Network Explorer</h1>',
        unsafe_allow_html=True,
    )

    # Sidebar
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

    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["🏠 Dashboard", "📄 Papers", "👥 People", "🌐 Networks", "🔍 Query"]
    )

    with tab1:
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
        MATCH (p:Paper)
        WHERE p.year IS NOT NULL
        RETURN p.year as year, count(p) as count
        ORDER BY year
        """

        year_data = execute_spark_query(year_query)

        if year_data:
            df_years = pd.DataFrame(year_data)
            fig_years = px.line(
                df_years,
                x="year",
                y="count",
                title="Papers Published by Year",
                markers=True,
            )
            fig_years.update_layout(height=400)
            st.plotly_chart(fig_years, use_container_width=True)
        else:
            st.info("No year data available for papers.")

    with tab2:
        st.header("📄 Papers Explorer")

        # Search and filter
        col1, col2 = st.columns([3, 1])

        with col1:
            search_term = st.text_input(
                "🔍 Search papers by title", placeholder="Enter keywords to search..."
            )

        with col2:
            limit = st.selectbox("Results limit", [10, 25, 50, 100], index=1)

        # Papers query
        if search_term:
            papers_query = """
            MATCH (p:Paper)
            WHERE toLower(p.title) CONTAINS toLower($search)
            RETURN p.title as title, p.year as year, p.author as author, p.abstract as abstract
            ORDER BY p.year DESC
            LIMIT $limit
            """
            parameters = {"search": search_term, "limit": limit}
        else:
            papers_query = """
            MATCH (p:Paper)
            RETURN p.title as title, p.year as year, p.author as author, p.abstract as abstract
            ORDER BY p.year DESC
            LIMIT $limit
            """
            parameters = {"limit": limit}

        papers_data = execute_spark_query(papers_query, parameters)

        if papers_data:
            st.success(f"Found {len(papers_data)} papers")

            for i, paper in enumerate(papers_data):
                with st.expander(
                    f"📄 {paper.get('title', 'Untitled')} ({paper.get('year', 'N/A')})"
                ):
                    col1, col2 = st.columns([2, 1])

                    with col1:
                        if paper.get("abstract"):
                            st.write("**Abstract:**")
                            st.write(paper["abstract"])
                        else:
                            st.write("*No abstract available*")

                    with col2:
                        if paper.get("author"):
                            st.write(f"**Author:** {paper['author']}")
                        if paper.get("year"):
                            st.write(f"**Year:** {paper['year']}")
        else:
            if search_term:
                st.info(f"No papers found matching '{search_term}'")
            else:
                st.info("No papers found in the database.")

    with tab3:
        st.header("👥 People Network")

        # Top authors by publication count
        st.subheader("📊 Most Prolific Authors")

        authors_query = """
        MATCH (person:Person)-[:AUTHORED]->(paper:Paper)
        RETURN person.name
        ORDER BY count(paper) DESC
        LIMIT 20
        """

        authors_data = execute_spark_query(authors_query)

        if authors_data:
            df_authors = pd.DataFrame(authors_data)
            fig_authors = px.bar(
                df_authors.head(10),
                x="paper_count",
                y="name",
                orientation="h",
                title="Top 10 Authors by Publication Count",
            )
            fig_authors.update_layout(height=500)
            st.plotly_chart(fig_authors, use_container_width=True)

            # Full table
            st.subheader("All Authors")
            st.dataframe(df_authors, use_container_width=True)
        else:
            st.info("No author data available.")

        # Editors
        st.subheader("📝 Volume Editors")

        editors_query = """
        MATCH (person:Person)-[r:EDITED]->(volume:Volume)
        RETURN person.name as name, count(volume) as volume_count
        ORDER BY volume_count DESC
        LIMIT 10
        """

        editors_data = execute_spark_query(editors_query)

        if editors_data:
            df_editors = pd.DataFrame(editors_data)
            st.dataframe(df_editors, use_container_width=True)

    with tab4:
        st.header("🌐 Network Visualizations")

        viz_type = st.selectbox(
            "Choose visualization type:",
            [
                "Author Collaboration Network",
                "Paper-Volume Network",
                "Keyword Co-occurrence",
            ],
        )

        if viz_type == "Author Collaboration Network":
            st.subheader("👥 Author Collaboration Network")

            min_collaborations = st.slider("Minimum collaborations to show", 1, 5, 2)

            collab_query = """
            MATCH (p1:Person)-[:AUTHORED]->(paper:Paper)<-[:AUTHORED]-(p2:Person)
            WHERE p1 <> p2
            WITH p1, p2, count(paper) as shared_papers
            WHERE shared_papers >= $min_collab
            RETURN p1.name as author1, p2.name as author2, shared_papers
            LIMIT 100
            """

            collab_data = execute_spark_query(collab_query)

            if collab_data:
                # Prepare network data
                nodes = []
                edges = []
                authors = set()

                for edge in collab_data:
                    authors.add(edge["author1"])
                    authors.add(edge["author2"])

                    edges.append(
                        {
                            "source": edge["author1"],
                            "target": edge["author2"],
                            "label": f"{edge['shared_papers']} papers",
                            "width": min(edge["shared_papers"] * 2, 10),
                        }
                    )

                for author in authors:
                    nodes.append(
                        {
                            "id": author,
                            "label": author,
                            "title": f"Author: {author}",
                            "color": "#ff7f0e",
                            "size": 25,
                        }
                    )

                if nodes and edges:
                    st.success(
                        f"Showing {len(nodes)} authors with {len(edges)} collaborations"
                    )
                    html_content = create_network_visualization(nodes, edges)
                    st.components.v1.html(html_content, height=600)
                else:
                    st.info(
                        f"No collaborations found with minimum {min_collaborations} shared papers."
                    )
            else:
                st.info("No collaboration data available.")

        elif viz_type == "Paper-Volume Network":
            st.subheader("📚 Paper-Volume Network")

            volume_limit = st.slider("Number of volumes to show", 1, 10, 5)

            pv_query = """
            MATCH (v:Volume)<-[:BELONGS_TO]-(p:Paper)
            WITH v, count(p) as paper_count
            ORDER BY paper_count DESC
            LIMIT $limit
            MATCH (v)<-[:BELONGS_TO]-(p:Paper)
            RETURN v.title as volume, p.title as paper
            """

            pv_data = execute_spark_query(pv_query, {"limit": volume_limit})

            if pv_data:
                nodes = []
                edges = []
                volumes = set()
                papers = set()

                for item in pv_data:
                    volumes.add(item["volume"])
                    papers.add(item["paper"])

                    edges.append(
                        {
                            "source": item["paper"],
                            "target": item["volume"],
                            "label": "belongs to",
                        }
                    )

                # Add volume nodes
                for volume in volumes:
                    nodes.append(
                        {
                            "id": volume,
                            "label": (
                                volume[:50] + "..." if len(volume) > 50 else volume
                            ),
                            "title": f"Volume: {volume}",
                            "color": "#d62728",
                            "size": 35,
                        }
                    )

                # Add paper nodes
                for paper in papers:
                    nodes.append(
                        {
                            "id": paper,
                            "label": paper[:30] + "..." if len(paper) > 30 else paper,
                            "title": f"Paper: {paper}",
                            "color": "#2ca02c",
                            "size": 20,
                        }
                    )

                if nodes and edges:
                    st.success(
                        f"Showing {len(volumes)} volumes with {len(papers)} papers"
                    )
                    html_content = create_network_visualization(nodes, edges)
                    st.components.v1.html(html_content, height=600)
            else:
                st.info("No paper-volume relationships found.")

    with tab5:
        st.header("🔍 Custom Query Interface")

        st.write("Execute custom Cypher queries against your Neo4j database:")

        # Predefined queries
        sample_queries = {
            "Show all node labels": "CALL db.labels()",
            "Show all relationship types": "CALL db.relationshipTypes()",
            "Sample papers": "MATCH (p:Paper) RETURN p LIMIT 5",
            "Sample people": "MATCH (p:Person) RETURN p LIMIT 5",
            "Sample volumes": "MATCH (v:Volume) RETURN v LIMIT 5",
            "Paper authors": "MATCH (p:Person)-[:AUTHORED]->(paper:Paper) RETURN p.name, paper.title LIMIT 10",
        }

        selected_query = st.selectbox(
            "Choose a sample query or write your own:",
            [""] + list(sample_queries.keys()),
        )

        if selected_query and selected_query in sample_queries:
            query_text = sample_queries[selected_query]
        else:
            query_text = ""

        query = st.text_area(
            "Cypher Query:",
            value=query_text,
            height=100,
            placeholder="MATCH (n) RETURN n LIMIT 10",
        )

        col1, col2 = st.columns([1, 4])

        with col1:
            execute_btn = st.button("▶️ Execute Query")

        with col2:
            if st.button("ℹ️ Show Schema"):
                schema_query = """
                CALL apoc.meta.graph()
                """
                try:
                    schema_result = execute_spark_query(schema_query)
                    if schema_result:
                        st.json(schema_result)
                    else:
                        # Fallback schema query
                        labels_result = execute_spark_query("CALL db.labels()")
                        rels_result = execute_spark_query("CALL db.relationshipTypes()")

                        st.write("**Node Labels:**")
                        if labels_result:
                            st.write([record["label"] for record in labels_result])

                        st.write("**Relationship Types:**")
                        if rels_result:
                            st.write(
                                [record["relationshipType"] for record in rels_result]
                            )
                except:
                    st.info("Schema information not available")

        if execute_btn and query.strip():
            with st.spinner("Executing query..."):
                try:
                    result = execute_spark_query(query)

                    if result:
                        st.success(
                            f"Query executed successfully! Found {len(result)} results."
                        )

                        # Convert to DataFrame if possible
                        try:
                            df = pd.DataFrame(result)
                            st.dataframe(df, use_container_width=True)
                        except:
                            # Fallback to JSON display
                            st.json(result)
                    else:
                        st.info("Query executed successfully but returned no results.")

                except Exception as e:
                    st.error(f"Query execution failed: {str(e)}")


if __name__ == "__main__":
    main()
