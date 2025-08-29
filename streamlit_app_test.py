import streamlit as st
import pandas as pd
import plotly.express as px
from pyvis.network import Network
import tempfile
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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

if "spark" not in st.session_state:
    st.session_state.spark = _initialize_spark()


@st.cache_data
def execute_spark_query(query: str):
    """Execute a Cypher query using Spark and return results as a DataFrame"""
    df = (
        st.session_state.spark.read.format("org.neo4j.spark.DataSource")
        .option("query", query)
        .load()
    )
    return df.toPandas()


@st.cache_data
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


@st.cache_data
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
        if not result.empty:
            overview[key] = result.iloc[0]["count"]
        else:
            overview[key] = 0

    return overview


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
    MATCH (p:Paper)
    WHERE p.year IS NOT NULL
    RETURN p.year as year, count(p) as count
    ORDER BY year
    """

    year_data = execute_spark_query(year_query)

    if not year_data.empty:
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


def tab2_overlay():
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
        escaped_search = search_term.replace("'", "\\'")
        papers_query = f"""
        MATCH (p:Paper)
        WHERE toLower(p.title) CONTAINS toLower('{escaped_search}')
        WITH p
        ORDER BY p.year DESC
        RETURN p.title as title, p.year as year, p.author as author, p.abstract as abstract
        """
    else:
        papers_query = """
        MATCH (p:Paper)
        WITH p
        ORDER BY p.year DESC
        RETURN p.title as title, p.year as year, p.author as author, p.abstract as abstract
        """

    papers_data = execute_spark_query(papers_query).head(limit)

    if not papers_data.empty:
        st.success(f"Found {len(papers_data)} papers")

        for i, row in papers_data.iterrows():
            with st.expander(
                f"📄 {row.get('title', 'Untitled')} ({row.get('year', 'N/A')})"
            ):
                col1, col2 = st.columns([2, 1])

                with col1:
                    if row.get("abstract"):
                        st.write("**Abstract:**")
                        st.write(row["abstract"])
                    else:
                        st.write("*No abstract available*")

                with col2:
                    if row.get("author"):
                        st.write(f"**Author:** {row['author']}")
                    if row.get("year"):
                        st.write(f"**Year:** {row['year']}")
    else:
        if search_term:
            st.info(f"No papers found matching '{search_term}'")
        else:
            st.info("No papers found in the database.")


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
    WITH person, count(volume) as volume_count
    ORDER BY volume_count DESC
    WITH person.name as name, volume_count
    RETURN name, volume_count
    """

    editors_data = execute_spark_query(editors_query)

    if not editors_data.empty:
        df_editors = pd.DataFrame(editors_data)
        st.dataframe(df_editors, use_container_width=True)


# Header
st.title("📚 Research Paper Network Explorer")

# Sidebar
sidebar()

# # Main content tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["🏠 Dashboard", "📄 Papers", "👥 People", "🌐 Networks", "🔍 Query"]
)

with tab1:
    tab1_overlay()

with tab2:
    tab2_overlay()

with tab3:
    tab3_overlay()
