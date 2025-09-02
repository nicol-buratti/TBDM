import streamlit as st
import pandas as pd
import plotly.express as px
import os
import logging
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

    # Fixed query - Papers get their year from the Volume they belong to
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
        
        # Create a more sophisticated plot using plotly
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
        
        # Additional statistics
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
    st.markdown("## Example")
    st_link_analysis(elements, "cose", node_styles, edge_styles)

    df_community, community_elements = display_community(node_styles, edge_styles)

    display_link_prediction(node_styles, edge_styles, community_elements)

    st.markdown("## Similarity")
    df_similarity = get_similarity(st.session_state.spark.createDataFrame(df_community))
    edges = []
    maxx = max(community_elements["edges"], key=lambda x: x["data"]["id"])["data"]["id"]
    for index, row in df_similarity.iterrows():
        maxx += 1
        edge = {}
        edge["id"] = maxx
        edge["label"] = "SIMILAR"
        edge["source"] = row["nodeId1"]
        edge["target"] = row["nodeId2"]
        edge["similarity"] = 1.0 - row["features_diff_sum"]

        edges.append({"data": edge})
    community_elements["edges"].extend(edges)
    st_link_analysis(community_elements, "cose", node_styles, edge_styles)


def display_community(node_styles, edge_styles):
    df_community = get_community_detection_df_graph(NODE_ID)
    community_elements = transform_df_to_graph_elements(df_community)
    st.markdown("## Community")
    st_link_analysis(community_elements, "cose", node_styles, edge_styles)
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

    maxx = max(community_elements["edges"], key=lambda x: x["data"]["id"])["data"]["id"]
    for p in predictions.collect():
        if p["p1"]["<id>"] != p["p2"]["<id>"] and p["p1"]["<id>"] > p["p2"]["<id>"]:
            maxx += 1
            edge = {}
            edge["id"] = maxx
            edge["label"] = "POSSIBLY_RELATED"
            edge["source"] = p["p1"]["<id>"]
            edge["target"] = p["p2"]["<id>"]
            edge["target"] = p["p2"]["<id>"]
            community_elements["edges"].append({"data": edge})
    st.markdown("## Link Prediction")
    st_link_analysis(
        community_elements, "cose", node_styles, edge_styles, key="POSSIBLY_RELATED"
    )


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