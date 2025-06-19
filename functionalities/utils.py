from pyspark.sql import SparkSession


def create_graph(spark: SparkSession, graph_name: str):
    graph_exists = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("gds", "gds.graph.exists")
        .option("gds.graphName", graph_name)
        .load()
        .first()["exists"]
    )
    if not graph_exists:
        (
            spark.read.format("org.neo4j.spark.DataSource")
            .option("gds", "gds.graph.project")
            .option("gds.graphName", graph_name)
            .option("gds.nodeProjection", ["Keyword", "Paper", "Volume"])
            .option(
                "gds.relationshipProjection",
                """
            {
            "KEYWORD": {"orientation": "UNDIRECTED"},
            "CONTAINS": {"orientation": "UNDIRECTED"}
            }
            """,
            )
            .load()
        )
