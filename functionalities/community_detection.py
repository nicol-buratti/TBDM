from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import collect_list, size
import os
from functionalities.utils import create_graph

NEO4J_URI = os.getenv("NEO4J_URI")


def get_spark_df_communities(
    spark: SparkSession, community_algorithm: str, graph_name: str
) -> DataFrame:
    create_graph(spark, graph_name)
    community_df = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("url", NEO4J_URI if NEO4J_URI else "bolt://neo4j:7687")
        .option("authentication.type", "basic")
        .option("authentication.basic.username", "neo4j")
        .option("authentication.basic.password", "password")
        .option("gds", f"gds.{community_algorithm}.stream")
        .option("gds.graphName", graph_name)
        .option("gds.configuration.maxIterations", "1000")  # Max num of iterations
        .option("gds.configuration.minCommunitySize", "5")  # Remove small communities
        .option("gds.configuration.consecutiveIds", True)
        .load()
        .groupBy("communityId")
        .agg(collect_list("nodeId").alias("nodeIds"))
        .withColumn("item_count", size("nodeIds"))
        .orderBy("item_count", ascending=False)
    )

    return community_df
