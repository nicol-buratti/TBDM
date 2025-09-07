from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr
import pandas as pd
from functionalities.utils import create_graph


def similarity(
    spark: SparkSession,
    graph_name: str,
    df_graph: DataFrame,
    SIMILARITY_THRESHOLD: float = 0.05,
) -> DataFrame:
    create_graph(spark, graph_name)
    if isinstance(df_graph, pd.DataFrame):
        df_graph = spark.createDataFrame(df_graph)
    df_ids = (
        df_graph.select(col("n.<id>").alias("id"))
        .union(df_graph.select(col("m.<id>").alias("id")))
        .distinct()
    )

    fastRP_df = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("gds", "gds.fastRP.stream")
        .option("gds.graphName", graph_name)
        .option("gds.configuration.embeddingDimension", "64")
        .option("gds.configuration.randomSeed", "42")
        .load()
        .join(df_ids, col("nodeId") == df_ids.id, "inner")
    )

    return (
        fastRP_df.alias("a")
        .crossJoin(fastRP_df.alias("b"))
        .select(
            col("a.nodeId").alias("nodeId1"),
            col("b.nodeId").alias("nodeId2"),
            col("a.embedding").alias("f1_arr"),
            col("b.embedding").alias("f2_arr"),
        )
        .where(col("a.nodeId") < col("b.nodeId"))
        .withColumn(
            "features_diff",
            expr(
                "transform(arrays_zip(f1_arr, f2_arr), x -> sqrt(pow(x.f1_arr - x.f2_arr, 2)))"
            ),
        )
        .withColumn(
            "similarity",
            expr("aggregate(features_diff, 0D, (acc, x) -> acc + abs(x))"),
        )
        .where(col("similarity") <= SIMILARITY_THRESHOLD)
        .select("nodeId1", "nodeId2", "similarity")
    )


def similarity_gds(
    spark: SparkSession,
    graph_name: str,
    SIMILARITY_THRESHOLD: float = 0.95,
) -> DataFrame:
    create_graph(spark, graph_name)
    return (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("gds", "gds.nodeSimilarity.stream")
        .option("gds.graphName", graph_name)
        .option("gds.configuration.similarityCutoff", str(SIMILARITY_THRESHOLD))
        .load()
        .select(
            col("node1").alias("nodeId1"),
            col("node2").alias("nodeId2"),
            col("similarity"),
        )
    )
