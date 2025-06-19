from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.sql.functions import col
from pyspark.ml.functions import array_to_vector
from pyspark.sql.functions import desc

from functionalities.utils import create_graph


def similarity(spark: SparkSession, graph_name: str, node: str) -> DataFrame:
    create_graph(spark, graph_name)
    fastRP_df = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("gds", "gds.fastRP.stream")
        .option("gds.graphName", graph_name)
        .option("gds.configuration.embeddingDimension", "64")
        .option("gds.configuration.randomSeed", "42")
        .load()
    )
    # Convert list of floats to Spark Vectors
    df = fastRP_df.withColumn("features", array_to_vector(col("embedding")))

    lsh = BucketedRandomProjectionLSH(
        inputCol="features", outputCol="hashes", bucketLength=1.0, numHashTables=3
    )
    model = lsh.fit(df)
    similar_items = (
        model.approxSimilarityJoin(df, df, threshold=0.05)
        .select(
            col("datasetA.nodeId").alias("node1"),
            col("datasetB.nodeId").alias("node2"),
            "distCol",
        )
        .orderBy(desc("distCol"))
    )
    nodes = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("labels", f":{node}")
        .load()
        .select(col("<id>").alias("nodeId"))
    )

    # Alias your DataFrames for clarity
    keywords1 = nodes.alias("k1")
    keywords2 = nodes.alias("k2")
    sim_items = similar_items.alias("s")

    # Join once for node1 and once for node2 with clear aliases
    result = (
        sim_items.join(keywords1, col("s.node1") == col("k1.nodeId"))
        .join(keywords2, col("s.node2") == col("k2.nodeId"))
        .select(
            col("k1.nodeId").alias("node1"),
            col("k2.nodeId").alias("node2"),
            col("s.distCol"),
        )
    )
    return result
