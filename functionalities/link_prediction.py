from pyspark.sql import SparkSession, DataFrame
import os

NEO4J_URI = os.getenv("NEO4J_URI")


def link_prediction(
    spark: SparkSession, node: str, id1: int, id2: int, algorithm="commonNeighbors"
) -> DataFrame:
    prediction_score = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("url", NEO4J_URI if NEO4J_URI else "bolt://neo4j:7687")
        .option("authentication.type", "basic")
        .option("authentication.basic.username", "neo4j")
        .option("authentication.basic.password", "password")
        .option(
            "query",
            f"""
        MATCH (p1:{node}) WHERE id(p1) = {id1}
        MATCH (p2:{node}) WHERE id(p2) = {id2}
        RETURN gds.alpha.linkprediction.{algorithm}(p1, p2) AS score
        """,
        )
        .option("partitions", "1")
        .load()
        .first()["score"]
    )
    return prediction_score
