from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import collect_list, size

from functionalities.utils import create_graph


def link_prediction(
    spark: SparkSession, node:str, id1: int, id2: int, algorithm = "commonNeighbors"
) -> DataFrame:
    prediction_score = (
    spark.read.format("org.neo4j.spark.DataSource")
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
