from pyspark.sql import SparkSession


def link_prediction(
    spark: SparkSession, node: str, id1: int, id2: int, algorithm="commonNeighbors"
) -> float:
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


def bulk_link_prediction(
    spark: SparkSession, node: str, ids: list[int], algorithm="adamicAdar"
) -> float:
    prediction_score = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option(
            "query",
            f"""
        MATCH (p1:{node}) WHERE id(p1) in {ids}
        MATCH (p2:{node}) WHERE id(p2) in {ids}
        RETURN p1, p2, gds.alpha.linkprediction.{algorithm}(p1, p2) AS score
        """,
        )
        .option("partitions", "1")
        .load()
    )
    return prediction_score
