from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pathlib import Path

load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI")

volume_param = [
    "volnr",
    "title",
    "pubyear",
    "volacronym",
    "voltitle",
    "fulltitle",
    "loctime",
]
paper_param = ["url", "abstract", "title", "pages"]

person_param = "name"
keyword_param = "name"

schema = StructType(
    [
        StructField("title", StringType()),
        StructField("volnr", StringType()),
        StructField("pubyear", StringType()),
        StructField("volacronym", StringType()),
        StructField("voltitle", StringType()),
        StructField("fulltitle", StringType()),
        StructField("loctime", StringType()),
        StructField("voleditors", ArrayType(StringType())),
        StructField(
            "papers",
            ArrayType(
                StructType(
                    [
                        StructField("authors", ArrayType(StringType())),
                        StructField("keywords", ArrayType(StringType())),
                        StructField("url", StringType()),
                        StructField("title", StringType()),
                        StructField("pages", StringType()),
                        StructField("abstract", StringType()),
                    ]
                )
            ),
        ),
    ]
)


def create_contains_relationship(df: DataFrame):
    volume_papers = df.withColumn("paper", explode("papers")).select(
        "title",
        "volnr",
        "pubyear",
        "volacronym",
        "voltitle",
        "fulltitle",
        "loctime",
        col("paper.url").alias("url"),
        col("paper.title").alias("paper_title"),
        col("paper.pages").alias("pages"),
        col("paper.abstract").alias("abstract"),
    )

    p_param = ",".join(paper_param).replace("title", "paper_title:title", 1)

    (
        volume_papers.write
        # Overwrite relationships
        .mode("Overwrite")
        .format("org.neo4j.spark.DataSource")
        # Assign a type to the relationships
        .option("relationship", "CONTAINS")
        # Use `keys` strategy
        .option("relationship.save.strategy", "keys")
        # Overwrite source nodes and assign them a label
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":Volume")
        # Map the DataFrame columns to node properties
        .option("relationship.source.node.properties", ",".join(volume_param))
        # Node keys are mandatory for overwrite save mode
        .option("relationship.source.node.keys", "volnr")
        # Overwrite target nodes and assign them a label
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Paper")
        # Map the DataFrame columns to node properties
        .option("relationship.target.node.properties", p_param)
        # Node keys are mandatory for overwrite save mode
        .option("relationship.target.node.keys", "url")
        # Map the DataFrame columns to relationship properties
        .option("relationship.properties", "")
        .save()
    )


def create_editor_relationship(df: DataFrame):
    volume_editor = (
        df.withColumn("voleditorname", explode("voleditors"))
        .drop("voleditors", "papers")
        .dropna(subset=["voleditorname"])
    )

    (
        volume_editor.write
        # Overwrite relationships
        .mode("Overwrite")
        .format("org.neo4j.spark.DataSource")
        # Assign a type to the relationships
        .option("relationship", "EDITOR")
        # Use `keys` strategy
        .option("relationship.save.strategy", "keys")
        # Overwrite source nodes and assign them a label
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":Volume")
        # Map the DataFrame columns to node properties
        .option("relationship.source.node.properties", ",".join(volume_param))
        # Node keys are mandatory for overwrite save mode
        .option("relationship.source.node.keys", "volnr")
        # Overwrite target nodes and assign them a label
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Person")
        # Map the DataFrame columns to node properties
        .option("relationship.target.node.properties", "voleditorname:name")
        # Node keys are mandatory for overwrite save mode
        .option("relationship.target.node.keys", "voleditorname:name")
        # Map the DataFrame columns to relationship properties
        .option("relationship.properties", "")
        .save()
    )


def create_author_relationship(papers: DataFrame):
    papers_authors = (
        papers.withColumn("authorname", explode("authors"))
        .drop("authors", "keywords")
        .dropna(subset=["authorname"])
    )

    (
        papers_authors.write
        # Overwrite relationships
        .mode("Overwrite")
        .format("org.neo4j.spark.DataSource")
        # Assign a type to the relationships
        .option("relationship", "AUTHOR")
        # Use `keys` strategy
        .option("relationship.save.strategy", "keys")
        # Overwrite source nodes and assign them a label
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":Paper")
        # Map the DataFrame columns to node properties
        .option("relationship.source.node.properties", ",".join(paper_param))
        # Node keys are mandatory for overwrite save mode
        .option("relationship.source.node.keys", "url")
        # Overwrite target nodes and assign them a label
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Person")
        # Map the DataFrame columns to node properties
        .option("relationship.target.node.properties", "authorname:name")
        # Node keys are mandatory for overwrite save mode
        .option("relationship.target.node.keys", "authorname:name")
        # Map the DataFrame columns to relationship properties
        .option("relationship.properties", "")
        .save()
    )


def create_keyword_relationship(papers: DataFrame):
    papers_authors = (
        papers.withColumn("keyword", explode("keywords"))
        .drop("authors", "keywords")
        .dropna(subset=["keyword"])
    )

    (
        papers_authors.write
        # Overwrite relationships
        .mode("Overwrite")
        .format("org.neo4j.spark.DataSource")
        # Assign a type to the relationships
        .option("relationship", "KEYWORD")
        # Use `keys` strategy
        .option("relationship.save.strategy", "keys")
        # Overwrite source nodes and assign them a label
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":Paper")
        # Map the DataFrame columns to node properties
        .option("relationship.source.node.properties", ",".join(paper_param))
        # Node keys are mandatory for overwrite save mode
        .option("relationship.source.node.keys", "url")
        # Overwrite target nodes and assign them a label
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Keyword")
        # Map the DataFrame columns to node properties
        .option("relationship.target.node.properties", "keyword:name")
        # Node keys are mandatory for overwrite save mode
        .option("relationship.target.node.keys", "keyword:name")
        # Map the DataFrame columns to relationship properties
        .option("relationship.properties", "")
        .save()
    )


def volume_relationships(df: DataFrame):
    create_contains_relationship(df)
    create_editor_relationship(df)


def papers_relationships(df: DataFrame):
    papers = df.withColumn("paper", explode("papers")).select(
        col("paper.authors").alias("authors"),
        col("paper.keywords").alias("keywords"),
        col("paper.url").alias("url"),
        col("paper.title").alias("title"),
        col("paper.pages").alias("pages"),
        col("paper.abstract").alias("abstract"),
    )
    create_author_relationship(papers)
    create_keyword_relationship(papers)


def main(spark: SparkSession, volumes_to_inject=-1):
    json_dir = Path("../data/Volumes").__str__()

    # Read all JSON files in the directory
    df = spark.read.schema(schema).option("multiline", "true").json(json_dir)
    if volumes_to_inject > 0:
        df = df.limit(volumes_to_inject)

    volume_relationships(df)
    papers_relationships(df)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("JsonToNeo4jInjection")
        .master("spark://spark:7077")
        .config(
            "spark.jars.packages", "neo4j-contrib:neo4j-spark-connector:5.3.1-s_2.12"
        )
        .config("neo4j.url", NEO4J_URI)
        .config("neo4j.authentication.basic.username", "neo4j")
        .config("neo4j.authentication.basic.password", "password")
        .config("neo4j.database", "neo4j")
        .getOrCreate()
    )

    main(spark, 1)
