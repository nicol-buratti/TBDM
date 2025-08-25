from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pathlib import Path

load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI")
VOLUMES = int(os.getenv("VOLUMES", -1))

volume_param = [
    "volnr",
    "title",
    "pubyear",
    "volacronym",
    "voltitle",
    "fulltitle",
    "loctime",
]
paper_param = ["url", "abstract", "title", "pages", "year"]

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
        col("pubyear").alias("year"),
    )

    # Map paper title correctly
    p_param = ",".join(paper_param).replace("title", "paper_title:title")

    print(
        f"Creating CONTAINS relationships for {volume_papers.count()} volume-paper pairs..."
    )

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

    print("✅ CONTAINS relationships created")


def create_editor_relationship(df: DataFrame):
    """Create EDITED relationship from Person to Volume"""
    volume_editor = (
        df.withColumn("voleditorname", explode("voleditors"))
        .drop("voleditors", "papers")
        .dropna(subset=["voleditorname"])
    )

    print(
        f"Creating EDITED relationships for {volume_editor.count()} editor-volume pairs..."
    )

    (
        volume_editor.write
        # Overwrite relationships
        .mode("Overwrite")
        .format("org.neo4j.spark.DataSource")
        .option("relationship", "EDITED")
        .option("relationship.save.strategy", "keys")
        # Overwrite source nodes and assign them a label
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":Person")
        .option("relationship.source.node.properties", "voleditorname:name")
        .option("relationship.source.node.keys", "voleditorname:name")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Volume")
        .option("relationship.target.node.properties", ",".join(volume_param))
        .option("relationship.target.node.keys", "volnr")
        .option("relationship.properties", "")
        .save()
    )

    print("✅ EDITED relationships created")


def create_author_relationship(papers: DataFrame):
    """Create AUTHORED relationship from Person to Paper"""
    papers_authors = (
        papers.withColumn("authorname", explode("authors"))
        .drop("authors", "keywords")
        .dropna(subset=["authorname"])
    )

    print(
        f"Creating AUTHORED relationships for {papers_authors.count()} author-paper pairs..."
    )

    (
        papers_authors.write
        # Overwrite relationships
        .mode("Overwrite")
        .format("org.neo4j.spark.DataSource")
        .option("relationship", "AUTHORED")  # Changed from AUTHOR to AUTHORED
        .option("relationship.save.strategy", "keys")
        # Overwrite source nodes and assign them a label
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":Person")
        .option("relationship.source.node.properties", "authorname:name")
        .option("relationship.source.node.keys", "authorname:name")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Paper")
        .option("relationship.target.node.properties", ",".join(paper_param))
        .option("relationship.target.node.keys", "url")
        .option("relationship.properties", "")
        .save()
    )

    print("✅ AUTHORED relationships created")


def create_keyword_relationship(papers: DataFrame):
    """Create HAS_KEYWORD relationship from Paper to Keyword"""
    papers_keywords = (
        papers.withColumn("keyword", explode("keywords"))
        .drop("authors", "keywords")
        .dropna(subset=["keyword"])
    )

    print(
        f"Creating HAS_KEYWORD relationships for {papers_keywords.count()} paper-keyword pairs..."
    )

    (
        papers_keywords.write
        # Overwrite relationships
        .mode("Overwrite")
        .format("org.neo4j.spark.DataSource")
        .option("relationship", "HAS_KEYWORD")  # Changed from KEYWORD to HAS_KEYWORD
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
    """Create all volume-related relationships"""
    print("🔗 Creating volume relationships...")
    create_contains_relationship(df)
    create_editor_relationship(df)


def papers_relationships(df: DataFrame):
    """Create all paper-related relationships"""
    print("🔗 Creating paper relationships...")

    papers = df.withColumn("paper", explode("papers")).select(
        col("paper.authors").alias("authors"),
        col("paper.keywords").alias("keywords"),
        col("paper.url").alias("url"),
        col("paper.title").alias("title"),
        col("paper.pages").alias("pages"),
        col("paper.abstract").alias("abstract"),
        col("pubyear").alias("year"),
    )
    create_author_relationship(papers)
    create_keyword_relationship(papers)


def main(spark: SparkSession, volumes_to_inject):
    json_dir = Path("./data/Volumes").__str__()

    # Read all JSON files in the directory
    df = spark.read.schema(schema).option("multiline", "true").json(json_dir)
    if volumes_to_inject > 0:
        df = df.limit(volumes_to_inject)

    volume_relationships(df)
    papers_relationships(df)


if __name__ == "__main__":
    print("🔗 Neo4j Data Injection Script")
    print("=" * 50)

    # Create Spark session with Neo4j connector
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

    main(spark, VOLUMES)
