from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, col, lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pathlib import Path

load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")

volume_param = [
    "volnr",
    "title",
    "pubyear", 
    "volacronym",
    "voltitle",
    "fulltitle",
    "loctime",
]

# Add year mapping for papers
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
    """Create CONTAINS relationship from Volume to Paper"""
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
        col("pubyear").alias("year")  # Add year to papers from volume
    )

    # Map paper title correctly
    p_param = ",".join(paper_param).replace("title", "paper_title:title")
    
    print(f"Creating CONTAINS relationships for {volume_papers.count()} volume-paper pairs...")

    (
        volume_papers.write
        .mode("Overwrite")
        .format("org.neo4j.spark.DataSource")
        .option("relationship", "CONTAINS")
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":Volume")
        .option("relationship.source.node.properties", ",".join(volume_param))
        .option("relationship.source.node.keys", "volnr")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Paper")
        .option("relationship.target.node.properties", p_param)
        .option("relationship.target.node.keys", "url")
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
    
    print(f"Creating EDITED relationships for {volume_editor.count()} editor-volume pairs...")

    (
        volume_editor.write
        .mode("Overwrite")
        .format("org.neo4j.spark.DataSource")
        .option("relationship", "EDITED")  # Changed from EDITOR to EDITED
        .option("relationship.save.strategy", "keys")
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
    
    print(f"Creating AUTHORED relationships for {papers_authors.count()} author-paper pairs...")

    (
        papers_authors.write
        .mode("Overwrite")
        .format("org.neo4j.spark.DataSource")
        .option("relationship", "AUTHORED")  # Changed from AUTHOR to AUTHORED
        .option("relationship.save.strategy", "keys")
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
    
    print(f"Creating HAS_KEYWORD relationships for {papers_keywords.count()} paper-keyword pairs...")

    (
        papers_keywords.write
        .mode("Overwrite")
        .format("org.neo4j.spark.DataSource")
        .option("relationship", "HAS_KEYWORD")  # Changed from KEYWORD to HAS_KEYWORD
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":Paper")
        .option("relationship.source.node.properties", ",".join(paper_param))
        .option("relationship.source.node.keys", "url")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Keyword")
        .option("relationship.target.node.properties", "keyword:name")
        .option("relationship.target.node.keys", "keyword:name")
        .option("relationship.properties", "")
        .save()
    )
    
    print("✅ HAS_KEYWORD relationships created")


def create_belongs_to_relationship(df: DataFrame):
    """Create BELONGS_TO relationship from Paper to Volume (reverse of CONTAINS)"""
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
        col("pubyear").alias("year")
    )

    p_param = ",".join(paper_param).replace("title", "paper_title:title")
    
    print(f"Creating BELONGS_TO relationships for {volume_papers.count()} paper-volume pairs...")

    (
        volume_papers.write
        .mode("Append")  # Use Append to avoid conflicts with CONTAINS
        .format("org.neo4j.spark.DataSource")
        .option("relationship", "BELONGS_TO")
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.save.mode", "Match")  # Match existing papers
        .option("relationship.source.labels", ":Paper")
        .option("relationship.source.node.properties", p_param)
        .option("relationship.source.node.keys", "url")
        .option("relationship.target.save.mode", "Match")  # Match existing volumes
        .option("relationship.target.labels", ":Volume")
        .option("relationship.target.node.properties", ",".join(volume_param))
        .option("relationship.target.node.keys", "volnr")
        .option("relationship.properties", "")
        .save()
    )
    
    print("✅ BELONGS_TO relationships created")


def volume_relationships(df: DataFrame):
    """Create all volume-related relationships"""
    print("🔗 Creating volume relationships...")
    create_contains_relationship(df)
    create_editor_relationship(df)
    create_belongs_to_relationship(df)


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
        col("pubyear").alias("year")  # Add year from volume
    )
    
    create_author_relationship(papers)
    create_keyword_relationship(papers)


def verify_data_injection(spark: SparkSession):
    """Verify that data was properly injected"""
    print("🔍 Verifying data injection...")
    
    try:
        # Check nodes
        volume_count = spark.read.format("org.neo4j.spark.DataSource").option("query", "MATCH (v:Volume) RETURN count(v) as count").load().collect()[0]['count']
        paper_count = spark.read.format("org.neo4j.spark.DataSource").option("query", "MATCH (p:Paper) RETURN count(p) as count").load().collect()[0]['count']
        person_count = spark.read.format("org.neo4j.spark.DataSource").option("query", "MATCH (p:Person) RETURN count(p) as count").load().collect()[0]['count']
        keyword_count = spark.read.format("org.neo4j.spark.DataSource").option("query", "MATCH (k:Keyword) RETURN count(k) as count").load().collect()[0]['count']
        
        print(f"📊 Data Summary:")
        print(f"   Volumes: {volume_count}")
        print(f"   Papers: {paper_count}")
        print(f"   People: {person_count}")
        print(f"   Keywords: {keyword_count}")
        
        # Check relationships
        authored_count = spark.read.format("org.neo4j.spark.DataSource").option("query", "MATCH ()-[r:AUTHORED]->() RETURN count(r) as count").load().collect()[0]['count']
        edited_count = spark.read.format("org.neo4j.spark.DataSource").option("query", "MATCH ()-[r:EDITED]->() RETURN count(r) as count").load().collect()[0]['count']
        contains_count = spark.read.format("org.neo4j.spark.DataSource").option("query", "MATCH ()-[r:CONTAINS]->() RETURN count(r) as count").load().collect()[0]['count']
        belongs_count = spark.read.format("org.neo4j.spark.DataSource").option("query", "MATCH ()-[r:BELONGS_TO]->() RETURN count(r) as count").load().collect()[0]['count']
        keyword_rel_count = spark.read.format("org.neo4j.spark.DataSource").option("query", "MATCH ()-[r:HAS_KEYWORD]->() RETURN count(r) as count").load().collect()[0]['count']
        
        print(f"🔗 Relationships:")
        print(f"   AUTHORED: {authored_count}")
        print(f"   EDITED: {edited_count}")
        print(f"   CONTAINS: {contains_count}")
        print(f"   BELONGS_TO: {belongs_count}")
        print(f"   HAS_KEYWORD: {keyword_rel_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False


def main(spark: SparkSession, volumes_to_inject=-1):
    """Main injection function"""
    print("🚀 Starting Neo4j data injection...")
    
    # Determine data path - check both possible locations
    data_paths = [
        Path("../data/Volumes"),
        Path("./data/Volumes"),
        Path("/app/data/Volumes"),
        Path("data/Volumes")
    ]
    
    json_dir = None
    for path in data_paths:
        if path.exists():
            json_dir = str(path)
            print(f"📁 Found data directory: {json_dir}")
            break
    
    if not json_dir:
        print("❌ Could not find data directory. Tried:")
        for path in data_paths:
            print(f"   - {path}")
        return False
    
    # Check if directory has JSON files
    json_files = list(Path(json_dir).glob("*.json"))
    if not json_files:
        print(f"❌ No JSON files found in {json_dir}")
        return False
    
    print(f"📄 Found {len(json_files)} JSON files")

    try:
        # Read all JSON files in the directory
        df = spark.read.schema(schema).option("multiline", "true").json(json_dir)
        
        total_volumes = df.count()
        print(f"📚 Loaded {total_volumes} volumes")
        
        if total_volumes == 0:
            print("❌ No data loaded from JSON files")
            return False
        
        if volumes_to_inject > 0:
            df = df.limit(volumes_to_inject)
            print(f"🔢 Processing {volumes_to_inject} volumes (limited)")

        # Create relationships
        volume_relationships(df)
        papers_relationships(df)
        
        # Verify injection
        success = verify_data_injection(spark)
        
        if success:
            print("✅ Data injection completed successfully!")
            print("🌐 Your data is now available in Neo4j and ready for Streamlit!")
        else:
            print("⚠️  Data injection completed but verification failed")
        
        return success
        
    except Exception as e:
        print(f"❌ Data injection failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("🔗 Neo4j Data Injection Script")
    print("=" * 50)
    
    # Create Spark session with Neo4j connector
    spark = (
        SparkSession.builder
        .appName("JsonToNeo4jInjection")
        .master("spark://spark:7077")
        .config("spark.jars.packages", "neo4j-contrib:neo4j-spark-connector:5.3.1-s_2.12")
        .config("neo4j.url", NEO4J_URI)
        .config("neo4j.authentication.basic.username", "neo4j")
        .config("neo4j.authentication.basic.password", "password")
        .config("neo4j.database", "neo4j")
        .config("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .getOrCreate()
    )
    
    try:
        # Test Neo4j connectivity
        test_df = spark.read.format("org.neo4j.spark.DataSource").option("query", "RETURN 1 as test").load()
        test_result = test_df.collect()[0]['test']
        if test_result == 1:
            print("✅ Neo4j connection successful")
        else:
            print("❌ Neo4j connection test failed")
            exit(1)
    except Exception as e:
        print(f"❌ Cannot connect to Neo4j: {e}")
        exit(1)

    # Run main injection with limited volumes for testing
    success = main(spark, volumes_to_inject=5)  # Process 5 volumes for testing
    
    if success:
        print("\n🎉 All done! Your Streamlit app should now show data.")
        print("   Visit: http://localhost:8501")
    else:
        print("\n❌ Injection failed. Check the logs above for details.")
    
    spark.stop()