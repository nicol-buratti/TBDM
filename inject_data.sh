#!/bin/bash

# inject_data.sh - Manual data injection script with volume limiting

set -e

echo "Manual Data Injection for Research Paper Network Explorer"
echo "========================================================="
echo ""

# Default number of volumes to inject (can be overridden)
VOLUMES_TO_INJECT=${1:-5}  # Default to 5 volumes if not specified

# Function to print colored output
print_step() {
    echo -e "\n🔵 $1"
}

print_success() {
    echo -e "✅ $1"
}

print_error() {
    echo -e "❌ $1"
}

print_warning() {
    echo -e "⚠️ $1"
}

# Check Docker Compose command
DOCKER_COMPOSE_CMD=""
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker compose"
else
    print_error "Docker Compose is not available."
    exit 1
fi

# Check prerequisites
print_step "Checking prerequisites..."

# Check if services are running
if ! docker ps | grep -q "neo4j"; then
    print_error "Neo4j is not running. Please run ./setup.sh first."
    exit 1
fi

if ! docker ps | grep -q "spark"; then
    print_error "Spark is not running. Please run ./setup.sh first."
    exit 1
fi

print_success "Services are running"

# Check for data files
print_step "Checking for data files..."
if [ -d "data/Volumes" ] && [ "$(ls -A data/Volumes/*.json 2>/dev/null)" ]; then
    JSON_FILE_COUNT=$(ls data/Volumes/*.json 2>/dev/null | wc -l)
    print_success "Found $JSON_FILE_COUNT JSON files in data/Volumes/"
else
    print_error "No JSON files found in data/Volumes/"
    echo "Please place your JSON volume files in data/Volumes/ directory"
    exit 1
fi

# Check for write_neo4j.py
print_step "Checking for write_neo4j.py..."
if [ ! -f "data-injection/write_neo4j.py" ]; then
    print_error "write_neo4j.py not found in data-injection folder"
    exit 1
fi
print_success "Found write_neo4j.py"

# Check if data already exists
print_step "Checking existing data in Neo4j..."
EXISTING_PAPERS=$(docker exec $(docker ps -q -f "name=neo4j") \
    cypher-shell -u neo4j -p password \
    "MATCH (p:Paper) RETURN count(p) as count" \
    --format plain 2>/dev/null | grep -o '[0-9]*' | head -1)

if [ ! -z "$EXISTING_PAPERS" ] && [ "$EXISTING_PAPERS" -gt 0 ]; then
    print_warning "Found $EXISTING_PAPERS existing papers in Neo4j"
    read -p "Do you want to replace existing data? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_warning "Injection cancelled by user"
        exit 0
    fi
    
    print_step "Clearing existing data..."
    docker exec $(docker ps -q -f "name=neo4j") \
        cypher-shell -u neo4j -p password \
        "MATCH (n) DETACH DELETE n" 2>/dev/null
    print_success "Existing data cleared"
fi

# Ask user how many volumes to inject
print_step "Data volume selection"
echo "You have $JSON_FILE_COUNT JSON files available."
echo "How many volumes do you want to inject? (Each volume typically contains 3-10 papers)"
echo "Enter a number (or press Enter for default of $VOLUMES_TO_INJECT volumes):"
read -r USER_VOLUMES
if [ ! -z "$USER_VOLUMES" ]; then
    VOLUMES_TO_INJECT=$USER_VOLUMES
fi
print_success "Will inject $VOLUMES_TO_INJECT volumes"

# Perform data injection
print_step "Starting data injection..."
echo "This may take a few minutes..."

# Get Spark container ID
SPARK_CONTAINER=$(docker ps -q -f "name=spark" -f "status=running" | head -1)

if [ -z "$SPARK_CONTAINER" ]; then
    print_error "Spark container not found or not running"
    exit 1
fi

# Copy the data directory to Spark container
print_step "Copying data to Spark container..."
docker exec $SPARK_CONTAINER mkdir -p /tmp/data 2>/dev/null || true
docker cp data/Volumes $SPARK_CONTAINER:/tmp/data/
print_success "Data copied to container"

# Copy the write_neo4j.py script
print_step "Copying injection script..."
docker cp data-injection/write_neo4j.py $SPARK_CONTAINER:/tmp/write_neo4j.py

# First install required Python packages in the Spark container
print_step "Installing required Python packages..."
docker exec $SPARK_CONTAINER pip install --user python-dotenv py4j 2>/dev/null || true

# Create a wrapper script that calls write_neo4j with limited volumes and correct paths
print_step "Creating injection wrapper..."
cat > /tmp/write_neo4j_wrapper.py << 'EOF'
import os
import sys
from pathlib import Path

sys.path.insert(0, '/tmp')

# Set environment variable for Neo4j
os.environ["NEO4J_URI"] = "bolt://neo4j:7687"

from pyspark.sql import SparkSession

# Create Spark session
spark = (
    SparkSession.builder
    .appName("LimitedJsonToNeo4jInjection")
    .master("spark://spark:7077")
    .config("spark.jars.packages", "neo4j-contrib:neo4j-spark-connector:5.3.1-s_2.12")
    .config("neo4j.url", "bolt://neo4j:7687")
    .config("neo4j.authentication.basic.username", "neo4j")
    .config("neo4j.authentication.basic.password", "password")
    .config("neo4j.database", "neo4j")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .getOrCreate()
)

# Import write_neo4j and modify its data paths
import write_neo4j

# Override the main function to use the correct data path
original_main = write_neo4j.main

def patched_main(spark_session, volumes_to_inject=-1):
    # Update data_paths to look in the right place
    import write_neo4j
    # Monkey-patch the data paths in the module
    data_paths = [
        Path("/tmp/data/Volumes"),
        Path("/home/jovyan/data/Volumes"),
        Path("../data/Volumes"),
        Path("./data/Volumes"),
        Path("/app/data/Volumes"),
        Path("data/Volumes")
    ]
    
    # Find the correct path
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
    
    # Now run the original function with the data
    from pyspark.sql.types import StructType, StructField, StringType, ArrayType
    
    schema = StructType([
        StructField("title", StringType()),
        StructField("volnr", StringType()),
        StructField("pubyear", StringType()),
        StructField("volacronym", StringType()),
        StructField("voltitle", StringType()),
        StructField("fulltitle", StringType()),
        StructField("loctime", StringType()),
        StructField("voleditors", ArrayType(StringType())),
        StructField("papers", ArrayType(
            StructType([
                StructField("authors", ArrayType(StringType())),
                StructField("keywords", ArrayType(StringType())),
                StructField("url", StringType()),
                StructField("title", StringType()),
                StructField("pages", StringType()),
                StructField("abstract", StringType()),
            ])
        )),
    ])
    
    try:
        # Read all JSON files in the directory
        df = spark_session.read.schema(schema).option("multiline", "true").json(json_dir)
        
        total_volumes = df.count()
        print(f"📚 Loaded {total_volumes} volumes")
        
        if total_volumes == 0:
            print("❌ No data loaded from JSON files")
            return False
        
        if volumes_to_inject > 0:
            df = df.limit(volumes_to_inject)
            print(f"📢 Processing {volumes_to_inject} volumes (limited)")
        
        # Create relationships using write_neo4j functions
        write_neo4j.volume_relationships(df)
        write_neo4j.papers_relationships(df)
        
        # Verify injection
        success = write_neo4j.verify_data_injection(spark_session)
        
        if success:
            print("✅ Data injection completed successfully!")
        else:
            print("⚠️ Data injection completed but verification failed")
        
        return success
        
    except Exception as e:
        print(f"❌ Data injection failed: {e}")
        import traceback
        traceback.print_exc()
        return False

write_neo4j.main = patched_main

# Get number of volumes from command line argument
volumes_to_inject = int(sys.argv[1]) if len(sys.argv) > 1 else 5

print(f"Injecting {volumes_to_inject} volumes...")

# Run main injection with limited volumes
success = write_neo4j.main(spark, volumes_to_inject=volumes_to_inject)

spark.stop()
sys.exit(0 if success else 1)
EOF

# Copy and run the wrapper script
docker cp /tmp/write_neo4j_wrapper.py $SPARK_CONTAINER:/tmp/write_neo4j_wrapper.py

# Run the injection with limited volumes
print_step "Running injection for $VOLUMES_TO_INJECT volumes..."
if docker exec $SPARK_CONTAINER \
    spark-submit \
    --packages neo4j-contrib:neo4j-spark-connector:5.3.1-s_2.12 \
    --conf spark.neo4j.url=bolt://neo4j:7687 \
    --conf spark.neo4j.authentication.basic.username=neo4j \
    --conf spark.neo4j.authentication.basic.password=password \
    --master spark://spark:7077 \
    /tmp/write_neo4j_wrapper.py $VOLUMES_TO_INJECT; then
    
    print_success "Data injection completed!"
    
else
    print_warning "Spark-submit method failed, trying alternative Python execution..."
    
    # Method 2: Try running directly with Python
    if docker exec $SPARK_CONTAINER python /tmp/write_neo4j_wrapper.py $VOLUMES_TO_INJECT; then
        print_success "Data injection completed via Python!"y
    else
        print_error "Data injection failed"
        
        # Show logs for debugging
        echo "Recent Spark logs:"
        docker logs --tail 20 $SPARK_CONTAINER
        exit 1
    fi
fi

# Cleanup temporary file
rm -f /tmp/write_neo4j_wrapper.py

# Verify injection
print_step "Verifying data injection..."

PAPER_COUNT=$(docker exec $(docker ps -q -f "name=neo4j") \
    cypher-shell -u neo4j -p password \
    "MATCH (p:Paper) RETURN count(p) as count" \
    --format plain 2>/dev/null | grep -o '[0-9]*' | head -1)

VOLUME_COUNT=$(docker exec $(docker ps -q -f "name=neo4j") \
    cypher-shell -u neo4j -p password \
    "MATCH (v:Volume) RETURN count(v) as count" \
    --format plain 2>/dev/null | grep -o '[0-9]*' | head -1)

PERSON_COUNT=$(docker exec $(docker ps -q -f "name=neo4j") \
    cypher-shell -u neo4j -p password \
    "MATCH (p:Person) RETURN count(p) as count" \
    --format plain 2>/dev/null | grep -o '[0-9]*' | head -1)

if [ ! -z "$PAPER_COUNT" ] && [ "$PAPER_COUNT" -gt 0 ]; then
    print_success "Data injection verified successfully!"
    echo ""
    echo "📊 Data Summary:"
    echo "   Papers:  $PAPER_COUNT"
    echo "   Volumes: $VOLUME_COUNT"
    echo "   People:  $PERSON_COUNT"
    echo ""
    echo "🎉 Data injection complete! Your Streamlit app should now show the data."
    echo "   Visit: http://localhost:8501"
    
    # Ask if user wants to refresh Streamlit
    read -p "Would you like to restart Streamlit to ensure it sees the new data? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_step "Restarting Streamlit..."
        $DOCKER_COMPOSE_CMD --profile streamlit restart streamlit-app
        print_success "Streamlit restarted"
    fi
else
    print_error "Data injection could not be verified"
    echo "Please check Neo4j Browser at http://localhost:7474 to verify data"
fi

echo ""
echo "📖 For detailed usage instructions, see the README.md file"
echo ""
echo "💡 Tip: You can specify the number of volumes to inject:"
echo "   ./inject_data.sh 10    # Inject 10 volumes"
echo "   ./inject_data.sh 1     # Inject just 1 volume for testing"