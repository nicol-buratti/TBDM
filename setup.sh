#!/bin/bash

set -e

echo "Research Paper Network Explorer - Setup with Integrated Data Injection"
echo "===================================================================="
echo ""

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

# Check prerequisites
print_step "Checking prerequisites..."

# Check Docker
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed. Please install Docker first."
    exit 1
fi
print_success "Docker found"

# Check Docker Compose
DOCKER_COMPOSE_CMD=""
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker-compose"
    print_success "Docker Compose (legacy) found"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker compose"
    print_success "Docker Compose (V2) found"
else
    print_error "Docker Compose is not available."
    exit 1
fi

# Check if data directory exists and has JSON files
print_step "Checking for data files..."
if [ -d "data/Volumes" ] && [ "$(ls -A data/Volumes/*.json 2>/dev/null)" ]; then
    print_success "Found data files in data/Volumes/"
    JSON_FILE_COUNT=$(ls data/Volumes/*.json 2>/dev/null | wc -l)
    echo "   Found $JSON_FILE_COUNT JSON files"
    HAS_DATA=true
else
    print_warning "No data files found in data/Volumes/"
    echo "   Please place your JSON volume files in data/Volumes/ directory"
    echo "   The system will start without data injection"
    HAS_DATA=false
fi

# Check if write_neo4j.py exists in data-injection folder
print_step "Checking for write_neo4j.py..."
if [ -f "data-injection/write_neo4j.py" ]; then
    print_success "Found write_neo4j.py in data-injection folder"
    HAS_INJECTION_SCRIPT=true
else
    print_warning "write_neo4j.py not found in data-injection folder"
    HAS_INJECTION_SCRIPT=false
fi

# Clean up any existing containers
print_step "Cleaning up existing containers..."
$DOCKER_COMPOSE_CMD --profile injection --profile streamlit down --remove-orphans --volumes 2>/dev/null || true
docker container prune -f 2>/dev/null || true
print_success "Cleanup completed"

# Create project structure
print_step "Setting up project structure..."
mkdir -p data/Volumes
mkdir -p logs
mkdir -p neo4j/data
mkdir -p neo4j/logs
mkdir -p neo4j/config
mkdir -p neo4j/plugins
mkdir -p functionalities
touch functionalities/__init__.py

print_success "Project structure created"

# Create Docker networks
print_step "Setting up Docker networks..."
docker network create spark-net 2>/dev/null || true
docker network create scraper-network 2>/dev/null || true
print_success "Docker networks ready"

# Start Neo4j first and wait for it
print_step "Starting Neo4j database..."
$DOCKER_COMPOSE_CMD --profile injection up -d neo4j

# Wait for Neo4j to be ready
print_step "Waiting for Neo4j to start..."
max_attempts=60
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if docker exec $(docker ps -q -f "name=neo4j") cypher-shell -u neo4j -p password "RETURN 1" > /dev/null 2>&1; then
        print_success "Neo4j is ready"
        break
    fi
    
    if [ $((attempt % 10)) -eq 0 ]; then
        echo "   ... still waiting for Neo4j ($attempt/$max_attempts)"
    fi
    
    sleep 5
    attempt=$((attempt + 1))
done

if [ $attempt -eq $max_attempts ]; then
    print_error "Neo4j failed to start within expected time"
    exit 1
fi

# Start Spark services
print_step "Starting Spark services..."
$DOCKER_COMPOSE_CMD --profile injection up -d spark spark-worker

# Wait for Spark to be ready
print_step "Waiting for Spark to start..."
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if curl -s -f "http://localhost:8080" > /dev/null 2>&1; then
        print_success "Spark is ready"
        break
    fi
    
    if [ $((attempt % 5)) -eq 0 ]; then
        echo "   ... still waiting for Spark ($attempt/$max_attempts)"
    fi
    
    sleep 5
    attempt=$((attempt + 1))
done

if [ $attempt -eq $max_attempts ]; then
    print_error "Spark failed to start within expected time"
    exit 1
fi

# Data injection
if [ "$HAS_DATA" = true ] && [ "$HAS_INJECTION_SCRIPT" = true ]; then
    print_step "Checking if data injection is needed..."
    
    # Check if data already exists in Neo4j
    EXISTING_PAPERS=$(docker exec $(docker ps -q -f "name=neo4j") \
        cypher-shell -u neo4j -p password \
        "MATCH (p:Paper) RETURN count(p) as count" \
        --format plain 2>/dev/null | grep -o '[0-9]*' | head -1)
    
    EXISTING_VOLUMES=$(docker exec $(docker ps -q -f "name=neo4j") \
        cypher-shell -u neo4j -p password \
        "MATCH (v:Volume) RETURN count(v) as count" \
        --format plain 2>/dev/null | grep -o '[0-9]*' | head -1)
    
    if [ ! -z "$EXISTING_PAPERS" ] && [ "$EXISTING_PAPERS" -gt 0 ]; then
        print_success "Found existing data in Neo4j:"
        echo "   Papers: $EXISTING_PAPERS"
        echo "   Volumes: $EXISTING_VOLUMES"
        print_success "Using existing data - skipping injection"
    else
        print_step "No data found in Neo4j - starting automatic data injection..."
        
        # Determine how many volumes to inject (default 10 for initial setup)
        DEFAULT_VOLUMES=10
        echo "   Will inject $DEFAULT_VOLUMES volumes for initial setup"
        echo "   (You can inject more data later using ./inject_data.sh)"
        
        # Get Spark container ID
        SPARK_CONTAINER=$(docker ps -q -f "name=spark" -f "status=running" | head -1)
        
        # Copy data to Spark container
        print_step "Preparing data injection..."
        docker exec $SPARK_CONTAINER mkdir -p /tmp/data 2>/dev/null || true
        docker cp data/Volumes $SPARK_CONTAINER:/tmp/data/
        
        # Copy injection script
        docker cp data-injection/write_neo4j.py $SPARK_CONTAINER:/tmp/write_neo4j.py
        
        # Install required packages
        docker exec $SPARK_CONTAINER pip install --user --quiet python-dotenv py4j 2>/dev/null || true
        
        # Create wrapper script for limited injection
        cat > /tmp/setup_injection_wrapper.py << 'EOF'
import os
import sys
from pathlib import Path

sys.path.insert(0, '/tmp')
os.environ["NEO4J_URI"] = "bolt://neo4j:7687"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SetupDataInjection") \
    .master("spark://spark:7077") \
    .config("spark.jars.packages", "neo4j-contrib:neo4j-spark-connector:5.3.1-s_2.12") \
    .config("neo4j.url", "bolt://neo4j:7687") \
    .config("neo4j.authentication.basic.username", "neo4j") \
    .config("neo4j.authentication.basic.password", "password") \
    .config("neo4j.database", "neo4j") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

import write_neo4j

# Patch to use correct data path
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

# Read and inject data
json_dir = "/tmp/data/Volumes"
df = spark.read.schema(schema).option("multiline", "true").json(json_dir)
total_volumes = df.count()
print(f"Found {total_volumes} volumes")

# Limit to specified number of volumes
volumes_to_inject = int(sys.argv[1]) if len(sys.argv) > 1 else 10
df = df.limit(volumes_to_inject)
print(f"Injecting {volumes_to_inject} volumes...")

# Create relationships
write_neo4j.volume_relationships(df)
write_neo4j.papers_relationships(df)

# Verify
write_neo4j.verify_data_injection(spark)

spark.stop()
EOF
        
        docker cp /tmp/setup_injection_wrapper.py $SPARK_CONTAINER:/tmp/setup_injection_wrapper.py
        
        # Run injection
        print_step "Running data injection..."
        if docker exec $SPARK_CONTAINER \
            spark-submit \
            --packages neo4j-contrib:neo4j-spark-connector:5.3.1-s_2.12 \
            /tmp/setup_injection_wrapper.py $DEFAULT_VOLUMES 2>&1 | tee /tmp/injection.log; then
            
            print_success "Data injection completed!"
            
            # Verify injection
            PAPER_COUNT=$(docker exec $(docker ps -q -f "name=neo4j") \
                cypher-shell -u neo4j -p password \
                "MATCH (p:Paper) RETURN count(p) as count" \
                --format plain 2>/dev/null | grep -o '[0-9]*' | head -1)
            
            if [ ! -z "$PAPER_COUNT" ] && [ "$PAPER_COUNT" -gt 0 ]; then
                print_success "Data injection verified: $PAPER_COUNT papers injected"
            fi
        else
            print_warning "Data injection encountered issues - check logs"
            echo "Continuing with setup anyway..."
        fi
        
        # Cleanup
        rm -f /tmp/setup_injection_wrapper.py
    fi
    
else
    if [ "$HAS_DATA" = false ]; then
        print_warning "Skipping data injection - no data files found"
        echo "   Place JSON files in data/Volumes/ and run ./inject_data.sh"
    elif [ "$HAS_INJECTION_SCRIPT" = false ]; then
        print_warning "Skipping data injection - write_neo4j.py not found"
    fi
fi

# Start Streamlit application
print_step "Starting Streamlit application..."
$DOCKER_COMPOSE_CMD --profile streamlit up -d streamlit-app

# Wait for Streamlit
print_step "Waiting for Streamlit to start..."
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if curl -s -f "http://localhost:8501/_stcore/health" > /dev/null 2>&1; then
        print_success "Streamlit is ready"
        break
    fi
    
    if [ $((attempt % 5)) -eq 0 ]; then
        echo "   ... still waiting for Streamlit ($attempt/$max_attempts)"
    fi
    
    sleep 5
    attempt=$((attempt + 1))
done

if [ $attempt -eq $max_attempts ]; then
    print_error "Streamlit failed to start within expected time"
    print_error "Check logs with: $DOCKER_COMPOSE_CMD --profile streamlit logs streamlit-app"
fi

# Final status check
print_step "Final status check..."

services=("neo4j" "spark" "streamlit-app")
all_running=true

for service in "${services[@]}"; do
    if $DOCKER_COMPOSE_CMD ps "$service" 2>/dev/null | grep -q "Up\|running"; then
        print_success "$service is running"
    else
        print_error "$service is not running properly"
        all_running=false
    fi
done

# Verify data injection if it was attempted or if data exists
print_step "Verifying data availability..."

PAPER_COUNT=$(docker exec $(docker ps -q -f "name=neo4j") cypher-shell -u neo4j -p password "MATCH (p:Paper) RETURN count(p) as count" --format plain 2>/dev/null | grep -o '[0-9]*' | head -1)
VOLUME_COUNT=$(docker exec $(docker ps -q -f "name=neo4j") cypher-shell -u neo4j -p password "MATCH (v:Volume) RETURN count(v) as count" --format plain 2>/dev/null | grep -o '[0-9]*' | head -1)
PERSON_COUNT=$(docker exec $(docker ps -q -f "name=neo4j") cypher-shell -u neo4j -p password "MATCH (p:Person) RETURN count(p) as count" --format plain 2>/dev/null | grep -o '[0-9]*' | head -1)

if [ ! -z "$PAPER_COUNT" ] && [ "$PAPER_COUNT" -gt 0 ]; then
    print_success "Data available in Neo4j:"
    echo "   Papers: $PAPER_COUNT"
    echo "   Volumes: $VOLUME_COUNT"
    echo "   People: $PERSON_COUNT"
    DATA_VERIFIED=true
else
    print_warning "No data found in Neo4j"
    DATA_VERIFIED=false
fi

# Display final information
echo ""
echo "=============================================="
echo "           Setup Complete!"
echo "=============================================="

if [ "$all_running" = true ]; then
    echo ""
    echo "🌐 Access your application:"
    echo "   Streamlit App:     http://localhost:8501"
    echo "   Neo4j Browser:     http://localhost:7474"
    echo "   Spark Master UI:   http://localhost:8080"
    echo ""
    echo "🔑 Neo4j Credentials:"
    echo "   Username: neo4j"
    echo "   Password: password"
    echo ""
    
    echo "📊 Data Status:"
    if [ "${DATA_VERIFIED:-false}" = true ]; then
        echo "   ✅ Data is available in Neo4j:"
        echo "      - Papers: $PAPER_COUNT"
        echo "      - Volumes: $VOLUME_COUNT"  
        echo "      - People: $PERSON_COUNT"
        echo ""
        echo "   To inject more data, use: ./inject_data.sh [number_of_volumes]"
    else
        echo "   ⚠️ No data in Neo4j yet"
        if [ "$HAS_DATA" = true ] && [ "$HAS_INJECTION_SCRIPT" = true ]; then
            echo "   Run: ./inject_data.sh to inject data"
        else
            echo "   1. Place JSON files in data/Volumes/"
            echo "   2. Ensure write_neo4j.py is in data-injection/"
            echo "   3. Run: ./inject_data.sh"
        fi
    fi
    echo ""
    
    echo "🛠️ Useful commands:"
    echo "   View all logs:        $DOCKER_COMPOSE_CMD --profile streamlit logs -f"
    echo "   View Streamlit logs:  $DOCKER_COMPOSE_CMD --profile streamlit logs -f streamlit-app"
    echo "   Stop all services:    $DOCKER_COMPOSE_CMD --profile streamlit down"
    echo "   Restart services:     $DOCKER_COMPOSE_CMD --profile streamlit restart"
    echo ""
    
    if [ "$HAS_DATA" = false ] || [ "$HAS_INJECTION_SCRIPT" = false ]; then
        echo "📝 To inject data manually:"
        echo "   1. Ensure write_neo4j.py is in data-injection/"
        echo "   2. Place JSON files in data/Volumes/"
        echo "   3. Run: ./inject_data.sh"
        echo ""
    fi
    
    echo "🎉 Your Research Paper Network Explorer is ready!"
    
    # Optionally open browser
    read -p "Would you like to open the application in your browser? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if command -v xdg-open > /dev/null; then
            xdg-open http://localhost:8501
        elif command -v open > /dev/null; then
            open http://localhost:8501
        else
            echo "Please open http://localhost:8501 in your browser"
        fi
    fi
    
else
    echo ""
    print_error "Some services are not running properly."
    echo "Check the logs with: $DOCKER_COMPOSE_CMD --profile streamlit logs"
    echo "Try restarting with: $DOCKER_COMPOSE_CMD --profile streamlit restart"
fi

echo ""
echo "📖 For detailed usage instructions, see the README.md file"