from pyspark.sql import SparkSession
import logging


def create_graph(spark: SparkSession, graph_name: str):
    """
    Create or ensure the graph exists in Neo4j GDS catalog
    Uses your existing graph projection configuration
    """
    try:
        # Check if graph already exists
        graph_exists = (
            spark.read.format("org.neo4j.spark.DataSource")
            .option("gds", "gds.graph.exists")
            .option("gds.graphName", graph_name)
            .load()
            .first()["exists"]
        )
        
        if not graph_exists:
            # Create graph using your existing configuration
            result = (
                spark.read.format("org.neo4j.spark.DataSource")
                .option("gds", "gds.graph.project")
                .option("gds.graphName", graph_name)
                .option("gds.nodeProjection", ["Keyword", "Paper", "Volume"])
                .option(
                    "gds.relationshipProjection",
                    """
                {
                "KEYWORD": {"orientation": "UNDIRECTED"},
                "CONTAINS": {"orientation": "UNDIRECTED"}
                }
                """,
                )
                .load()
            )
            
            logging.info(f"Graph '{graph_name}' created successfully")
            return result
        else:
            logging.info(f"Graph '{graph_name}' already exists")
            return None
        
    except Exception as e:
        logging.error(f"Error creating graph '{graph_name}': {str(e)}")
        
        # Try to drop existing graph and recreate if there's a conflict
        try:
            logging.info(f"Attempting to drop and recreate graph '{graph_name}'")
            spark.read.format("org.neo4j.spark.DataSource") \
                .option("gds", "gds.graph.drop") \
                .option("gds.graphName", graph_name) \
                .load()
            
            # Recreate the graph
            result = (
                spark.read.format("org.neo4j.spark.DataSource")
                .option("gds", "gds.graph.project")
                .option("gds.graphName", graph_name)
                .option("gds.nodeProjection", ["Keyword", "Paper", "Volume"])
                .option(
                    "gds.relationshipProjection",
                    """
                {
                "KEYWORD": {"orientation": "UNDIRECTED"},
                "CONTAINS": {"orientation": "UNDIRECTED"}
                }
                """,
                )
                .load()
            )
            
            logging.info(f"Graph '{graph_name}' recreated successfully")
            return result
            
        except Exception as recreate_error:
            logging.error(f"Failed to recreate graph '{graph_name}': {str(recreate_error)}")
            raise recreate_error


def get_available_node_types(spark: SparkSession):
    """
    Get available node types from your Neo4j database
    """
    try:
        result = spark.read.format("org.neo4j.spark.DataSource") \
            .option("query", "CALL db.labels()") \
            .option("partitions", "1") \
            .load()
        
        labels = [row['label'] for row in result.collect()]
        return labels
        
    except Exception as e:
        logging.error(f"Error getting node types: {str(e)}")
        return ["Paper", "Volume", "Keyword", "Person"]  # Default fallback


def get_available_relationship_types(spark: SparkSession):
    """
    Get available relationship types from your Neo4j database
    """
    try:
        result = spark.read.format("org.neo4j.spark.DataSource") \
            .option("query", "CALL db.relationshipTypes()") \
            .option("partitions", "1") \
            .load()
        
        rel_types = [row['relationshipType'] for row in result.collect()]
        return rel_types
        
    except Exception as e:
        logging.error(f"Error getting relationship types: {str(e)}")
        return ["KEYWORD", "CONTAINS", "AUTHORED", "EDITED"]  # Default fallback


def validate_graph_projection(spark: SparkSession, graph_name: str):
    """
    Validate that the graph projection contains the expected data
    """
    try:
        # Check if graph exists and get basic stats
        graph_info = spark.read.format("org.neo4j.spark.DataSource") \
            .option("gds", "gds.graph.list") \
            .option("gds.graphName", graph_name) \
            .load()
        
        if graph_info.count() > 0:
            info = graph_info.first()
            node_count = info.get("nodeCount", 0)
            relationship_count = info.get("relationshipCount", 0)
            
            logging.info(f"Graph '{graph_name}' validation:")
            logging.info(f"  Nodes: {node_count}")
            logging.info(f"  Relationships: {relationship_count}")
            
            if node_count == 0:
                logging.warning(f"Graph '{graph_name}' has no nodes")
                return False
            
            if relationship_count == 0:
                logging.warning(f"Graph '{graph_name}' has no relationships")
                return False
            
            return True
        else:
            logging.error(f"Graph '{graph_name}' not found")
            return False
            
    except Exception as e:
        logging.error(f"Error validating graph '{graph_name}': {str(e)}")
        return False