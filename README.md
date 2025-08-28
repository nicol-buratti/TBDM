# 📚 Research Paper Network Explorer
A comprehensive system for scraping, analyzing, and visualizing research paper networks from CEUR Workshop Proceedings. This project combines web scraping, graph database technology, distributed computing, and interactive visualization to explore relationships between academic papers, authors, keywords, and volumes.

# 🎯 Project Overview
The Research Paper Network Explorer is designed to extract, process, and analyze academic research data from [CEUR-WS](www.CEUR-WS.org), a free open-access publication service for workshop and conference proceedings. The system creates a knowledge graph that reveals hidden patterns in academic collaboration, research trends, and paper relationships.

## Key Features
- **Automated Web Scraping**: Extracts paper metadata, abstracts, and keywords from PDF documents
- **Graph Database Storage**: Utilizes Neo4j to model complex relationships between papers, authors, volumes, and keywords
- **Distributed Processing**: Leverages Apache Spark for scalable data injection and analysis
- **Interactive Visualization**: Provides a Streamlit-based web interface for exploring the network
- **Advanced Analytics**: Includes community detection, link prediction, and similarity analysis algorithms

## 🛠️ Technologies
**Neo4j (Graph Database)**  
Neo4j is a native graph database that stores data as nodes and relationships rather than tables. It's ideal for this project because academic data is inherently graph-structured - papers are written by authors, belong to volumes, and share keywords. Neo4j's Cypher query language makes it easy to traverse these complex relationships.

**Apache Spark**  
Apache Spark is a unified analytics engine for large-scale data processing. In this project, Spark:

- Handles parallel data injection into Neo4j
- Executes distributed graph algorithms through the Neo4j Spark Connector
- Processes large volumes of papers efficiently

**Docker & Docker Compose**  
Docker ensures consistent environments across different systems by containerizing each component. Docker Compose orchestrates multiple services (Neo4j, Spark, Streamlit) with proper networking and dependencies.

**Streamlit (Web Interface)**
Streamlit is a Python framework for creating data applications. It provides an intuitive interface for users to explore the paper network without needing technical expertise.

## 📊 Data Pipeline
## 1. Web Scraping  
The scraper extracts data in multiple stages:

1. **Volume Discovery:** Fetches all available volume identifiers from CEUR-WS.org
2. **Metadata Extraction:** For each volume, extracts:
   - Volume information (title, year, location, editors)
   - Paper listings with authors and page numbers
3. **PDF Processing:** Downloads and analyzes paper PDFs to extract:
   - Abstracts (text between "abstract" and "keywords/introduction" sections)
   - Keywords (parsed from the keywords section)
4. **JSON Storage:** Saves structured data as JSON files for processing

## 2. Data Injection  
The injection module uses PySpark to:  
1. **Read JSON Files**: Loads scraped data with defined schema
2. **Create Graph Structure**: Transforms flat data into graph relationships:  
  - `Person -[AUTHORED]-> Paper`
  - `Person -[EDITED]-> Volume`
  - `Paper -[HAS_KEYWORD]-> Keyword`
  - `Volume -[CONTAINS]-> Paper`
    
3. **Batch Processing**: Efficiently writes nodes and relationships to Neo4j

# 🚀 Functionalities / Core Features

// TODO

# 📦 Installation & Setup
## Quickstart
  1. **Clone Repository**
  ```bash
  git clone https://github.com/nicol-buratti/TBDM.git
  ```

 2. **Run the web scraper**
  ```bash
  docker compose --profile scraper up
  ```

 3. **Inject the data into Neo4j**
  ```bash
  docker compose --profile injection up
  ```

 4. **Run the frontend**
  ```bash
  docker compose --profile streamlit up
  ```
Access the application at <u> http://localhost:8501 <u>

## Service ports
- **Neo4j Browser**: <u> http://localhost:7474 <u>
- **Spark UI**: <u> http://localhost:8080 <u>
- **Streamlit App**: <u> http://localhost:8501 <u>

# 📁 Project Structure
<pre>
.
├── docker-compose.yml          
├── Dockerfile.spark            
├── Dockerfile.streamlit       
├── streamlit_app.py          
├── functionalities/          
│   ├── community_detection.py
│   ├── link_prediction.py
│   ├── similarity.py
│   └── utils.py
├── web-scraper/              
│   ├── main.py
│   ├── scraper/
│   └── models/
├── data-injection/          
│   └── write_neo4j.py
└── data/                    
    └── Volumes/ </pre>

# 📄 License 
This project is licensed under the Apche 2.0 license - see the LICENSE file for details.




