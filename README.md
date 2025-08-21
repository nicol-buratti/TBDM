# TBDM

to run only the neo4j database: 
```console
docker compose up
```


to run the submodules like the web-scraper: 
```console
docker compose --profile scraper up
```

to run the data injection notebook
```console
docker compose --profile injection up
```
# Setup and Run

Make scripts executable
```console
chmod +x setup.sh inject_data.sh
```
Run setup (automatically starts all services)
```console
./setup.sh
```
Access the application
```console
http://localhost:8501
```

# Data Injection

## Automatic Injection

The setup script automatically injects 10 volumes if Neo4j is empty on first run.

## Manual Injection
Inject specific number of volumes
```console
./inject_data.sh 20
```

Interactive mode (prompts for volume count)
```console
./inject_data.sh
```
