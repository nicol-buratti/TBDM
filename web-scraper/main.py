import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from neo4j.exceptions import ServiceUnavailable
from tqdm import tqdm
from dotenv import load_dotenv

from models.database import Neo4jDatabase
from models.paper import Paper
from models.volume import Volume
from models.people import Person
from scraper.scraper import Scraper
from neomodel import db, config
from datetime import datetime

log_path = Path("logs")
log_path.mkdir(parents=True, exist_ok=True)

current_time = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
log_file_path = log_path / f"scraping{current_time}.log"

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI")
config.DATABASE_URL = NEO4J_URI


def load_json_volume(file_path):
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
        data["papers"] = [Paper(**paper) for paper in data["papers"]]
        data["voleditors"] = [Person(name=name) for name in data["voleditors"]]
        volume = Volume(**data)

        return volume
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")


def process_volume(json_file):
    volume = load_json_volume(json_file)
    Neo4jDatabase.create_volume(volume)
    return volume


def main():
    scraper = Scraper()

    all_volumes = scraper.get_all_volumes()

    volume_path = Path("./data/Volumes")
    os.makedirs(volume_path, exist_ok=True)

    with ThreadPoolExecutor(max_workers=20) as executor:

        # Extract all volumes informations and save them in json files
        for volume in tqdm(
            executor.map(scraper.get_volume_metadata, all_volumes),
            total=len(all_volumes),
            desc="Processing volumes",
            unit="volume",
        ):
            # The scraping failed
            if not volume:
                continue

            file_path = volume_path / f"{volume['volnr']}.json"
            with open(file_path, "w") as json_file:
                json.dump(volume, json_file, indent=4)

        # Save all volumes in the database
        json_files = [file for file in volume_path.iterdir() if file.suffix == ".json"]

        for _ in tqdm(
            executor.map(process_volume, json_files),
            total=len(json_files),
            desc="Saving volumes",
            unit="volume",
        ):
            pass

def connect_to_database():
    db.cypher_query("RETURN 1")  # Simple query to validate the connection


if __name__ == "__main__":
    logging.info("Connected to Neo4j successfully.")
    db.cypher_query(
        """MATCH (n)
    DETACH DELETE n
    """
    )
    main()
