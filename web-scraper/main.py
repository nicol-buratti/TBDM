import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from neomodel import config
from tqdm import tqdm

from models.database import Neo4jDatabase
from models.paper import Paper
from models.volume import Volume
from scraper.scraper import Scraper
from neomodel import db

logging.basicConfig(
    filename="scraping.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
config.DATABASE_URL = "bolt://neo4j:password@localhost:7687"


def load_json_volume(file_path):
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
        volume = Volume(**data)
        volume.papers = [Paper(**paper) for paper in volume.papers]
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

            file_path = volume_path / f"{volume.volnr}.json"
            with open(file_path, "w") as json_file:
                json.dump(volume.to_dict(), json_file, indent=4)

        # Save all volumes in the database
        json_files = (file for file in volume_path.iterdir() if file.suffix == ".json")

        for _ in tqdm(
            executor.map(process_volume, json_files),
            desc="Saving volumes",
            unit="volume",
        ):
            pass


if __name__ == "__main__":
    db.cypher_query(
        """MATCH (n)
    DETACH DELETE n
    """
    )
    main()
