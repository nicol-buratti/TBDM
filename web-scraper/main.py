import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from neomodel import config
from tqdm import tqdm

from models.database import Neo4jDatabase
from models.paper import Paper
from models.people import Person
from models.volume import Volume
from scraper.scraper import Scraper
from neomodel import db

logging.basicConfig(
    filename="scraping.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
config.DATABASE_URL = "bolt://neo4j:password@localhost:7687"


def scrape_volume(volume_id: int, scraper: Scraper, volume_path: Path):
    volume_metadata = scraper.get_volume_metadata(volume_id)
    # if the scraping failed
    if not volume_metadata:
        return

    file_path = volume_path / f"{volume_id}.json"

    # Writing the object to a JSON file
    with open(file_path, "w") as json_file:
        json.dump(volume_metadata.to_dict(), json_file, indent=4)

    Neo4jDatabase.create_volume(volume_metadata)
    return volume_metadata


def load_json(file_path):
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
        volume = Volume(**data)
        volume.papers = [Paper(**paper) for paper in volume.papers]
        return volume
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        return None


def main():
    # import logging

    # logger = logging.getLogger()  # Get the root logger
    # logger.setLevel(logging.ERROR)
    scraper = Scraper()

    all_volumes = scraper.get_all_volumes()

    volume_path = Path("./data/Volumes")
    os.makedirs(volume_path, exist_ok=True)

    with ThreadPoolExecutor(max_workers=20) as executor:
        # Submit each volume scraping task to executor

        for volume in tqdm(
            executor.map(scraper.get_volume_metadata, all_volumes),
            total=len(all_volumes),
            desc="Processing volumes",
            unit="volume",
        ):
            file_path = volume_path / f"{volume.volnr}.json"

            # Writing the object to a JSON file
            with open(file_path, "w") as json_file:
                json.dump(volume.to_dict(), json_file, indent=4)

        # Save all volumes in the database
        # TODO still to test
        json_files = (file for file in volume_path.iterdir() if file.suffix == ".json")

        for volume in tqdm(
            executor.map(load_json, json_files),
            desc="Saving volumes",
            unit="volume",
        ):
            Neo4jDatabase.create_volume(volume)


if __name__ == "__main__":
    db.cypher_query(
        """MATCH (n)
    DETACH DELETE n
    """
    )
    main()
