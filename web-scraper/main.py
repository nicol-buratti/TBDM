import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from neomodel import config

from scraper.scraper import Scraper
from neomodel import db

logging.basicConfig(
    filename="scraping.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
config.DATABASE_URL = "bolt://neo4j:password@localhost:7687"


def scrape_volume(volume_id: int, scraper: Scraper):
    volume_metadata = scraper.get_volume_metadata(volume_id)
    # if the scraping failed
    if not volume_metadata:
        return

    return volume_metadata


def main():
    scraper = Scraper()

    all_volumes = scraper.get_all_volumes()

    volume_path = Path("./data/Volumes")
    os.makedirs(volume_path, exist_ok=True)

    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit each volume scraping task to executor
        futures = []
        for volume_id in all_volumes:
            futures.append(executor.submit(scrape_volume, volume_id, scraper))

        # Wait for all tasks to complete
        for future in futures:
            future.result()


if __name__ == "__main__":
    db.cypher_query(
        """MATCH (n)
    DETACH DELETE n
    """
    )
    main()
