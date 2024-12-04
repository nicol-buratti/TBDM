import json
import logging
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
from scraper.scraper import Scraper
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    filename="scraping.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def scrape_volume(volume_id: int, scraper: Scraper):
    volume_metadata = scraper.get_volume_metadata(volume_id)
    if not volume_metadata:
        return

    volume_papers = scraper.get_volume_papers(volume_id)
    volume_metadata.papers = volume_papers

    volume_path = Path("../data/Volumes")
    os.makedirs(volume_path, exist_ok=True)
    json_volume_path = volume_path / f"{volume_metadata.volnr}.json"

    # Save json file
    with open(json_volume_path, "w") as file:
        json.dump(volume_metadata.to_dict(), file, indent=4)


def main():
    scraper = Scraper()

    all_volumes = scraper.get_all_volumes()

    with ThreadPoolExecutor(max_workers=20) as executor:  # Adjust max_workers as needed
        # Submit each volume scraping task to executor
        futures = []
        for volume_id in all_volumes:
            futures.append(executor.submit(scrape_volume, volume_id, scraper))

        # Wait for all tasks to complete
        for future in futures:
            future.result()


if __name__ == "__main__":
    main()
