import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tqdm import tqdm
from scraper.scraper import Scraper
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


def main():
    scraper = Scraper()

    all_volumes = scraper.get_volumes_to_scrape()

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


if __name__ == "__main__":
    main()
