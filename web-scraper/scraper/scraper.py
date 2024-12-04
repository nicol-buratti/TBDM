import json
import logging
import os
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from models.volume import Volume
from models.paper import Paper
import fitz


class Scraper:
    base_url = "https://ceur-ws.org/"

    logging.basicConfig(
        filename="scraping.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    def get_all_volumes(self):
        if os.path.exists("volumes.json"):
            logging.info("Getting cached volumes")
            with open("volumes.json", "r") as file:
                return json.load(file)

        logging.info("Getting all volumes")
        response = requests.get(self.base_url)
        soup = BeautifulSoup(response.text, "html.parser")
        vol_tags = soup.find_all(
            "a", {"name": lambda value: value and value.startswith("Vol-")}
        )
        vol_values = [tag["name"] for tag in vol_tags]
        with open("volumes.json", "w") as file:
            json.dump(vol_values, file, indent=4)
        return vol_values

    def get_volume_metadata(self, volume_id) -> Volume:
        logging.info(f"Getting metadata for {volume_id}")

        response = requests.get(self.base_url + volume_id)
        soup = BeautifulSoup(response.text, "html.parser")

        try:
            volume = Volume(
                title=soup.title.string,
                volnr=soup.find("span", class_="CEURVOLNR").string,
                urn=soup.find("span", class_="CEURURN").string,
                pubyear=soup.find("span", class_="CEURPUBYEAR").string,
                volacronym=soup.find("span", class_="CEURVOLACRONYM").string,
                voltitle=soup.find("span", class_="CEURVOLTITLE").string,
                fulltitle=soup.find("span", class_="CEURFULLTITLE").string,
                loctime=soup.find("span", class_="CEURLOCTIME").string,
                voleditor=[
                    editor.string
                    for editor in soup.find_all("span", class_="CEURVOLEDITOR")
                ],
            )
            return volume
        except ValueError as e:
            logging.error(f"{volume_id}, Scraping error: {e}")
        except Exception as e:
            logging.error(f"{volume_id}, An unexpected error occurred: {e}")

    def get_volume_metadata_first_900(self, volume_id):
        pass

    def get_volume_papers(self, volume_id):
        logging.info(f"Getting all papers for {volume_id}")
        response = requests.get(self.base_url + volume_id)
        soup = BeautifulSoup(response.text, "html.parser")

        papers = []
        for num, li in enumerate(soup.find("div", class_="CEURTOC").find_all("li")):
            title_element = li.find("span", class_="CEURTITLE")
            is_paper = li.a and title_element and title_element.string
            if not is_paper:
                logging.info(f"Volume {volume_id} contains non-paper content")
                continue

            url = self.base_url + volume_id + "/" + li.a.get("href")
            pages = li.find("span", class_="CEURPAGES").string
            # abstract, keywords = self.__extract_data_from_pdf(url, volume_id, num)
            abstract, keywords = "TODO", []
            paper = Paper(
                url=url,
                title=title_element.string,
                pages=pages if pages else None,
                author=[
                    author.string for author in li.find_all("span", class_="CEURAUTHOR")
                ],
                abstract=abstract,
                keywords=keywords,
            )
            papers.append(paper)
        return papers

    def __extract_data_from_pdf(self, url, volume_id, num):
        response = requests.get(url)
        pdf_path = Path("../data/tmp")
        pdf_path.mkdir(parents=True, exist_ok=True)
        pdf_name = f"{volume_id}-{num}.pdf"
        pdf_filepath = pdf_path / pdf_name
        logging.info(f"Saving pdf {volume_id}-{num} in {pdf_filepath.resolve()}")
        with open(pdf_filepath, "wb") as f:
            f.write(response.content)

        doc = fitz.open(pdf_filepath)
        logging.info(f"Extracting pdf {volume_id}-{num}")

        # Loop through pages to extract text
        for page_num in range(len(doc)):
            page = doc[page_num]
            text = page.get_text()

            abstract = ""
            keywords = []
            # Search for "Abstract"
            if "Abstract" in text:
                abstract_start = text.find("Abstract")
                abstract_end = text.find("\n", abstract_start)
                abstract = text[abstract_start:abstract_end].strip()

            # Search for "Keywords"
            if "Keywords" in text:
                keywords_start = text.find("Keywords")
                keywords_end = text.find("\n", keywords_start)
                keywords = text[keywords_start:keywords_end].strip()
        return abstract, keywords
