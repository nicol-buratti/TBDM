import json
import logging
import os
from pathlib import Path
import re
import requests
from bs4 import BeautifulSoup
from models.volume import Volume
from models.paper import Paper
from pypdf import PdfReader


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
            abstract, keywords = "TODO", []
            abstract, keywords = self.__extract_data_from_pdf(url, volume_id, num)
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
        # get and save the pdf
        response = requests.get(url)
        pdf_path = Path("../data/tmp")
        pdf_path.mkdir(parents=True, exist_ok=True)
        pdf_name = f"{volume_id}-{num}.pdf"
        pdf_filepath = pdf_path / pdf_name
        with open(pdf_filepath, "wb") as f:
            f.write(response.content)

        # extract abstract and keywords from pdf
        reader = PdfReader(pdf_filepath)
        page = reader.pages[0]
        text = page.extract_text()
        abstract = extract_abstract(text)
        keywords = extract_keywords(text)
        # delete the saved pdf file
        os.remove(pdf_filepath)
        return abstract, keywords


def extract_abstract(text):
    lines = text.splitlines()
    start_found = False
    extracted_lines = []

    for line in lines:
        if "abstract" in line.lower():
            start_found = True
        elif "keywords" in line.lower() and start_found:
            break
        elif start_found:
            extracted_lines.append(line.strip())

    # Join the extracted lines into a single string
    return " ".join(extracted_lines)


def extract_keywords(text):
    def clear_keyword(keyword):
        keyword = "".join(char for char in keyword if not char.isdigit())
        if keyword[0].isspace():
            keyword = keyword[1:]
        return keyword

    lines = text.splitlines()
    start_found = False
    extracted_lines = []

    for line in lines:
        if "keywords" in line.lower():
            start_found = True
        elif ("." in line or "introduction" in line.lower()) and start_found:
            break
        elif start_found:
            extracted_lines.append(line.strip())

    # Join the extracted lines into a single string
    string = "".join(extracted_lines)
    return [
        clear_keyword(keyword) for keyword in re.split(r"[;,]", string) if keyword != ""
    ]
