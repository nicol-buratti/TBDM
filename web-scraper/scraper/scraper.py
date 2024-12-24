import json
import logging
import os
import re
from pathlib import Path
from typing import List

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader

from models.paper import Paper
from models.volume import Volume


class Scraper:
    base_url = "https://ceur-ws.org/"

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
        logging.info(f"Getting metadata for volume {volume_id}")

        response = requests.get(self.base_url + volume_id)
        soup = BeautifulSoup(response.text, "html.parser")

        try:
            # Generate the list of editors
            voleditor = [
                name.string for name in soup.find_all("span", class_="CEURVOLEDITOR")
            ]

            # Fetch volume papers
            papers = self.get_volume_papers(volume_id)

            # Create and save volume node
            volume = Volume(
                title=soup.title.string,
                volnr=soup.find("span", class_="CEURVOLNR").string,
                urn=soup.find("span", class_="CEURURN").string,
                pubyear=soup.find("span", class_="CEURPUBYEAR").string,
                volacronym=soup.find("span", class_="CEURVOLACRONYM").string,
                voltitle=soup.find("span", class_="CEURVOLTITLE").string,
                fulltitle=soup.find("span", class_="CEURFULLTITLE").string,
                loctime=soup.find("span", class_="CEURLOCTIME").string,
                voleditor=voleditor,
                papers=papers,
            )

            return volume

        except ValueError as e:
            logging.error(f"{volume_id}, Scraping error: {e}")
        except Exception as e:
            logging.error(f"{volume_id}, An unexpected error occurred: {e}")

    def get_volume_metadata_first_900(self, volume_id):
        pass

    def get_volume_papers(self, volume_id) -> List[Paper]:
        logging.info(f"Getting all papers for {volume_id}")

        # Fetch page content
        response = requests.get(self.base_url + volume_id)
        soup = BeautifulSoup(response.text, "html.parser")
        try:
            papers = []

            # Find all list items within the div with class "CEURTOC"
            for num, li in enumerate(soup.select("div.CEURTOC li")):
                title_element = li.select_one("span.CEURTITLE")
                if not (
                    li.a and title_element and title_element.string
                ):  # Skip non-paper content
                    continue

                # Extract paper information
                url = self.base_url + volume_id + "/" + li.a["href"]
                pages = (
                    li.select_one("span.CEURPAGES").string
                    if li.select_one("span.CEURPAGES")
                    else None
                )
                abstract, keywords = extract_data_from_pdf(url, volume_id, num)

                # Create authors and keywords
                # authors = [
                #     author.string for author in soup.select("span.CEURAUTHOR") if author
                # ]
                authors = [
                    author.string for author in li.find_all("span", class_="CEURAUTHOR")
                ]
                # Create Paper object
                paper = Paper(
                    url=url,
                    title=title_element.string,
                    pages=pages,
                    abstract=abstract,
                    keywords=keywords,
                    authors=authors,
                )
                papers.append(paper)
            return papers
        except ValueError as e:
            logging.error(f"{volume_id} paper {num}, Scraping error: {e}")
        except Exception as e:
            logging.error(f"{volume_id} paper {num}, An unexpected error occurred: {e}")


def extract_data_from_pdf(url, volume_id, num):
    # get and save the pdf
    response = requests.get(url)
    pdf_path = Path("./data/tmp")
    pdf_path.mkdir(parents=True, exist_ok=True)
    pdf_name = f"{volume_id}-{num}.pdf"
    pdf_filepath = pdf_path / pdf_name
    with open(pdf_filepath, "wb") as f:
        f.write(response.content)

    # extract abstract and keywords from pdf
    reader = PdfReader(pdf_filepath)
    page = reader.pages[0]
    text = page.extract_text().lower()
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
        if "abstract" in line:
            start_found = True
        elif ("keywords" in line or "introduction" in line) and start_found:
            break
        elif start_found:
            extracted_lines.append(line.strip())

    # Join the extracted lines into a single string
    abstract = " ".join(extracted_lines)
    # Capitalize letters after ".", "!", "?"
    return re.sub("(^|[.?!])\s*([a-zA-Z])", lambda p: p.group(0).upper(), abstract)


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
        if "keywords" in line:
            start_found = True
        elif ("." in line or "introduction" in line) and start_found:
            break
        elif start_found:
            extracted_lines.append(line.strip())

    # Join the extracted lines into a single string
    string = "".join(extracted_lines)
    return [
        clear_keyword(keyword) for keyword in re.split(r"[;,]", string) if keyword != ""
    ]
