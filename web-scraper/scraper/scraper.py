import logging
import os
import re
from pathlib import Path
from typing import List

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader

from models.paper import Keyword, Paper, Person
from models.volume import Volume


class Scraper:
    base_url = "https://ceur-ws.org/"

    def get_volumes_to_scrape(self):
        # Since this function can take a while, the volume numbers are cached
        scraped_volumes = {file.stem for file in Path("./data/Volumes").iterdir()}

        logging.info("Getting volumes to scrape")
        response = requests.get(self.base_url)
        soup = BeautifulSoup(response.text, "html.parser")
        vol_tags = soup.find_all(
            "a", {"name": lambda value: value and value.startswith("Vol-")}
        )
        vol_values = (tag["name"] for tag in vol_tags)
        vol_values = [vol for vol in vol_values if vol not in scraped_volumes]
        logging.info(f"Volumes to scrape extracted: {vol_values}")
        return vol_values

    def get_volume_metadata(self, volume_id) -> Volume:
        logging.info(f"Getting metadata for volume {volume_id}")

        response = requests.get(self.base_url + volume_id)
        soup = BeautifulSoup(response.text, "html.parser")

        try:
            # Generate the list of editors
            voleditor = [
                Person(name.string)
                for name in soup.find_all("span", class_="CEURVOLEDITOR")
                if name
            ]

            # Fetch volume papers
            papers = self.get_volume_papers(volume_id)

            title = soup.title
            volnr = volume_id
            urn = soup.find("span", class_="CEURURN")
            pubyear = soup.find("span", class_="CEURPUBYEAR")
            volacronym = soup.find("span", class_="CEURVOLACRONYM")
            voltitle = soup.find("span", class_="CEURVOLTITLE")
            fulltitle = soup.find("span", class_="CEURFULLTITLE")
            loctime = soup.find("span", class_="CEURLOCTIME")

            volume = Volume(
                title=title.string if title else None,
                volnr=volnr,
                urn=urn.string if urn else None,
                pubyear=pubyear.string if pubyear else None,
                volacronym=volacronym.string if volacronym else None,
                voltitle=voltitle.string if voltitle else None,
                fulltitle=fulltitle.string if fulltitle else None,
                loctime=loctime.string if loctime else None,
                voleditors=voleditor,
                papers=papers,
            )

            return volume

        except ValueError as e:
            logging.error(f"{volume_id}, Scraping error: {e}")
        except Exception as e:
            logging.error(f"{volume_id}, An unexpected error occurred: {e}")

    def get_volume_metadata_first_900(self, volume_id):
        # TODO
        pass

    def get_volume_papers(self, volume_id) -> List[Paper]:
        logging.info(f"Getting all papers for {volume_id}")

        response = requests.get(self.base_url + volume_id)
        soup = BeautifulSoup(response.text, "html.parser")
        papers = []

        try:
            for num, li in enumerate(soup.select("div.CEURTOC li")):
                title_element = li.select_one("span.CEURTITLE")
                if not (
                    li.a and title_element and title_element.string
                ):  # Skip non-paper content
                    continue

                url = self.base_url + volume_id + "/" + li.a["href"]
                pages = (
                    li.select_one("span.CEURPAGES").string
                    if li.select_one("span.CEURPAGES")
                    else None
                )
                abstract, keywords = extract_data_from_pdf(url, volume_id, num)

                authors = [
                    Person(author.string)
                    for author in li.find_all("span", class_="CEURAUTHOR")
                    if author
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
        except ValueError as e:
            logging.error(f"{volume_id} paper {num}, Scraping error: {e}")
        except Exception as e:
            logging.error(f"{volume_id} paper {num}, An unexpected error occurred: {e}")
        finally:
            return papers


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

    # Extract the text between "abstract" and "keywords" or "introduction"
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

    # Extract text between "keywords" and "introduction" or "."
    for line in lines:
        if "keywords" in line:
            start_found = True
        elif ("." in line or "introduction" in line) and start_found:
            break
        elif start_found:
            extracted_lines.append(line.strip())

    # Join the extracted lines into a single string
    string = "".join(extracted_lines)
    keys = (
        clear_keyword(keyword) for keyword in re.split(r"[;,]", string) if keyword != ""
    )
    return [Keyword(key) for key in keys]
