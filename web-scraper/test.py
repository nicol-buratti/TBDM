from pathlib import Path
import re

from pypdf import PdfReader


def extract_keywords(text):
    keywords_match = re.search(r"Keywords\s+1\s+(.*?)\s+1\. Introduction", text, re.S)
    return keywords_match.group(1).strip() if keywords_match else None


def extract_section_between_keywords_and_introduction(text):
    # Split text into lines
    lines = text.splitlines()

    # Initialize flags and result container
    start_found = False
    extracted_lines = []

    # Iterate through lines
    for line in lines:
        if "Keywords" in line:
            start_found = True  # Start capturing
        elif "Introduction" in line and start_found:
            break  # Stop capturing
        elif start_found:
            extracted_lines.append(line.strip())  # Add stripped line to results

    # Join the extracted lines into a single string
    string = " ".join(extracted_lines)
    return string.split(",")


pdf_filepath = Path("../data/tmp/Vol-3699-2.pdf")
# pdf_filepath = Path("../data/tmp/Vol-3706-3.pdf")
reader = PdfReader(pdf_filepath)
page = reader.pages[0]
text = page.extract_text()
keywords = extract_keywords(text)
print(text)
print("\n")
print(keywords)

print("\n")
print(extract_section_between_keywords_and_introduction(text))
