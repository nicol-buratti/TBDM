from models.paper import Paper
from models.people import Person


class Volume:
    title: str
    volnr: str
    urn: str
    pubyear: str
    volacronym: str
    voltitle: str
    fulltitle: str
    loctime: str
    voleditors: list[Person] = []
    papers: list[Paper] = []

    def __str__(self):
        editor_names = [editor.name for editor in self.voleditors]
        editor_str = ", ".join(editor_names) if editor_names else "No editor"

        paper_titles = [paper.title for paper in self.papers]
        papers_str = ", ".join(paper_titles) if paper_titles else "No papers"

        return f"Volume: {self.title} (Volume Number: {self.volnr}, Editor(s): {editor_str}, Papers: {papers_str})"
