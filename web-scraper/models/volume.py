from neomodel import (
    StructuredNode,
    StringProperty,
    RelationshipTo,
)

from models.paper import Paper
from models.people import Person


class Volume(StructuredNode):
    title = StringProperty()
    volnr = StringProperty()
    urn = StringProperty()
    pubyear = StringProperty()
    volacronym = StringProperty()
    voltitle = StringProperty()
    fulltitle = StringProperty()
    loctime = StringProperty()
    voleditors = RelationshipTo(Person, "EDITOR")
    papers = RelationshipTo(Paper, "CONTAINS")

    def to_dict(self):
        papers = [paper.to_dict() for paper in self.papers]

        return {
            "title": self.title,
            "volnr": self.volnr,
            "urn": self.urn,
            "pubyear": self.pubyear,
            "volacronym": self.volacronym,
            "voltitle": self.voltitle,
            "fulltitle": self.fulltitle,
            "loctime": self.loctime,
            "voleditors": self.voleditors,
            "papers": papers,
        }

    def __str__(self):
        editor_names = [editor.name for editor in self.voleditors]
        editor_str = ", ".join(editor_names) if editor_names else "No editor"

        paper_titles = [paper.title for paper in self.papers]
        papers_str = ", ".join(paper_titles) if paper_titles else "No papers"

        return f"Volume: {self.title} (Volume Number: {self.volnr}, Editor(s): {editor_str}, Papers: {papers_str})"
