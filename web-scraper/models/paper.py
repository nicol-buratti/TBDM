from typing import List
from models.people import Person
from dataclasses import dataclass, field


@dataclass
class Keyword:
    name: str

    def __str__(self):
        return f"Keyword: {self.name}"


@dataclass
class Paper:
    url: str
    title: str
    pages: str
    abstract: str

    # Relationships
    authors: List[Person] = field(default_factory=list)
    keywords: List[Keyword] = field(default_factory=list)

    def __str__(self):
        author_names = [author.name for author in self.authors]
        authors_str = ", ".join(author_names) if author_names else "No authors"

        keyword_names = [keyword.name for keyword in self.keywords]
        keywords_str = ", ".join(keyword_names) if keyword_names else "No keywords"

        return f"Paper: {self.title} (Authors: {authors_str}, Keywords: {keywords_str})"

    def to_dict(self):
        dic = self.__dict__

        dic["authors"] = [a.__dict__ for a in self.authors]
        dic["keywords"] = [k.__dict__ for k in self.keywords]

        return dic
