from neomodel import StructuredNode, StringProperty, RelationshipTo


class Keyword(StructuredNode):
    name = StringProperty()

    def to_dict(self):
        return {"name": self.name}

    def __str__(self):
        return f"Keyword: {self.name}"


class Paper(StructuredNode):
    url = StringProperty()
    title = StringProperty()
    pages = StringProperty()
    abstract = StringProperty()

    # Relationships
    authors = RelationshipTo("Person", "AUTHOR")
    keywords = RelationshipTo("Keyword", "KEYWORD")

    def to_dict(self):
        authors = [author.to_dict() for author in self.authors]

        keywords = [keyword.to_dict() for keyword in self.keywords]

        return {
            "url": self.url,
            "title": self.title,
            "pages": self.pages,
            "abstract": self.abstract,
            "authors": authors,
            "keywords": keywords,
        }

    def __str__(self):
        author_names = [author.name for author in self.authors]
        authors_str = ", ".join(author_names) if author_names else "No authors"

        keyword_names = [keyword.name for keyword in self.keywords]
        keywords_str = ", ".join(keyword_names) if keyword_names else "No keywords"

        return f"Paper: {self.title} (Authors: {authors_str}, Keywords: {keywords_str})"
