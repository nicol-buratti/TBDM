from models.people import Person


class Keyword:
    name: str

    def __str__(self):
        return f"Keyword: {self.name}"


class Paper:
    url: str
    title: str
    pages: str
    abstract: str

    # Relationships
    authors: list[Person] = []
    keywords: list[Keyword] = []

    def __str__(self):
        author_names = [author.name for author in self.authors]
        authors_str = ", ".join(author_names) if author_names else "No authors"

        keyword_names = [keyword.name for keyword in self.keywords]
        keywords_str = ", ".join(keyword_names) if keyword_names else "No keywords"

        return f"Paper: {self.title} (Authors: {authors_str}, Keywords: {keywords_str})"
