class Paper:
    def __init__(self, url, title, pages, author, abstract, keywords):
        self.url = url
        self.title = title
        self.pages = pages
        self.author = author
        self.abstract = abstract
        self.keywords = keywords

    def to_dict(self):
        return {
            "url": self.url,
            "title": self.title,
            "pages": self.pages,
            "author": self.author,
            "abstract": self.abstract,
            "keywords": self.keywords,
        }
