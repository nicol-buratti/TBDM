import logging
from abc import abstractmethod

from models.paper import Keyword, Paper
from models.people import Person
from models.volume import Volume
from neomodel import db


class Neo4jDatabase:

    @abstractmethod
    def create_volume(volume: Volume):
        logging.info("Attempting to create the volume node: %s", volume.volnr)

        try:
            # Ensure editors are created or retrieved
            editors = get_or_create_voleditors(volume.voleditor)
            volume.voleditor = None

            # Ensure papers are created or retrieved
            papers = [Neo4jDatabase.create_paper(paper) for paper in volume.papers]
            volume.papers = None

            # Save the volume node itself
            volume.save()

            # Connect relationships (editor relationships)
            for editor in editors:
                volume.voleditor.connect(editor)

            # Connect relationships (paper relationships)
            for paper in papers:
                volume.papers.connect(paper)

            logging.info(
                "Volume node with title: %s created successfully.", volume.title
            )
            return volume

        except Exception as e:
            logging.error(
                "Failed to create volume node with title: %s. Error: %s",
                volume.volnr,
                str(e),
            )

    @abstractmethod
    def create_paper(paper: Paper):
        logging.info("Attempting to create a paper node with id: %s", paper.url)

        try:
            # Ensure authors and keywords are created or retrieved
            authors = get_or_create_authors(paper.authors)
            paper.authors = None

            keywords = get_or_create_keywords(paper.keywords)
            paper.keywords = None

            # Save the paper node itself
            paper.save()

            # Connect relationships
            for author in authors:
                paper.authors.connect(author)
            for keyword in keywords:
                paper.keywords.connect(keyword)

            logging.info("Paper node with id: %s created successfully.", paper.url)
            return paper

        except Exception as e:
            logging.error(
                "Failed to create paper node with id: %s. Error: %s", paper.url, str(e)
            )
            raise


def get_or_create_voleditors(editors_list):
    query = """
    UNWIND $names AS name
    MERGE (p:Person {name: name})
    RETURN p
    """

    # Parameters
    params = {"names": editors_list}

    # Execute the query
    results, _ = db.cypher_query(query, params)
    editors = [Person.inflate(row[0]) for row in results]

    return editors


def get_or_create_authors(authors_list):
    query = """
    UNWIND $names AS name
    MERGE (p:Person {name: name})
    RETURN p
    """

    # Parameters
    params = {"names": authors_list}

    # Execute the query
    results, _ = db.cypher_query(query, params)
    authors = [Person.inflate(row[0]) for row in results]

    return authors


def get_or_create_keywords(keywords):
    query = """
    UNWIND $names AS name
    MERGE (p:Keyword {name: name})
    RETURN p
    """

    # Parameters
    params = {"names": keywords}

    # Execute the query
    results, _ = db.cypher_query(query, params)
    keywords = [Keyword.inflate(row[0]) for row in results]

    return keywords
