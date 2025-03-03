import logging

from models.paper import Keyword, Paper
from models.people import Person
from models.volume import Volume
from neomodel import db


class Neo4jDatabase:

    @staticmethod
    def create_volume(volume: Volume):
        logging.debug("Attempting to create the volume node: %s", volume.volnr)

        try:
            # Before saving a Volume object, its relationships must be saved
            editor_names = [e.name for e in volume.voleditors]
            editors = get_or_create_person(editor_names)
            editors = editors if editors else []
            papers = [Neo4jDatabase.create_paper(paper) for paper in volume.papers]

            # Remove the relationships from the Volume object in order for it to be saved correctly
            dic = volume.__dict__
            del dic["voleditors"]
            del dic["papers"]
            volume = Volume(**dic).save()

            # Connect relationships (editor relationships)
            for editor in editors:
                volume.voleditors.connect(editor)

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

    @staticmethod
    def create_paper(paper: Paper):
        logging.debug("Attempting to create a paper node with id: %s", paper.url)

        try:
            # Save the authors and keywords
            authors = get_or_create_person(paper.authors)
            keywords = get_or_create_keywords(paper.keywords)

            dic = paper.to_dict()
            del dic["keywords"]
            del dic["authors"]
            # Save the paper node itself
            paper = Paper(**dic).save()

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


def get_or_create_person(people_list):
    try:
        query = """
        UNWIND $names AS name
        MERGE (p:Person {name: name})
        RETURN p
        """

        # Parameters
        params = {"names": people_list}

        # Execute the query
        results, _ = db.cypher_query(query, params)
        people = [Person.inflate(row[0]) for row in results]
        return people
    except Exception as e:
        logging.error(f"Failed to save Person object. Error:{str(e)}")


def get_or_create_keywords(keywords):
    try:
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
    except Exception as e:
        logging.error(f"Failed to save Keyword object. Error:{str(e)}")
