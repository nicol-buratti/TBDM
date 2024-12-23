import logging
from abc import abstractmethod

from models.paper import Keyword, Paper
from models.people import Person
from models.volume import Volume


class Neo4jDatabase:

    @abstractmethod
    def create_volume(volume: Volume):
        logging.info("Attempting to create a volume node with title: %s", volume.title)

        try:
            # Ensure editors are created or retrieved
            editors = [
                Person.get_or_create(name=editor)[0] for editor in volume.voleditor
            ]

            # Ensure papers are created or retrieved
            papers = [Neo4jDatabase.create_paper(paper) for paper in volume.papers]

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
            raise

    @abstractmethod
    def create_paper(paper: Paper):
        logging.info("Attempting to create a paper node with id: %s", paper.url)

        try:
            # Ensure authors and keywords are created or retrieved
            authors = [Person.get_or_create(name=author)[0] for author in paper.authors]
            keywords = [
                Keyword.get_or_create(name=keyword)[0] for keyword in paper.keywords
            ]

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
