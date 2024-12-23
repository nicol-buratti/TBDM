import json
import logging
import sys

from neo4j import GraphDatabase
from neomodel import db
from neo4j.exceptions import ServiceUnavailable
from pyspark.sql import SparkSession
from graphframes import GraphFrame
import random

from neomodel import (
    config,
    StructuredNode,
    StringProperty,
    StructuredRel,
    IntegerProperty,
    UniqueIdProperty,
    RelationshipTo,
)

import os

from models.volume import Volume


class Neo4jDatabase:

    logging.basicConfig(
        filename="database.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    def __init__(self, uri):
        db.set_connection(uri)

    def create_volume(volume: Volume):
        logging.info("Attempting to create a Volume node with id: %s", volume.volnr)

    def create_paper(paper_id):
        logging.info("Attempting to create a paper node with id: %s", paper_id)
