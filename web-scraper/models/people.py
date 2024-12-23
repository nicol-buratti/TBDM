from neomodel import (
    config,
    StructuredNode,
    StringProperty,
    StructuredRel,
    IntegerProperty,
    UniqueIdProperty,
    RelationshipTo,
)


class Person(StructuredNode):
    name = StringProperty()

    def to_dict(self):
        return {"name": self.name}

    def __str__(self):
        return f"Person: {self.name}"
