from dataclasses import dataclass


@dataclass
class Person:
    name: str

    def __str__(self):
        return f"Person: {self.name}"
