from typing import Type
from datetime import datetime
from sqlalchemy.orm import registry
from sqlalchemy import Column, String, Date, Enum, FLOAT
from sqlalchemy.dialects.mysql import DATETIME, JSON

from .model import PersonGenderType

mapper_registry = registry()


@mapper_registry.mapped
class Person:
    __tablename__ = "test_person"

    id = Column(String(36), primary_key=True, nullable=False, index=True)
    name = Column(String(128), nullable=False, index=True)
    gender = Column(
        Enum(PersonGenderType, native_enum=False, length=16), nullable=False
    )
    birth_date = Column(Date, nullable=False)
    height = Column(FLOAT)
    weight = Column(FLOAT)
    bmi = Column(FLOAT)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "gender": self.gender.value,
            "birth_date": datetime.strftime(self.birth_date, "%Y-%m-%d"),
            "height": self.height,
            "weight": self.weight,
            "bmi": self.bmi,
        }

    @classmethod
    def create(
        cls,
        id: str,
        name: str,
        gender: PersonGenderType,
        birth_date_str: str,
        height: float,
        weight: float,
        bmi: float = None,
    ):
        return Person(
            id=id,
            name=name,
            gender=gender,
            birth_date=datetime.strptime(birth_date_str, "%Y-%m-%d"),
            height=height,
            weight=weight,
            bmi=bmi,
        )


@mapper_registry.mapped
class PersonJsonDoc:
    __tablename__ = "test_person_json_doc"

    id = Column(String(36), primary_key=True, nullable=False, index=True)
    creation_time = Column(DATETIME(timezone=True, fsp=6), nullable=False)
    doc = Column(JSON)

    @classmethod
    def create(cls, p: Person):
        return PersonJsonDoc(id=p.id, creation_time=datetime.now(), doc=p.to_dict())
