from sqlalchemy.orm import registry
from sqlalchemy import Column, String, Date, Enum, Text
from sqlalchemy.dialects.sqlite import DATETIME, JSON

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


@mapper_registry.mapped
class JsonDoc:
    __tablename__ = "test_json_doc"

    id = Column(String(36), primary_key=True, nullable=False, index=True)
    creation_time = Column(DATETIME(timezone=True), nullable=False)
    doc = Column(JSON)
