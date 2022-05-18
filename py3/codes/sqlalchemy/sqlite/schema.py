from sqlalchemy.orm import registry
from sqlalchemy import Column, String, Date, Enum

from .model import PersonGenderType

mapper_registry = registry()


@mapper_registry.mapped
class Person:
    __tablename__ = "person"

    id = Column(String(36), primary_key=True, nullable=False, index=True)
    name = Column(String(128), nullable=False, index=True)
    gender = Column(
        Enum(PersonGenderType, native_enum=False, length=16), nullable=False
    )
    birth_date = Column(Date, nullable=False)
