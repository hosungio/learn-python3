import pandas as pd
from datetime import datetime
from loguru import logger

from .repository import MariadbRepository
from .schema import Person, PersonJsonDoc
from .model import PersonGenderType
from .settings import get_app_settings


def _get_person_samples() -> list[Person]:
    p1 = Person.create("p-1", "Foo", PersonGenderType.MALE, "1944-05-06", 170.5, 80.1)
    p2 = Person.create("p-2", "Bar", PersonGenderType.FEMALE, "1966-07-08", 163.4, 60.2)
    p3 = Person.create("p-3", "Ekta", PersonGenderType.MALE, "1977-08-09", 180.3, 73.3)
    p4 = Person.create("p-4", "Mega", PersonGenderType.MALE, "1988-09-10", 183.2, 80.4)
    p5 = Person.create(
        "p-5", "Zeta", PersonGenderType.FEMALE, "2002-03-04", 167.1, 50.5
    )
    return [p1, p2, p3, p4, p5]


def _get_person_json_doc_samples(persons: list[Person]) -> list[PersonJsonDoc]:
    return [PersonJsonDoc.create(p) for p in persons]
    # doc1 = PersonJsonDoc(
    #     id="doc-1",
    #     creation_time=datetime.now(),
    #     doc={"name": "foo", "age": 44, "car": None},
    # )
    # doc2 = PersonJsonDoc(
    #     id="doc-2",
    #     creation_time=datetime.now(),
    #     doc={"name": "bar", "age": 55, "car": None},
    # )
    # doc3 = PersonJsonDoc(
    #     id="doc-3",
    #     creation_time=datetime.now(),
    #     doc={"name": "zoo", "age": 66, "car": "z"},
    # )
    # return [doc1, doc2, doc3]


def _cal_age(repo: MariadbRepository) -> None:
    now = datetime.now()
    persons = repo.get_all_persons()
    for p in persons:
        age = now.year - p.birth_date.year
        age_group = int(age / 10) * 10
        logger.debug(f"{now.strftime('%Y-%m-%d')} {p.birth_date} {age} {age_group}")


def test_json_extract_from_docs(repo: MariadbRepository) -> None:
    path_list = ["$.name", "$.gender", "$.birth_date", "$.height", "$.weight", "$.bmi"]
    keys, record_list = repo.json_extract_from_docs(path_list)
    logger.debug(f"json_extract_from_docs: {path_list}")
    logger.debug(f"  keys: {keys}")
    logger.debug(f"  record_list: {record_list}")
    df = pd.DataFrame(record_list, columns=keys)
    df.info()
    print()
    print(df)
    logger.debug("")

    # Check field order.
    path_list = ["$.bmi", "$.gender", "$.birth_date"]
    keys, record_list = repo.json_extract_from_docs(path_list)
    logger.debug(f"json_extract_from_docs: {path_list}")
    logger.debug(f"  keys: {keys}")
    logger.debug(f"  record_list: {record_list}")
    df = pd.DataFrame(record_list, columns=keys)
    df.info()
    print()
    print(df)
    logger.debug("")

    # Check not exist field.
    path_list = [
        "$.name",
        "$.not.exist.field",
        "$.gender",
        "$.birth_date",
        "$.height",
        "$.weight",
        "$.bmi",
    ]
    keys, record_list = repo.json_extract_from_docs(path_list)
    logger.debug(f"json_extract_from_docs: {path_list}")
    logger.debug(f"  keys: {keys}")
    logger.debug(f"  record_list: {record_list}")
    df = pd.DataFrame(record_list, columns=keys)
    df.info()
    print()
    print(df)
    logger.debug("")


def test_json_extract_from_column(repo: MariadbRepository) -> None:
    table_name = PersonJsonDoc.__tablename__
    column_name = "doc"
    path_list = ["$.name", "$.gender", "$.birth_date", "$.height", "$.weight", "$.bmi"]
    keys, record_list = repo.json_extract_from_column(
        table_name, column_name, path_list
    )
    logger.debug(
        f"test_json_extract_from_column: {table_name}, {column_name}, {path_list}"
    )
    logger.debug(f"  keys: {keys}")
    logger.debug(f"  record_list: {record_list}")
    df = pd.DataFrame(record_list, columns=keys)
    df.info()
    print()
    print(df)
    logger.debug("")

    # Check field order.
    table_name = PersonJsonDoc.__tablename__
    column_name = "doc"
    path_list = ["$.bmi", "$.gender", "$.birth_date"]
    keys, record_list = repo.json_extract_from_column(
        table_name, column_name, path_list
    )
    logger.debug(
        f"test_json_extract_from_column: {table_name}, {column_name}, {path_list}"
    )
    logger.debug(f"  keys: {keys}")
    logger.debug(f"  record_list: {record_list}")
    df = pd.DataFrame(record_list, columns=keys)
    df.info()
    print()
    print(df)
    logger.debug("")

    # Check not exist field.
    table_name = PersonJsonDoc.__tablename__
    column_name = "doc"
    path_list = [
        "$.name",
        "$.not.exist.field",
        "$.gender",
        "$.birth_date",
        "$.height",
        "$.weight",
        "$.bmi",
    ]
    keys, record_list = repo.json_extract_from_column(
        table_name, column_name, path_list
    )
    logger.debug(
        f"test_json_extract_from_column: {table_name}, {column_name}, {path_list}"
    )
    logger.debug(f"  keys: {keys}")
    logger.debug(f"  record_list: {record_list}")
    df = pd.DataFrame(record_list, columns=keys)
    df.info()
    print()
    print(df)
    logger.debug("")


if __name__ == "__main__":
    repo = MariadbRepository()
    repo.connect_to_db()

    persons = _get_person_samples()
    docs = _get_person_json_doc_samples(persons)

    repo.delete_all_docs()
    repo.add_docs(docs)
    test_json_extract_from_column(repo)
    # test_json_extract_from_docs(repo)

    # repo.delete_all_persons()
    # repo.add_persons(persons)
    # _cal_age(repo)

    repo.close_db_connection()
