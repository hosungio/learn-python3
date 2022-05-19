import pandas as pd
from datetime import datetime
from loguru import logger

from .repository import MariadbRepository
from .schema import JsonDoc, Person
from .model import PersonGenderType
from .settings import get_app_settings


def _get_person_samples() -> list[Person]:
    p1 = Person(
        id="p1",
        name="Foo",
        gender=PersonGenderType.MALE,
        birth_date=datetime.strptime("1944-05-06", "%Y-%m-%d"),
    )
    p2 = Person(
        id="p2",
        name="Bar",
        gender=PersonGenderType.FEMALE,
        birth_date=datetime.strptime("1966-07-07", "%Y-%m-%d"),
    )
    p3 = Person(
        id="p3",
        name="Zoo",
        gender=PersonGenderType.MALE,
        birth_date=datetime.strptime("1977-08-09", "%Y-%m-%d"),
    )
    return [p1, p2, p3]


def _cal_age(repo: MariadbRepository) -> None:
    now = datetime.now()
    persons = repo.get_all_persons()
    for p in persons:
        age = now.year - p.birth_date.year
        age_group = int(age / 10) * 10
        logger.debug(f"{now.strftime('%Y-%m-%d')} {p.birth_date} {age} {age_group}")


def _get_jsondoc_samples() -> list[JsonDoc]:
    doc1 = JsonDoc(
        id="doc-1",
        creation_time=datetime.now(),
        doc={"name": "foo", "age": 44, "car": None},
    )
    doc2 = JsonDoc(
        id="doc-2",
        creation_time=datetime.now(),
        doc={"name": "bar", "age": 55, "car": None},
    )
    doc3 = JsonDoc(
        id="doc-3",
        creation_time=datetime.now(),
        doc={"name": "zoo", "age": 66, "car": "z"},
    )
    return [doc1, doc2, doc3]


def test_json_extract_from_docs(repo: MariadbRepository) -> None:
    path_list = ["$.name", "$.age", "$.car"]
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
    path_list = ["$.age", "$.car", "$.name"]
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
        "$.age",
        "$.not.exist.field",
        "$.car",
        "$.name",
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
    table_name = "test_json_doc"
    column_name = "doc"
    path_list = ["$.name", "$.age", "$.car"]
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
    table_name = "test_json_doc"
    column_name = "doc"
    path_list = ["$.age", "$.car", "$.name"]
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
    table_name = "test_json_doc"
    column_name = "doc"
    path_list = [
        "$.age",
        "$.not.exist.field",
        "$.car",
        "$.name",
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

    repo.delete_all_docs()
    repo.add_docs(_get_jsondoc_samples())
    test_json_extract_from_column(repo)
    # test_json_extract_from_docs(repo)

    # repo.delete_all_persons()
    # repo.add_persons(_get_person_samples())
    # _cal_age(repo)

    repo.close_db_connection()
