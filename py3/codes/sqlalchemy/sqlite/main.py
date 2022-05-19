import pandas as pd
from datetime import datetime
from loguru import logger

from .repository import LiteRepository
from .schema import JsonDoc, Person
from .model import PersonGenderType


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


def _cal_age(repo: LiteRepository) -> None:
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


def _extract_json(repo: LiteRepository) -> None:
    key, values = repo.extract_field_from_json("$.name")
    logger.debug("extract_field_from_json: $.name")
    logger.debug(f"  key: {key}")
    logger.debug(f"  values: {values}")
    df = pd.DataFrame(values, columns=[key])
    df.info()
    print()
    print(df)
    logger.debug("")

    key, values = repo.extract_field_from_json("$.age")
    logger.debug("extract_field_from_json: $.age")
    logger.debug(f"  key: {key}")
    logger.debug(f"  values: {values}")
    df = pd.DataFrame(values, columns=[key])
    df.info()
    print()
    print(df)
    logger.debug("")

    keys, record_list = repo.extract_fields_from_json("$.name", "$.age", "$.car")
    logger.debug("extract_fields_from_json")
    logger.debug(f"  keys: {keys}")
    logger.debug(f"  record_list: {record_list}")
    df = pd.DataFrame(record_list, columns=keys)
    df.info()
    print()
    print(df)
    logger.debug("")


def _exec_sql(repo: LiteRepository) -> None:
    sql_text = "select * from json_doc"
    r = repo.exec_sql(sql_text)
    logger.debug(sql_text)
    print(r)
    logger.debug("")

    sql_text = "select json_extract(json_doc.doc, '$.name', '$.age') from json_doc"
    r = repo.exec_sql(sql_text)
    logger.debug(sql_text)
    print(r)
    logger.debug("")


def _exec_json_extract(repo: LiteRepository) -> None:
    keys, record_list = repo.exec_json_extract(
        "json_doc", "doc", ["$.name", "$.age", "$.car"]
    )
    logger.debug("exec_json_extract")
    logger.debug(f"  keys: {keys}")
    logger.debug(f"  record_list: {record_list}")
    df = pd.DataFrame(record_list, columns=keys)
    df.info()
    print()
    print(df)
    logger.debug("")


if __name__ == "__main__":
    repo = LiteRepository()
    repo.connect_to_db()

    repo.delete_all_docs()
    repo.add_docs(_get_jsondoc_samples())
    _exec_json_extract(repo)
    # _exec_sql(repo)
    # _extract_json(repo)

    # repo.delete_all_persons()
    # repo.add_persons(_get_person_samples())
    # _cal_age(repo)

    repo.close_db_connection()
