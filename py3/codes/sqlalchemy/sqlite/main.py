from datetime import datetime
from loguru import logger

from .repository import LiteRepository
from .schema import Person
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
        name="X",
        gender=PersonGenderType.MALE,
        birth_date=datetime.strptime("1977-08-09", "%Y-%m-%d"),
    )
    p3 = Person(
        id="p3",
        name="X",
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


if __name__ == "__main__":
    repo = LiteRepository()
    repo.connect_to_db()

    repo.delete_all_persons()
    repo.add_persons(_get_person_samples())

    _cal_age(repo)

    repo.close_db_connection()
