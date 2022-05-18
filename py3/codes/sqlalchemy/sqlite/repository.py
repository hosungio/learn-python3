import os
from loguru import logger
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import scoped_session, sessionmaker, Query
from sqlalchemy.orm.session import Session

from .schema import Person


class LiteRepository:
    def __init__(self) -> None:
        self._db_dir = "/tmp/learn-py3"
        self._db_url = f"sqlite:///{self._db_dir}/lite.db"
        self._engine = None
        self._inspect = None
        self._session = None
        if not os.path.exists(self._db_dir):
            os.makedirs(self._db_dir)

    def connect_to_db(self) -> None:
        if self._engine:
            logger.info("Database connection already established")
        else:
            logger.info("Connecting to database")
            self._engine = create_engine(self._db_url, echo=False)
            self._inspect = inspect(self._engine)
            self._session = scoped_session(
                sessionmaker(autocommit=False, autoflush=False, bind=self._engine)
            )
            logger.debug("Check and create tables")
            if not self._inspect.has_table(Person.__tablename__):
                Person.__table__.create(bind=self._engine)

    def close_db_connection(self) -> None:
        if self._engine:
            logger.info("Closing database connection")
            self._session.close_all()
            self._engine.dispose()
            logger.info("Database connection closed")
        else:
            logger.info("Database connection already closed")

    def add_persons(self, persons: list[Person]) -> None:
        sess: Session = None
        with self._session() as sess:
            try:
                sess.add_all(persons)
                sess.commit()
            except Exception as ex:
                sess.rollback()
                raise ex

    def get_all_persons(self) -> list[Person]:
        founds = []
        sess: Session = None
        with self._session() as sess:
            q: Query = sess.query(Person)
            founds = q.all()
        return founds

    def delete_all_persons(self) -> int:
        count = 0
        sess: Session = None
        with self._session() as sess:
            try:
                q: Query = sess.query(Person)
                count = q.delete()
                sess.commit()
            except Exception as ex:
                sess.rollback()
        return count
