import os
import json
from typing import Any, Tuple
from loguru import logger
from sqlalchemy import create_engine, inspect, func
from sqlalchemy.engine import Row
from sqlalchemy.orm import scoped_session, sessionmaker, Query
from sqlalchemy.orm.session import Session

from .schema import Person, JsonDoc


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
            if not self._inspect.has_table(JsonDoc.__tablename__):
                JsonDoc.__table__.create(bind=self._engine)

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

    def add_docs(self, docs: list[JsonDoc]) -> None:
        sess: Session = None
        with self._session() as sess:
            try:
                sess.add_all(docs)
                sess.commit()
            except Exception as ex:
                sess.rollback()
                raise ex

    def get_all_docs(self) -> list[JsonDoc]:
        founds = []
        sess: Session = None
        with self._session() as sess:
            q: Query = sess.query(JsonDoc)
            founds = q.all()
        return founds

    def delete_all_docs(self) -> int:
        count = 0
        sess: Session = None
        with self._session() as sess:
            try:
                q: Query = sess.query(JsonDoc)
                count = q.delete()
                sess.commit()
            except Exception as ex:
                sess.rollback()
        return count

    def extract_field_from_json(self, path: str) -> Tuple[str, list[Any]]:
        """
        sqlite> select json_extract(json_doc.doc, '$.name') from json_doc;
        John
        """
        key = path[2:]
        values = []
        sess: Session = None
        with self._session() as sess:
            q: Query = sess.query(func.json_extract(JsonDoc.doc, path))
            rows: list[Row] = q.all()
            for r in rows:
                r_val = r[0]
                logger.debug(f"r_val: {type(r_val)}, [{r_val}]")
                values.append(r_val)
        return (key, values)

    def extract_fields_from_json(
        self, *paths: str
    ) -> Tuple[list[str], list[list[Any]]]:
        """
        sqlite> select json_extract(doc, '$.name', '$.age') from json_doc;
        ["John",30]
        sqlite> select json_extract(doc, '$.age', '$.name') from json_doc;
        [30,"John"]
        """
        keys = [p[2:] for p in paths]
        record_list = []
        sess: Session = None
        with self._session() as sess:
            q: Query = sess.query(func.json_extract(JsonDoc.doc, *paths))
            rows: list[Row] = q.all()
            for r in rows:
                r_val = r[0]
                logger.debug(f"r_val: {type(r_val)}, [{r_val}]")
                record_list.append(json.loads(r_val))
        return (keys, record_list)

    def exec_sql(self, sql_text: str):
        results = None
        sess: Session = None
        with self._session() as sess:
            results = sess.execute(sql_text).fetchall()
        return results

    def exec_json_extract(
        self, table_name: str, json_column_name: str, json_path_list: list[str]
    ) -> Tuple[list[str], list[list[Any]]]:
        json_path_expr = str(json_path_list)[1:-1]
        sql_text = f"select json_extract({json_column_name}, {json_path_expr}) from {table_name}"
        logger.debug(f"json_path_expr: {json_path_expr}")
        logger.debug(f"sql_text: {sql_text}")

        keys = [p[2:] for p in json_path_list]
        sess: Session = None
        record_list = []
        with self._session() as sess:
            rows = sess.execute(sql_text).fetchall()
            for r in rows:
                r_val = r[0]
                logger.debug(f"r_val: {type(r_val)}, [{r_val}]")
                record_list.append(json.loads(r_val))
        return (keys, record_list)
