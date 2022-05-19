import ast
from typing import Any, Tuple
from loguru import logger
from sqlalchemy import create_engine, inspect, func
from sqlalchemy.orm import scoped_session, sessionmaker, Query
from sqlalchemy.orm.session import Session

from .schema import Person, PersonJsonDoc
from .settings import get_app_settings


class MariadbRepository:
    def __init__(self) -> None:
        self._settings = get_app_settings()
        self._orm_list = [Person, PersonJsonDoc]
        self._engine = None
        self._inspect = None
        self._session = None

    def connect_to_db(self) -> None:
        if self._engine:
            logger.info("Database connection already established")
        else:
            logger.info("Connecting to database")
            self._engine = create_engine(
                self._settings.db_dsn,
                pool_size=self._settings.db_pool_size,
                pool_recycle=self._settings.db_pool_recycle,
                max_overflow=self._settings.db_max_overflow,
                echo=self._settings.db_echo,
            )
            self._inspect = inspect(self._engine)
            self._session = scoped_session(
                sessionmaker(autocommit=False, autoflush=False, bind=self._engine)
            )
            logger.debug("Check and create tables")
            for orm_class in self._orm_list:
                if not self._inspect.has_table(orm_class.__tablename__):
                    logger.debug(f"Creating table: name={orm_class.__tablename__}")
                    orm_class.__table__.create(bind=self._engine)
                    logger.debug(f"Table created: name={orm_class.__tablename__}")

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

    def add_docs(self, docs: list[PersonJsonDoc]) -> None:
        sess: Session = None
        with self._session() as sess:
            try:
                sess.add_all(docs)
                sess.commit()
            except Exception as ex:
                sess.rollback()
                raise ex

    def get_all_docs(self) -> list[PersonJsonDoc]:
        founds = []
        sess: Session = None
        with self._session() as sess:
            q: Query = sess.query(PersonJsonDoc)
            founds = q.all()
        return founds

    def delete_all_docs(self) -> int:
        count = 0
        sess: Session = None
        with self._session() as sess:
            try:
                q: Query = sess.query(PersonJsonDoc)
                count = q.delete()
                sess.commit()
            except Exception as ex:
                sess.rollback()
        return count

    def exec_sql(self, sql_text: str) -> Any:
        r = None
        sess: Session = None
        with self._session() as sess:
            r = sess.execute(sql_text).fetchall()
        return r

    def _eval_string_value(self, s: str) -> Any:
        if s:
            if s in ["null", "NULL"]:
                return None
            else:
                return ast.literal_eval(s)
        else:
            return None

    def json_extract_from_docs(
        self, path_list: list[str]
    ) -> Tuple[list[str], list[list[Any]]]:
        """
        > select json_extract(doc, '$.name'), json_extract(doc, '$.age'), json_extract(doc, '$.car') from test_json_doc;
        +-----------------------------+----------------------------+----------------------------+
        | json_extract(doc, '$.name') | json_extract(doc, '$.age') | json_extract(doc, '$.car') |
        +-----------------------------+----------------------------+----------------------------+
        | "foo"                       | 44                         | null                       |
        | "bar"                       | 55                         | null                       |
        | "zoo"                       | 66                         | "z"                        |
        +-----------------------------+----------------------------+----------------------------+
        """

        keys = [p[2:] for p in path_list]
        record_list = []
        sess: Session = None
        with self._session() as sess:
            extract_fns = [
                func.json_extract(PersonJsonDoc.doc, path) for path in path_list
            ]
            q: Query = sess.query(*extract_fns)
            rows = q.all()
            for r in rows:
                value_list = [self._eval_string_value(e) for e in r]
                logger.debug(f"convert:\n {str(r)} -> {value_list}")
                record_list.append(value_list)
        return (keys, record_list)

    def json_extract_from_column(
        self, table_name: str, column_name: str, path_list: list[str]
    ) -> Tuple[list[str], list[list[Any]]]:
        extract_list = [f"json_extract({column_name}, '{path}')" for path in path_list]
        extract_text = ",".join(extract_list)
        sql_text = f"select {extract_text} from {table_name}"
        logger.debug(f"extract_text: {extract_text}")
        logger.debug(f"sql_text: {sql_text}")

        keys = [p[2:] for p in path_list]
        record_list = []
        sess: Session = None
        with self._session() as sess:
            rows = sess.execute(sql_text).fetchall()
            for r in rows:
                value_list = [self._eval_string_value(e) for e in r]
                logger.debug(f"convert:\n {str(r)} -> {value_list}")
                record_list.append(value_list)
        return (keys, record_list)

    def json_extract_from_table(self):
        pass

    def json_value_from_docs(self) -> str:
        """
        Deprecated: We don't know a value type.

        > select json_value(doc, '$.age'), json_value(doc, '$.name') from test_json_doc;
        +--------------------------+---------------------------+`
        | json_value(doc, '$.age') | json_value(doc, '$.name') |
        +--------------------------+---------------------------+
        | 44                       | foo                       |
        | 55                       | bar                       |
        | 66                       | zoo                       |
        +--------------------------+---------------------------+`
        """
        pass
