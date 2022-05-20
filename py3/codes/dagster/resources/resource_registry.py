from typing import Any
from loguru import logger
from contextlib import contextmanager
from dagster import resource


class RunEnvironment:
    def __init__(self, user: str, working_dir: str) -> None:
        self._user = user
        self._working_dir = working_dir

    @property
    def user(self) -> str:
        return self._user

    @property
    def working_dir(self) -> str:
        return self._working_dir

    @staticmethod
    @resource(config_schema={"user": str, "working_dir": str})
    def resource_def_fn(init_context):
        user = init_context.resource_config["user"]
        working_dir = init_context.resource_config["working_dir"]
        return RunEnvironment(user, working_dir)


class DBConnection:
    def __init__(self) -> None:
        logger.info("DBConnection init")

    def __enter__(self) -> Any:
        logger.info("DBConnection enter")
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        logger.info("DBConnection exit")

    def __repr__(self) -> str:
        return "DBConnection(...)"

    def execute(self, sql: str) -> str:
        logger.info(f"Execute: {sql}")
        return "sql execution results"

    def close() -> None:
        logger.info("DBConnection close")


class DBConnectionManager:
    def __init__(self) -> None:
        logger.info("Init DBConnectionManager")

    def connect(self) -> DBConnection:
        return DBConnection()

    def dispose(self):
        logger.info("Dispose DBConnectionManager")

    @staticmethod
    @resource
    @contextmanager
    def resource_def_fn():
        db: DBConnectionManager = None
        try:
            db = DBConnectionManager()
            yield db
        finally:
            if db:
                db.dispose()


_DEFAULT_RESOURCE_DEFS = {
    "run_env": RunEnvironment.resource_def_fn,
    "db": DBConnectionManager.resource_def_fn,
}


def get_default_resource_defs():
    return _DEFAULT_RESOURCE_DEFS


def get_default_resource_keys() -> set[str]:
    return frozenset(_DEFAULT_RESOURCE_DEFS.keys())
