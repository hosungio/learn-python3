from contextlib import contextmanager
from typing import Any
from loguru import logger
from dagster import resource, job, op, OpExecutionContext


class FooSession:
    def __init__(self) -> None:
        logger.info("Init session")

    def __enter__(self) -> Any:
        logger.info("Enter session")
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        logger.info("Exit session")

    def __repr__(self) -> str:
        return "FooSession(...)"

    def read(self) -> str:
        logger.info("do reading")
        return "hello world"

    def write(self, something: str) -> None:
        logger.info(f"do writing: {something}")


class FooSessionManager:
    def __init__(self) -> None:
        logger.info("Init session manager")

    def get_session(self) -> FooSession:
        return FooSession()

    def dispose(self):
        logger.info("Dispose session manager")


@resource
@contextmanager
def session_manager():
    manager = None
    try:
        manager = FooSessionManager()
        yield manager
    finally:
        manager.dispose()


@op(required_resource_keys={"session_manager"})
def read(context: OpExecutionContext) -> str:
    logger.debug("read op start")
    something = None
    with context.resources.session_manager.get_session() as sess:
        something = sess.read()
    logger.debug("read op stop")
    return something


@op(required_resource_keys={"session_manager"})
def write(context: OpExecutionContext, something: str) -> None:
    logger.debug("write op start")
    with context.resources.session_manager.get_session() as sess:
        sess.write(something)
    logger.debug("write op stop")


@job(resource_defs={"session_manager": session_manager})
def do_session_job():
    write(read())


if __name__ == "__main__":
    run_config = {"loggers": {"console": {"config": {"log_level": "INFO"}}}}
    do_session_job.execute_in_process(run_config=run_config, run_id="run-1")
