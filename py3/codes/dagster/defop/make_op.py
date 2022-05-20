from loguru import logger
from dagster import op, OpExecutionContext, job, graph

from codes.dagster.resources.resource_registry import (
    get_default_resource_keys,
    RunEnvironment,
    DBConnectionManager,
    get_default_resource_defs,
)

# fmt: off
@op(
    # required_resource_keys=get_default_resource_keys()
    required_resource_keys=get_default_resource_keys()
)
# fmt: on
def do_something(context: OpExecutionContext):
    logger.info("do something")

    run_env: RunEnvironment = context.resources.run_env
    logger.info(f"run_env: user={run_env.user}, working_dir={run_env.working_dir}")

    db: DBConnectionManager = context.resources.db
    with db.connect() as conn:
        conn.execute("select * from table")


@job(resource_defs=get_default_resource_defs())
def do_something_job():
    do_something()


@graph
def do_something_graph():
    do_something()


if __name__ == "__main__":
    # fmt: off
    run_config = {
        "loggers": {
            "console": {
                "config": {
                    "log_level": "INFO"
                }
            }
        },
        "resources": {
            "run_env": {
                "config": {
                    "user": "foo", 
                    "working_dir": "bar"
                }
            }
        },
    }
    # fmt: on

    r1 = do_something_job.execute_in_process(run_config=run_config, run_id="run-1")
    logger.info(
        f"r1: {r1.run_id}, {r1.dagster_run.pipeline_name}, {r1.dagster_run.is_success}"
    )
    print()

    job = do_something_graph.to_job(
        name="do_something_graph", resource_defs=get_default_resource_defs()
    )
    r2 = job.execute_in_process(run_config=run_config, run_id="run-1-by-graph")
    logger.info(
        f"r2: {r2.run_id}, {r2.dagster_run.pipeline_name}, {r2.dagster_run.is_success}"
    )
