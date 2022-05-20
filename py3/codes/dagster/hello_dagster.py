from dagster import op, OpExecutionContext, job
from loguru import logger


@op
def return_hello(context: OpExecutionContext) -> str:
    return "hello"


@op
def return_world(context: OpExecutionContext) -> str:
    return "world"


@op
def return_five(context: OpExecutionContext) -> int:
    return 5


@op
def concat_string(context: OpExecutionContext, s1: str, s2: str) -> str:
    return f"{s1} {s2}"


@op
def print_message(context: OpExecutionContext, message: str, count: int) -> None:
    print("-------------------------------------------------------------------")
    for i in range(count):
        print(i, message)
    print("-------------------------------------------------------------------")


@job
def print_hello_world():
    print_message(concat_string(return_hello(), return_world()), return_five())


if __name__ == "__main__":
    run_config = {"loggers": {"console": {"config": {"log_level": "INFO"}}}}
    print_hello_world.execute_in_process(run_config=run_config, run_id="run-1")
