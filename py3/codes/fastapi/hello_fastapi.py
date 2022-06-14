import enum
import uvicorn
from typing import Optional
from fastapi import FastAPI
from loguru import logger


app = FastAPI()


@app.get("/jobs")
def get_jobs(user: Optional[str] = None, name: Optional[str] = None) -> str:
    rsp = f"Get jobs: user={user}, name={name}"
    logger.info(rsp)
    return rsp


@app.post("/jobs/{id}/start")
def start(id: str) -> str:
    logger.info(f"Start {id}")
    return f"Start {id}"


@app.post("/jobs/{id}/stop")
def stop(id: str) -> str:
    logger.info(f"Stop {id}")
    return f"Stop {id}"


class TaskActionType(str, enum.Enum):
    start = "start"
    stop = "stop"


@app.post("/tasks/{id}:{action}")
def do_action(id: str, action: TaskActionType) -> str:
    logger.info(f"Do: id={id}, action={action}")
    return f"Do: id={id}, action={action}"
