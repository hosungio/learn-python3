import uvicorn
import time
import threading
from fastapi import FastAPI
from loguru import logger


app = FastAPI()


@app.get("/")
def get_root():
    return "Hello World"


@app.get("/echo")
def echo(msg: str, secs: int = 3):
    """다른 AnyIO worker thread가 처리해서 각각 처리됨"""
    logger.debug("echo:", threading.current_thread().name, threading.get_ident())
    time.sleep(secs)
    return msg


@app.get("/async-echo")
async def async_echo(msg: str, secs: int = 3):
    """같은 MainThread가 처리해서 이전 request가 완료되고 다음 request가 처리됨"""
    logger.debug("async-echo:", threading.current_thread().name, threading.get_ident())
    time.sleep(secs)
    return msg


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9090, reload=False)
