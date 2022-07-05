from functools import lru_cache
from pydantic import BaseSettings


class HelloSettings(BaseSettings):
    app_name: str = "hello example"
    admin_email: str
    items_per_user: int = 50

    class Config:
        env_file = "hello_fastapi.env"


@lru_cache
def get_hello_settings():
    return HelloSettings()
