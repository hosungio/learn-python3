import os
from pydantic import BaseSettings
from functools import lru_cache


class AppSettings(BaseSettings):
    db_dsn: str = os.environ[f"MARIADB176_DSN1"]
    db_pool_size: int = 10
    db_pool_recycle: int = 3600  # seconds.
    db_max_overflow: int = 10
    db_echo: bool = False


@lru_cache
def get_app_settings() -> AppSettings:
    return AppSettings()


if __name__ == "__main__":
    print(get_app_settings())
