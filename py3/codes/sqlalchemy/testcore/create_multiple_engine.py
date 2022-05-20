import os
from sqlalchemy import create_engine


if __name__ == "__main__":
    url = os.environ[f"MARIADB176_DSN1"]
    engine1 = create_engine(url)
    engine2 = create_engine(url)

    print("engine1:", engine1)
    print("engine2:", engine2)

    engine1.dispose()
    engine2.dispose()
