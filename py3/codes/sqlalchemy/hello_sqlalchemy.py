from sqlalchemy import create_engine

class HelloRepository:
    def __init__(self) -> None:
        self._db_url = "sqlite:///hello.db"
        self._engine = None

    def connect_to_db(self) -> None:
        pass

    def close_db_connection(self) -> None:
        pass


if __name__ == "__main__":
    repo = HelloRepository()
    repo.connect_to_db()
    repo.close_db_connection()