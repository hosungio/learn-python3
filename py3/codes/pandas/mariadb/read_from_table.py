import os
import pandas as pd
from sqlalchemy import create_engine


if __name__ == "__main__":
    engine = create_engine(os.environ[f"MARIADB176_DSN1"])

    with engine.connect() as conn:
        df = pd.read_sql("select * from test_person", conn)
        df.info()
        print(df)
        print()

        df = pd.read_sql("select gender, height from test_person", conn)
        df.info()
        print(df)
        print()

        df = pd.read_sql("select * from test_person where gender = 'MALE'", conn)
        df.info()
        print(df)
        print()

    with engine.connect() as conn:
        df = pd.read_sql(
            "select json_extract(doc, '$.name'), json_extract(doc, '$.height') from test_person_json_doc",
            conn,
        )
        df.info()
        print(df)
        df.columns = ["name", "height"]
        df["name"] = df["name"].str.replace('"', "")
        df["height"] = pd.to_numeric(df["height"])
        df.info()
        print(df)
        print()

        df = pd.read_sql(
            "select json_extract(doc, '$') from test_person_json_doc",
            conn,
        )
        df.info()
        print(df)
        print()

        df = pd.read_sql(
            "select json_extract(doc, '$') from test_person_json_doc where json_extract(doc, '$.gender') = 'MALE'",
            conn,
        )
        df.info()
        print(df)
        print()

    engine.dispose()
