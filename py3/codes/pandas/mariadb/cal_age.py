import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


def cal_age_from_birth_date(datetime_series):
    today = datetime.now()
    return today.year - datetime_series.dt.year


def cal_age_group_from_age(age_series):
    age_group = (age_series / 10).astype("int") * 10
    return age_group


cal_age_from_birth_date_code = """
def cal_age_from_birth_date(datetime_series):
    today = datetime.now()
    return today.year - datetime_series.dt.year
"""

if __name__ == "__main__":
    engine = create_engine(os.environ[f"MARIADB176_DSN1"])

    with engine.connect() as conn:
        df = pd.read_sql("select * from test_person", conn)
        df.info()
        print(df)
        print()

        df["birth_date"] = pd.to_datetime(df["birth_date"])
        df.info()
        print(df)
        print()

        # today = datetime.now()
        # df["age"] = today.year - df["birth_date"].dt.year
        df["age"] = cal_age_from_birth_date(df["birth_date"])
        df.info()
        print(df)
        print()

        df["age_group"] = cal_age_group_from_age(df["age"])
        df.info()
        print(df)
        print()

    engine.dispose()
