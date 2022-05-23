import os
import pandas as pd
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_number,
    is_integer,
    is_int64_dtype,
    is_numeric_dtype,
)
from sqlalchemy import create_engine
from datetime import datetime


def cal_age_from_birth_date(datetime_series):
    today = datetime.now()
    return today.year - datetime_series.dt.year


def cal_age_group_from_age(age_series):
    age_group = (age_series / 10).astype("int") * 10
    return age_group


def add_age_col(df, birth_date_col_name):
    if birth_date_col_name in df.columns:
        print("Brith date column exist")
        birth_date_col = df[birth_date_col_name]
        if is_datetime64_any_dtype(birth_date_col):
            print("Brith date column is_datetime64_any_dtype")
            df["age"] = cal_age_from_birth_date(birth_date_col)
            df.info()
            print(df)
            print()
    else:
        print("Brith date column not found")
    return df


def add_age_group_col(df, age_col_name):
    if age_col_name in df.columns:
        print("Age column exist:")
        age_col = df[age_col_name]
        # print(">> is_number:", is_number(age_col)) # False
        # print(">> is_integer:", is_integer(age_col)) # False
        # print(">> is_int64_dtype:", is_int64_dtype(age_col)) # True
        # print(">> is_numeric_dtype:", is_numeric_dtype(age_col)) # True
        if is_numeric_dtype(age_col):
            print("Age column is_number")
            df["age_group"] = cal_age_group_from_age(age_col)
            df.info()
            print(df)
            print()
    else:
        print("Age column not found")
    return df


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

        df = add_age_col(df, "birth_date")

        df = add_age_group_col(df, "age")

        # birth_date_col = "birth_date"
        # if birth_date_col in df.columns:
        #     df["age"] = cal_age_from_birth_date(df[birth_date_col])
        #     df.info()
        #     print(df)
        #     print()
        # else:
        #     print("Brith date column not found")

        # df["age_group"] = cal_age_group_from_age(df["age"])
        # df.info()
        # print(df)
        # print()

    engine.dispose()
