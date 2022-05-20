import pandas as pd
from loguru import logger


df = pd.DataFrame([["1"], ["2"], ["3"]], columns=["number"])
df.info()
df["number"] = pd.to_numeric(df["number"])
df.info()
logger.info("string integer -> int64")


df = pd.DataFrame([["1.1"], ["2.2"], ["3.3"]], columns=["number"])
df.info()
df["number"] = pd.to_numeric(df["number"])
df.info()
logger.info("string integer -> float64")


df = pd.DataFrame([['"foo"'], ['"bar"'], ['"zoo"']], columns=["text"])
df.info()
print(df)
df["text"] = df["text"].str.replace('"', "").astype(str)
df.info()
print(df)
logger.info("text -> string")


df = pd.DataFrame([["1944-05-06"], ["1966-07-08"], ["1977-08-09"]], columns=["date"])
df.info()
df["date"] = pd.to_datetime(df["date"])
df.info()
print(df)
logger.info("string date -> datetime")


df = pd.DataFrame(
    [
        ["2022-05-20 17:04:45.593"],
        ["2022-05-20 17:04:45.593"],
        ["2022-05-20 17:04:45.593"],
    ],
    columns=["datetime"],
)
df.info()
df["datetime"] = pd.to_datetime(df["datetime"])
df.info()
print(df)
logger.info("string datetime -> datetime")
