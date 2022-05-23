from typing import Any
from pandas.core.frame import DataFrame as PandasDataFrame
from dagster import DagsterType, TypeCheckContext
from dagster.core.types.dagster_type import TypeCheckFn


class OpDataType(DagsterType):
    def __init__(self, type_check_fn: TypeCheckFn, key: str = None) -> None:
        super().__init__(type_check_fn=type_check_fn, key=key)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    @staticmethod
    def save_to_file(data: Any, path: str):
        raise NotImplementedError(f"save_to_file method in {__class__}")


class PandasDataFrameType(OpDataType):
    def __init__(self):
        super().__init__(
            type_check_fn=PandasDataFrameType._type_check_fn,
            key=PandasDataFrameType.type_key(),
        )

    @staticmethod
    def type_key() -> str:
        return f"{__name__}.{__class__.__name__}"

    @staticmethod
    def _type_check_fn(_: TypeCheckContext, value: Any) -> bool:
        return isinstance(value, PandasDataFrame)

    @staticmethod
    def save_to_file(data: PandasDataFrame, path: str) -> str:
        index_label = "index"
        if data.index.name:
            index_label = data.index.name
        csv_path = f"{path}.csv"
        data.to_csv(csv_path, index_label=index_label)
        return csv_path
