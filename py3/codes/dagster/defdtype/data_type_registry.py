import importlib
import pkgutil
import inspect
from dagster import DagsterError
from loguru import logger

from .custom_data_types import OpDataType


class DataTypeRegistry:
    def __init__(self) -> None:
        self._data_types: dict[str, type[OpDataType]] = self._load_data_types(
            "codes.dagster.defdtype"
        )

    def get_data_type_class(self, key: str) -> type[OpDataType]:
        return self._data_types.get(key)

    def _find_data_type_modules(self, parent_name: str) -> list[str]:
        dtype_mod_list = []
        parent = importlib.import_module(parent_name)
        mods = pkgutil.iter_modules(parent.__path__)
        for _, mod_name, is_pkg in mods:
            if not is_pkg:
                dtype_mod_list.append(f"{parent_name}.{mod_name}")
        return dtype_mod_list

    def _load_data_types(self, package_name: str) -> dict[str, OpDataType]:
        def is_op_data_type_class(attr) -> bool:
            if inspect.isclass(attr) and attr.__base__ is OpDataType:
                return True
            else:
                return False

        dtype_mod_names = self._find_data_type_modules(package_name)
        logger.debug(f"Op data type modules: {dtype_mod_names}")

        dtypes = {}
        for mod_name in dtype_mod_names:
            try:
                mod = importlib.import_module(mod_name)
                for attr_name in mod.__dir__():
                    attr = getattr(mod, attr_name)
                    if is_op_data_type_class(attr):
                        logger.debug(attr)
                        dtypes[attr.type_key()] = attr
                        # obj = attr()
                        # dtypes[obj.key] = obj
                        # logger.debug(obj)
            except DagsterError as dag_ex:
                logger.warning(f"Invalid op data type in '{mod_name}': {dag_ex}")
            except Exception as ex:
                logger.opt(exception=True).error(ex)
        return dtypes


if __name__ == "__main__":
    registry = DataTypeRegistry()
    print(registry._data_types)

    import pandas as pd

    out_val = pd.DataFrame([["1.1"], ["2.2"], ["3.3"]], columns=["number"])
    out_type_key = "codes.dagster.defdtype.custom_data_types.PandasDataFrameType"
    dtype_class = registry.get_data_type_class(out_type_key)
    saved_path = dtype_class.save_to_file(out_val, "/tmp/pandas-out-val")
    print(saved_path)
