from inspect import isasyncgen, iscoroutine, isgenerator
from typing import Optional, TypeGuard, TypeVar
import pandas as pd


async def yield_data_batches(data: object):
    if iscoroutine(data):
        data = await data

    if isgenerator(data) or isasyncgen(data):
        if isasyncgen(data):
            async for d in data:
                yield d
        else:
            for d in data:
                yield d
    else:
        yield data


T = TypeVar("T")


def safe_truthy(data: Optional[T]) -> TypeGuard[T]:
    """
    Safely determines the truthiness of the provided data object.

    This method checks if the input `data` is a pandas DataFrame, a DuckDBPyRelation,
    or another type, and returns a boolean indicating whether the data should be
    considered "truthy" (i.e., non-empty and valid).

    - For pandas DataFrames, returns False if the DataFrame is empty.
    - For DuckDBPyRelation objects, does not perform a truthiness check to avoid potential hangs.
    - For other types, returns False if the data evaluates to False.
    - Returns True otherwise.

    Args:
        data: The data object to check, which can be a pandas DataFrame, DuckDBPyRelation,
              or any other type.

    Returns:
        bool: True if the data is considered truthy, False otherwise.
    """
    import duckdb

    if isinstance(data, pd.DataFrame):
        if data.empty:
            return False
    # duckdb hangs with the not self.data check, so make sure self.data isn't
    # a duckdb pyrelation before checking self.data
    elif isinstance(data, duckdb.DuckDBPyRelation):
        pass
    elif not data:
        return False

    return True
