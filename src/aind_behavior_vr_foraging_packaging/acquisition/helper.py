import json
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Union

import numpy as np
import pandas as pd


def clean_dataframe_for_nwb(data: pd.DataFrame) -> pd.DataFrame:
    """
    Clean a pandas DataFrame to ensure compatibility with NWB format.

    Parameters
    ----------
    data : pd.DataFrame
        The cleaned input DataFrame for NWB compatibility

    Returns
    -------
    pd.DataFrame
        A cleaned DataFrame that adheres to NWB data types
    """
    for column in data.columns:
        # convert to nwb allowable types
        data[column] = data[column].replace({None: np.nan})
        data[column] = data[column].apply(lambda x: x.value if isinstance(x, Enum) else x)
        data[column] = data[column].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    return data


_NestedStructureType = Union[dict, list, Any]


def convert_values_in_nested_structure(
    data: _NestedStructureType,
    check_fn: Callable[[Any], bool],
    convert_fn: Callable[[Any], Any],
) -> _NestedStructureType:
    """
    Recursively convert values in nested dictionaries/lists based on a condition.

    Parameters
    ----------
    data : _NestedStructureType
        Input data structure which may contain nested dictionaries and lists.
    check_fn : Callable
        Function that returns True if value should be converted.
    convert_fn : Callable
        Function that converts the value.

    Returns
    -------
    _NestedStructureType
        Data structure with converted values.
    """
    if isinstance(data, dict):
        return {k: convert_values_in_nested_structure(v, check_fn, convert_fn) for k, v in data.items()}
    if isinstance(data, list):
        return [convert_values_in_nested_structure(item, check_fn, convert_fn) for item in data]
    return convert_fn(data) if check_fn(data) else data


def convert_datetimes_to_iso_string(
    data: _NestedStructureType,
) -> _NestedStructureType:
    """
    Convert datetime objects in a nested structure to ISO format strings.

    Parameters
    ----------
    data : _NestedStructureType
        Input data structure which may contain nested dictionaries and lists.

    Returns
    -------
    _NestedStructureType
        Data structure with datetime objects converted to ISO format strings.
    """
    return convert_values_in_nested_structure(
        data,
        check_fn=lambda x: isinstance(x, datetime),
        convert_fn=lambda x: x.isoformat(),
    )
