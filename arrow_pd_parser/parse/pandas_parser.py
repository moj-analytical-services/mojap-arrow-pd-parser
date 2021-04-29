from typing import List, Union, IO, Callable
from copy import deepcopy
import warnings

from datetime import datetime

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

from mojap_metadata import Metadata

_allowed_type_categories = [
    "integer",
    "boolean",
    "string",
    "float",
    "timestamp",
    "struct",
    "list",
]


class PandasCastError(Exception):
    pass


def _convert_str_to_ns_timestamp_series(
    s: pd.Series, str_datetime_format=None,
) -> pd.Series:
    s = pd.to_datetime(s, format=str_datetime_format,)

    return s


def _convert_str_to_datetime_obj_series(
    s: pd.Series, is_date, str_datetime_format=None,
) -> pd.Series:

    if str_datetime_format is None:
        str_datetime_format = "%Y-%m-%d" if is_date else "%Y-%m-%d %H:%M:%S"

    # Need to create a new series to force a Series with an object dtype
    s_new = pd.Series([None] * len(s), dtype=object)
    for index, value in s.items():
        check_value = "" if pd.isna(value) else value
        if not check_value:
            continue  # Leave as None
        else:
            value = datetime.strptime(value, str_datetime_format)
            if is_date:
                value = value.date()
            s_new.loc[index] = value

    return s_new


def _default_str_bool_mapper(s: str):

    basic_map = {
        "1": True,
        "0": False,
        "t": True,
        "f": False,
    }

    if pd.isna(s) or not s:
        return np.nan
    else:
        return basic_map.get(s.lower()[0], np.nan)


def _infer_bool_type(s: pd.Series):

    t = str(s.dtype)
    if t in ["bool", "boolean"]:
        pass
    elif is_numeric_dtype(s):
        t = "numeric"
    else:
        # determine t
        if np.all(s.apply(lambda x: isinstance(x, bool) or pd.isna(x))):
            t = "bool_object"
        else:
            t = "str_object"

    return t


# Define functions that convert str series to their specific type
def convert_to_integer_series(
    s: pd.Series, pd_integer: bool, num_errors: str
) -> pd.Series:
    """
    Reads a pandas Series (str/string dtype) and casts to a integer
    """
    s = pd.to_numeric(s, errors=num_errors)
    if pd_integer:
        s = s.astype(pd.Int64Dtype())
    return s


def convert_to_float_series(s: pd.Series, num_errors: str) -> pd.Series:
    """
    Reads a pandas Series (str/string / numeric dtype) and casts to a float
    """
    s = pd.to_numeric(s, errors=num_errors)
    # in case pandas converts to int rather than float
    s = s.astype(np.float64)
    return s


def convert_to_bool_series(s: pd.Series, pd_boolean, bool_map=None,) -> pd.Series:
    """
    Reads a pandas Series and casts to a bool. If type is already boolean like
    i.e. an object of bools and nulls, bool dtype or boolean dtype then conversion
    occurs. Otherwise function expects a str series and will apply mapping to series
    before casting to Pandas Boolean type. If bool_map if provided this is used for
    mapping otherwise the _default_str_bool_mapper is used

    Returns:
      pd.Series: Column casted to boolean type
    """
    if not pd_boolean:
        raise NotImplementedError("Casting to old bool type is not yet implemented")

    t = _infer_bool_type(s)

    if t == "numeric":
        s = s.astype(str)

    if t in ["str_object", "numeric"]:
        if bool_map is None:
            s = s.map(_default_str_bool_mapper)
        else:
            s = s.map(bool_map)

    s = s.convert_dtypes(
        infer_objects=False,
        convert_integer=False,
        convert_boolean=True,
        convert_string=False,
        convert_floating=False,
    )
    return s


def convert_to_string_series(s: pd.Series, pd_string: bool) -> pd.Series:
    """
    Reads a pandas Series (str/string dtype) and casts to a (str/string dtype).
    """
    if pd_string:
        s = s.astype(pd.StringDtype())
    else:
        s = s.astype(str)
    return s


def convert_str_to_timestamp_series(
    s: pd.Series, is_date, pd_type, str_datetime_format=None
) -> pd.Series:
    if pd_type == "pd_timestamp":
        s = _convert_str_to_ns_timestamp_series(s, str_datetime_format)
    elif pd_type == "datetime_object":
        s = _convert_str_to_datetime_obj_series(s, is_date, str_datetime_format)
    elif pd_type == "pd_period":
        raise NotImplementedError(
            "Conversion to period is not available yet for this caster"
        )
    else:
        raise ValueError(
            'Incorrect pd_type, expecting "datetime_object"',
            '"pd_timestamp" or "pd_period".' f" Got {pd_type}.",
        )

    return s


def cast_pandas_column_to_schema(
    s: pd.Series,
    metacol: dict,
    pd_integer=True,
    pd_string=True,
    pd_boolean=True,
    pd_date_type: str = "datetime_object",
    pd_timestamp_type: str = "datetime_object",
    num_errors="raise",
    bool_map=None,
) -> pd.Series:

    # Conversions
    try:
        if metacol["type_category"] == "integer":
            s = convert_to_integer_series(s, pd_integer, num_errors)

        elif metacol["type_category"] == "float":
            s = convert_to_float_series(s, num_errors)

        elif metacol["type_category"] == "boolean":
            s = convert_to_bool_series(s, pd_boolean, bool_map)

        elif metacol["type_category"] == "string":
            s = convert_to_string_series(s, pd_string)

        elif metacol["type_category"] == "timestamp":
            is_date = metacol["type"].startswith("date")
            s = convert_str_to_timestamp_series(
                s,
                pd_type=pd_date_type if is_date else pd_timestamp_type,
                is_date=is_date,
                str_datetime_format=metacol.get("datetime_format"),
            )
        else:
            raise ValueError(
                f"meta type_category must be one of {_allowed_type_categories}."
                f"Got {metacol['type_category']} from column {metacol['name']}"
            )

    except Exception as e:
        starter_msg = (
            f"Failed conversion - name: {metacol['name']} | "
            f"type_category: {metacol['type_category']} | "
            f"type: {metacol.get('type')} - see traceback."
        )
        raise PandasCastError(starter_msg).with_traceback(e.__traceback__)

    return s


def cast_pandas_table_to_schema(
    df: pd.DataFrame,
    metadata: Union[Metadata, dict],
    ignore_columns: List = None,
    drop_columns: List = None,
    pd_integer: bool = True,
    pd_string: bool = True,
    pd_boolean: bool = True,
    pd_date_type: str = "datetime_object",
    pd_timestamp_type: str = "datetime_object",
    bool_map: Union[Callable, dict] = None,
):
    """
    Casts the columns in dataframe provided to the meta data dictionary provided.
    Safest casting occurs when all coltypes of input dataframe are strings.

    df: Pandas dataframe
    meta: Metadata or dict representation of metadata
    ignore_columns: a list of column names to not cast to the meta data dictionary.
        These columns are remained unchanged.
    drop_columns: Removes these columns from the dataframe
    bool_map (Callable, dict, optional): A custom mapping function that is applied
        to str cols to be converted to booleans before conversion to boolean type.
        e.g. {"Yes": True, "No": False}. If not set bool values are inferred by the
        _default_str_bool_mapper.
    """

    num_errors = "raise"

    if ignore_columns is None:
        ignore_columns = []

    if drop_columns is None:
        drop_columns = []

    if isinstance(metadata, Metadata):
        meta = metadata.to_dict()
    elif isinstance(metadata, dict):
        if "columns" not in metadata:
            raise KeyError('metadata missing a "columns" key')

        _ = Metadata.from_dict(metadata)  # Check metadata is valid
        meta = deepcopy(metadata)
    else:
        error_msg = (
            "Input metadata must be of type Metadata " f"or dict got {type(metadata)}"
        )
        raise ValueError(error_msg)
    df = df.copy()

    all_exclude_cols = ignore_columns + drop_columns
    meta_cols_to_convert = [
        c for c in meta["columns"] if c["name"] not in all_exclude_cols
    ]

    for c in meta_cols_to_convert:
        # Null first if applicable
        if c["name"] not in df.columns:
            raise ValueError(f"Column '{c['name']}' not in df")

        else:
            df[c["name"]] = cast_pandas_column_to_schema(
                df[c["name"]],
                metacol=c,
                pd_integer=pd_integer,
                pd_string=pd_string,
                pd_boolean=pd_boolean,
                pd_date_type=pd_date_type,
                pd_timestamp_type=pd_timestamp_type,
                num_errors=num_errors,
                bool_map=bool_map,
            )

    final_cols = [c["name"] for c in meta["columns"] if c["name"] not in drop_columns]
    df = df[final_cols]

    return df


def pd_read_csv(
    input_file: Union[IO, str],
    metadata: Union[Metadata, dict],
    ignore_columns: List = None,
    drop_columns: List = None,
    pd_integer: bool = True,
    pd_string: bool = True,
    pd_boolean: bool = True,
    pd_date_type: str = "datetime_object",
    pd_timestamp_type: str = "datetime_object",
    bool_map=None,
    **kwargs,
) -> pd.DataFrame:
    """Read a csv file into a Pandas dataframe casting cols based on Metadata.

    Args:
        input_file (Union[IO, str]): the CSV you want to read. string, path or
            file-like object.
        metadata Union[Metadata, dict]: what you want the column to be cast to.
        ignore_columns: (List, optional): a list of column names to not cast to
            the meta data dictionary. These columns are remained unchanged.
        drop_columns:  (List, optional): a list of column names you want to drop
            from the dataframe.
        pd_boolean: whether to use the new pandas boolean format. Defaults to True.
            When set to False, uses a custom boolean format to coerce object type.
        pd_integer: if True, converts integers to Pandas int64 format.
            If False, uses float64. Defaults to True.
        pd_string: Defaults to True.
        pd_date_type (str, optional): specifies the timestamp type. Can be one of:
            "datetime_object", "pd_timestamp" or "pd_period" ("pd_period" not yet
            implemented).
        pd_timestamp_type (str, optional): specifies the timestamp type. Can be one of:
            "datetime_object", "pd_timestamp" or "pd_period" ("pd_period" not yet
            implemented).
        bool_map (Callable, dict, optional): A custom mapping function that is applied
            to str cols to be converted to booleans before conversion to boolean type.
            e.g. {"Yes": True, "No": False}. If not set bool values are inferred by the
            _default_str_bool_mapper.
        **kwargs (optional): Additional kwargs are passed to pandas.read_csv

    Returns:
        Pandas DataFrame: the csv data as a dataframe, with the specified data types
    """
    if "low_memory" not in kwargs:
        kwargs["low_memory"] = False
    if "dtype" not in kwargs:
        kwargs["dtype"] = str

    df = pd.read_csv(input_file, **kwargs)
    df = cast_pandas_table_to_schema(
        df=df,
        metadata=metadata,
        ignore_columns=ignore_columns,
        drop_columns=drop_columns,
        pd_integer=pd_integer,
        pd_string=pd_string,
        pd_boolean=pd_boolean,
        pd_date_type=pd_date_type,
        pd_timestamp_type=pd_timestamp_type,
        bool_map=bool_map,
    )

    return df


def pd_read_json(
    input_file: Union[IO, str],
    metadata: Union[Metadata, dict],
    ignore_columns: List = None,
    drop_columns: List = None,
    pd_integer: bool = True,
    pd_string: bool = True,
    pd_boolean: bool = True,
    pd_date_type: str = "datetime_object",
    pd_timestamp_type: str = "datetime_object",
    bool_map=None,
    **kwargs,
) -> pd.DataFrame:
    """Read a csv file into a Pandas dataframe casting cols based on Metadata.

    Args:
        input_file (Union[IO, str]): the CSV you want to read. string, path or
            file-like object.
        metadata Union[Metadata, dict]: what you want the column to be cast to.
        ignore_columns: (List, optional): a list of column names to not cast to
            the meta data dictionary. These columns are remained unchanged.
        drop_columns:  (List, optional): a list of column names you want to drop
            from the dataframe.
        pd_boolean: whether to use the new pandas boolean format. Defaults to True.
            When set to False, uses a custom boolean format to coerce object type.
        pd_integer: if True, converts integers to Pandas int64 format.
            If False, uses float64. Defaults to True.
        pd_string: Defaults to True.
        pd_date_type (str, optional): specifies the timestamp type. Can be one of:
            "datetime_object", "pd_timestamp" or "pd_period" ("pd_period" not yet
            implemented).
        pd_timestamp_type (str, optional): specifies the timestamp type. Can be one of:
            "datetime_object", "pd_timestamp" or "pd_period" ("pd_period" not yet
            implemented).
        bool_map (Callable, dict, optional): A custom mapping function that is applied
            to str cols to be converted to booleans before conversion to boolean type.
            e.g. {"Yes": True, "No": False}. If not set bool values are inferred by the
            _default_str_bool_mapper.
        **kwargs (optional): Additional kwargs are passed to pandas.read_csv. Params
            orient and lines will be ignored as always set to lines=True and
            orient="records".

    Returns:
        Pandas DataFrame: the csv data as a dataframe, with the specified data types
    """

    if "lines" in kwargs:
        warnings.warn("Ignoring lines in kwargs. Setting to lines=True.")
        kwargs.pop("lines")
    if "orient" in kwargs:
        warnings.warn('Ignoring orient in kwargs. Setting to orient="records"')
        kwargs.pop("orient")

    df = pd.read_json(input_file, lines=True, orient="records", **kwargs)
    df = cast_pandas_table_to_schema(
        df=df,
        metadata=metadata,
        ignore_columns=ignore_columns,
        drop_columns=drop_columns,
        pd_integer=pd_integer,
        pd_string=pd_string,
        pd_boolean=pd_boolean,
        pd_date_type=pd_date_type,
        pd_timestamp_type=pd_timestamp_type,
        bool_map=bool_map,
    )

    return df
