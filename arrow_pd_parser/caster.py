import warnings
from copy import deepcopy
from typing import Callable, List, Union

import numpy as np
import pandas as pd
from mojap_metadata import Metadata
from pandas.api.types import is_numeric_dtype

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
    s: pd.Series,
    ts_errors: str,
    str_datetime_format=None,
) -> pd.Series:
    s = pd.to_datetime(
        s,
        format=str_datetime_format,
        errors=ts_errors,
    )

    return s


def _convert_str_to_datetime_obj_series(
    s: pd.Series,
    is_date,
    ts_errors: str,
    str_datetime_format=None,
) -> pd.Series:
    if str_datetime_format is None:
        str_datetime_format = "%Y-%m-%d" if is_date else "%Y-%m-%d %H:%M:%S"

    # As a new Series is created keep the original index
    original_index = s.index

    # Use to_datetime to convert dates to pandas datetimes,
    # convert to an array of datetime.datetimes, and put those back in a Series
    s_new = pd.Series(
        pd.to_datetime(
            s, format=str_datetime_format, errors=ts_errors
        ).dt.to_pydatetime(),
        dtype=object,
    )

    s_new[s_new.isna() | (s_new == "")] = None

    if is_date:
        s_new = s_new.apply(lambda d: d.date() if d else None)

    # Restore the original index
    s_new.index = original_index

    return s_new


def _default_str_bool_mapper(
    s: Union[str, int, float], bool_errors="coerce"
) -> Union[bool, float]:
    """
    Maps a given input to a boolean value based on predefined strings.

    Parameters:
    -----------
    s : Union[str, int, float]
        The input value to be mapped. Can be a string, integer, or float.
        Common values include 'yes', 'no', 'true', 'false', '1', '0'.

    bool_errors : str, optional, default="coerce"
        Specifies the behavior when the input value cannot be mapped to a boolean.
        - "coerce": returns np.nan for unmappable values.
        - "raise": raises a ValueError for unmappable values.

    Returns:
    --------
    Union[bool, float]
        The mapped boolean value if the input is recognized, or np.nan if the input is unrecognized and
        bool_errors is set to "coerce". If bool_errors is set to "raise", a ValueError is raised for unrecognized inputs.

    Raises:
    -------
    ValueError
        If bool_errors is "raise" and the input value cannot be mapped to a boolean.

    Notes:
    ------
    - The function is case-insensitive and ignores leading and trailing whitespace in the input string.
    - Empty strings and None are mapped to np.nan.
    - If the input is NaN (as determined by pandas.isna()), function returns np.nan.

    Examples:
    ---------
    >>> _default_str_bool_mapper("yes")
    True
    >>> _default_str_bool_mapper("no")
    False
    >>> _default_str_bool_mapper("True")
    True
    >>> _default_str_bool_mapper("0")
    False
    >>> _default_str_bool_mapper("")
    nan
    >>> _default_str_bool_mapper("maybe", bool_errors="coerce")
    nan
    >>> _default_str_bool_mapper("maybe", bool_errors="raise")
    Traceback (most recent call last):
        ...
    ValueError: maybe
    """

    basic_map = {
        "yes": True,
        "no": False,
        "true": True,
        "t": True,
        "false": False,
        "f": False,
        "1": True,
        "1.0": True,
        "0.0": False,
        "0": False,
        "": np.nan,
        None: np.nan,
    }

    # In case bools are interpreted as numerics
    s_str = str(s).strip().lower()

    # Check for "truthiness" of string e.g. not 0 == True but not "0" == False
    if pd.isna(s) or not s_str:
        return np.nan

    if bool_errors == "coerce":
        result = basic_map.get(s_str, np.nan)
        return result

    elif bool_errors == "raise":
        try:
            result = basic_map[s_str]
        except KeyError:
            raise ValueError(f"{s_str}")
        return result


def check_bool_mapping_errors(
    series: pd.Series, func=_default_str_bool_mapper, bool_errors="coerce"
) -> pd.Series:
    """
    Applies a boolean mapping function to a pandas Series and checks for errors.

    Parameters:
    -----------
    series : pd.Series
        The input pandas Series containing values to be mapped to boolean.

    func : callable, optional, default=_default_str_bool_mapper
        The function to apply to each element of the series. This function should take two arguments:
        the value to be mapped and the bool_errors parameter.

    bool_errors : str, optional, default="coerce"
        Specifies the behavior when an input value cannot be mapped to a boolean by the given function.
        - "coerce": returns np.nan for values that cannot be cast to boolean.
        - "raise": raises a ValueError for values that cannot be cast to boolean.

    Returns:
    --------
    pd.Series
        A pandas Series with the same index as the input, containing the mapped boolean values or np.nan for unmappable values.

    Raises:
    -------
    ValueError
        If bool_errors is "raise" and there are values in the series that cannot be cast to boolean,
        a ValueError is raised after processing all values, detailing the number of errors and the problematic values.

    Examples:
    ---------
    >>> series = pd.Series(["yes", "no", "true", "false", "maybe"])
    >>> check_bool_mapping_errors(series)
    0     True
    1    False
    2     True
    3    False
    4      NaN
    dtype: object

    >>> check_bool_mapping_errors(series, bool_errors="raise")
    Traceback (most recent call last):
        ...
    ValueError: 1 values could not be cast to boolean values. Details: ['maybe']

    Notes:
    ------
    - The function iterates over each element of the input series and applies the given boolean mapping function.
    - If bool_errors is set to "coerce", values that cannot be mapped to boolean are replaced with np.nan.
    - If bool_errors is set to "raise", a ValueError is raised after processing all values if there are any unmappable values.
    """

    casting_errors = []

    def wrapper(v):
        try:
            return func(v, bool_errors=bool_errors)
        except ValueError as e:
            casting_errors.append(str(e))
            return np.nan

    result = series.map(wrapper)

    if casting_errors:
        raise ValueError(
            f"{len(casting_errors)} values could not be cast to boolean values. Details: {casting_errors}"
        )

    return result


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


def convert_to_bool_series(
    s: pd.Series, pd_boolean, bool_map=None, bool_errors="coerce"
) -> pd.Series:
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
            try:
                s = check_bool_mapping_errors(
                    s, _default_str_bool_mapper, bool_errors=bool_errors
                )
            except ValueError as e:
                print(e)
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
    s: pd.Series, is_date, pd_type, ts_errors: str, str_datetime_format=None
) -> pd.Series:
    if pd_type == "pd_timestamp":
        s = _convert_str_to_ns_timestamp_series(s, ts_errors, str_datetime_format)
    elif pd_type == "datetime_object":
        s = _convert_str_to_datetime_obj_series(
            s, is_date, ts_errors, str_datetime_format
        )
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
    ts_errors="raise",
    bool_errors="coerce",
    bool_map=None,
) -> pd.Series:
    complex_type_categories = ["struct", "list"]

    # get type_category if not exist
    if "type_category" not in metacol:
        tmp_meta = Metadata(columns=[metacol])
        tmp_meta.set_col_type_category_from_types()
        metacol = tmp_meta.get_column(metacol["name"])

    # Conversions
    try:
        if metacol["type_category"] == "integer":
            s = convert_to_integer_series(s, pd_integer, num_errors)

        elif metacol["type_category"] == "float":
            s = convert_to_float_series(s, num_errors)

        elif metacol["type_category"] == "boolean":
            s = convert_to_bool_series(s, pd_boolean, bool_map, bool_errors)

        elif metacol["type_category"] == "string":
            s = convert_to_string_series(s, pd_string)

        elif metacol["type_category"] == "timestamp":
            is_date = metacol["type"].startswith("date")
            s = convert_str_to_timestamp_series(
                s,
                pd_type=pd_date_type if is_date else pd_timestamp_type,
                is_date=is_date,
                ts_errors=ts_errors,
                str_datetime_format=metacol.get("datetime_format"),
            )
        elif metacol["type_category"] in complex_type_categories:
            warnings.warn(
                f"complex types ({complex_type_categories}) are not cast "
                f"(column: {metacol['name']})"
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
    num_error_map: dict = None,
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

    default_num_errors = "raise"

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
        c
        for c in meta["columns"]
        if c["name"] not in all_exclude_cols or c["name"] not in meta["partitions"]
    ]

    for c in meta_cols_to_convert:
        # Null first if applicable
        if c["name"] not in df.columns:
            raise ValueError(f"Column '{c['name']}' not in df")

        else:
            # must get num_errors from either meta or num_error_map. Meta has precedence
            if c.get("num_errors"):
                num_errors = c.get("num_errors")
            elif isinstance(num_error_map, dict):
                num_errors = num_error_map.get(c["name"])
            else:
                num_errors = default_num_errors

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

    final_cols = [
        c["name"]
        for c in meta["columns"]
        if c["name"] not in drop_columns or c["name"] not in meta["partitions"]
    ]
    df = df[final_cols]

    return df
