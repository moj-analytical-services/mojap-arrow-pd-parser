import os
import pandas as pd
import warnings

import awswrangler as wr

from abc import ABC, abstractmethod
from .pandas_caster import cast_pandas_table_to_schema
from io import TextIOWrapper
from mojap_metadata import Metadata
from typing import List, Union


class reader(ABC):
    """basic reader class"""

    @abstractmethod
    def read_file(self, file_path: str, metadata: Metadata, **kwargs) -> pd.DataFrame:
        """reads the file into pandas DataFrame"""


class csv_reader(reader):
    """reader for CSV files"""

    def read_file(self, file_path: str, metadata: Metadata, **kwargs) -> pd.DataFrame:
        return pd_read_csv(file_path, metadata, **kwargs)


class json_reader(reader):
    """reader for json files"""

    def read_file(self, file_path: str, metadata: Metadata, **kwargs) -> pd.DataFrame:
        return pd_read_json(file_path, metadata, **kwargs)


class parquet_reader(reader):
    """reader for parquet files"""

    def read_file(self, file_path: str, metadata: Metadata, **kwargs) -> pd.DataFrame:
        return pd_read_parquet(file_path, metadata, **kwargs)


def _get_reader(
    input_file: Union[str, TextIOWrapper], input_type
) -> Union[wr.s3.read_csv, pd.read_csv]:
    use_s3_reader = {
        "csv": {True: wr.s3.read_csv, False: pd.read_csv},
        "json": {True: wr.s3.read_json, False: pd.read_json},
        "parquet": {True: wr.s3.read_parquet, False: pd.read_parquet},
    }

    if isinstance(input_file, str):
        is_s3 = input_file.startswith("s3://")
    elif isinstance(input_file, TextIOWrapper):
        is_s3 = False
    else:
        raise TypeError("input file not of correct type (IO or str)")
    return use_s3_reader[input_type][is_s3]


def pd_read_csv(
    input_file: Union[TextIOWrapper, str],
    metadata: Union[Metadata, dict, None],
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
        input_file (Union[TextIOWrapper, str]): the CSV you want to read. string, path
            or file-like object.
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
    if "dtype" not in kwargs and metadata:
        kwargs["dtype"] = str

    reader = _get_reader(input_file, "csv")
    df = reader(input_file, **kwargs)
    if metadata:
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
    input_file: Union[TextIOWrapper, str],
    metadata: Union[Metadata, dict, None],
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
    """Read a json file into a Pandas dataframe casting cols based on Metadata.

    Args:
        input_file (Union[TextIOWrapper, str]): the json you want to read. string, path
            or file-like object.
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
        Pandas DataFrame: the json data as a dataframe, with the specified data types
    """

    if "lines" in kwargs:
        warnings.warn("Ignoring lines in kwargs. Setting to lines=True.")
        kwargs.pop("lines")
    if "orient" in kwargs:
        warnings.warn('Ignoring orient in kwargs. Setting to orient="records"')
        kwargs.pop("orient")

    reader = _get_reader(input_file, "json")
    df = reader(input_file, lines=True, orient="records", **kwargs)
    if metadata:
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


def pd_read_parquet(
    input_file: Union[TextIOWrapper, str],
    metadata: Union[Metadata, dict, None],
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
    """Read a parquet file into a Pandas dataframe casting cols based on Metadata.

    Args:
        input_file (Union[TextIOWrapper, str]): the parquet you want to read. string,
            path or file-like object.
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
        Pandas DataFrame: the parquet data as a dataframe, with the specified data types
    """

    reader = _get_reader(input_file, "parquet")
    df = reader(input_file, **kwargs)
    if metadata:
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


def read_factory(input_file: str) -> reader:
    readers = {"csv": csv_reader(), "jsonl": json_reader(), "parquet": parquet_reader()}
    file_format = os.path.splitext(input_file)[1][1:]
    if file_format in readers:
        return readers[file_format]
    else:
        raise TypeError(f"{file_format} not supported")


def pd_read(input_file: str, metadata: Metadata = None, **kwargs) -> pd.DataFrame:
    reader = read_factory(input_file)
    return reader.read_file(input_file, metadata, **kwargs)
