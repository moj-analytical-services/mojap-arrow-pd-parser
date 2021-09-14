import pandas as pd
import warnings

import awswrangler as wr

from abc import ABC, abstractmethod
from mojap_metadata import Metadata
from typing import List, Union
from utils import FileFormat, match_file_format_to_str

from arrow_pd_parser.caster import cast_pandas_table_to_schema


class DataFrameFileReader(ABC):
    """
    Abstract class for reader functions used by reader API
    Should just have a read method.
    """

    @abstractmethod
    def read(
        self, input_file: str, metadata: Union[Metadata, dict] = None, **kwargs
    ) -> pd.DataFrame:
        """reads the file into pandas DataFrame"""

    def is_s3_filepath(self, input_file) -> bool:
        return input_file.startswith("s3://")


class CsvReader(DataFrameFileReader):
    """reader for CSV files"""

    def read(
        self, input_file: str, metadata: Metadata = None, **kwargs
    ) -> pd.DataFrame:
        """
        Reads a CSV file and returns a Pandas DataFrame
        input_file: File to read either local or S3.
        metadata: A metadata object or dict
        **kwargs (optional): Additional kwargs are passed to pandas or awswrangler
            read_csv. Note if metadata is not None then kwargs: low_memory=False
            and dtype=str are set in order to properly cast CSV to metadata schema.
        """
        if metadata:
            # If metadata is provided force
            # str read in ready for type conversion
            if "low_memory" not in kwargs:
                kwargs["low_memory"] = False
            if "dtype" not in kwargs:
                kwargs["dtype"] = str

        if self.is_s3(input_file):
            return wr.s3.read_csv(input_file, **kwargs)
        else:
            return pd.read_csv(input_file, **kwargs)


class JsonReader(DataFrameFileReader):
    """reader for json files"""

    def read(
        self, input_file: str, metadata: Metadata = None, **kwargs
    ) -> pd.DataFrame:
        """
        Reads a JSONL file and returns a Pandas DataFrame
        input_file: File to read either local or S3.
        metadata: A metadata object or dict
        **kwargs (optional): Additional kwargs are passed to pandas or awswrangler
            read_json. Note orient and lines will be ignored as always set to
            lines=True and orient="records".
        """

        if not kwargs.get("lines", True):
            warnings.warn("Ignoring lines in kwargs. Setting to lines=True.")
        kwargs["lines"] = True

        if kwargs.get("orient", "records") != "records":
            warnings.warn('Ignoring orient in kwargs. Setting to orient="records"')
        kwargs["orient"] = "records"

        if self.is_s3(input_file):
            return wr.s3.read_json(input_file, **kwargs)
        else:
            return pd.read_json(input_file, **kwargs)


class ParquetReader(DataFrameFileReader):
    """reader for parquet files"""

    def read(self, input_file: str, **kwargs) -> pd.DataFrame:
        """
        Reads a Parquet file and returns a Pandas DataFrame
        input_file: File to read either local or S3.
        metadata: A metadata object or dict
        **kwargs (optional): Additional kwargs are passed to pandas or awswrangler
            read_parquet.
        """
        if self.is_s3(input_file):
            return wr.s3.read_parquet(input_file, **kwargs)
        else:
            return pd.read_parquet(input_file, **kwargs)


class ReaderAPI:
    """basic reader class to manage reading and writing to S3"""

    def __init__(self, reader: DataFrameFileReader) -> None:
        self.reader = reader

    def read(
        self,
        input_file: str,
        metadata: Union[Metadata, dict] = None,
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
        """
        Read a file into a Pandas dataframe.
        Casting cols based on Metadata if provided.

        Args:
            input_file (Union[IOBase, str]): the CSV you want to read. string, path
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
            Pandas DataFrame: the data from the file as a dataframe,
            with the specified data types
        """

        df = self.reader.read(input_file, **kwargs)
        if metadata is not None:
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


def get_reader_from_file_format(
    file_format: Union[FileFormat, str]
) -> DataFrameFileReader:
    # Convert to enum
    if isinstance(file_format, str):
        file_format = match_file_format_to_str(file_format, True)

    # Get reader
    if file_format == FileFormat.CSV:
        reader = CsvReader()
    elif file_format == FileFormat.JSON:
        reader = JsonReader()
    elif file_format == FileFormat.PARQUET:
        reader = ParquetReader()
    else:
        raise ValueError(f"Unsupported file_format {file_format}")

    return reader
