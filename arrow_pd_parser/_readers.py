from abc import ABC, abstractmethod
from typing import List, Union, Dict, IO
import warnings
from dataclasses import dataclass

import pandas as pd
import awswrangler as wr
from pyarrow import parquet as pq

from mojap_metadata import Metadata
from mojap_metadata.converters.arrow_converter import ArrowConverter

from arrow_pd_parser.utils import (
    FileFormat,
    is_s3_filepath,
    EngineNotImplementedError,
    validate_and_enrich_metadata,
)
from arrow_pd_parser.caster import cast_pandas_table_to_schema
from arrow_pd_parser.pa_pd import arrow_to_pandas
from arrow_pd_parser._arrow_parsers import cast_arrow_table_to_schema


@dataclass
class DataFrameFileReader(ABC):
    """
    Abstract class for reader functions used by reader API
    Should just have a read method.
    """

    ignore_columns: List = None
    drop_columns: List = None
    pd_integer: bool = True
    pd_string: bool = True
    pd_boolean: bool = True
    pd_date_type: str = "datetime_object"
    pd_timestamp_type: str = "datetime_object"
    bool_map: Dict = None

    @abstractmethod
    def read(
        self, input_path: str, metadata: Union[Metadata, dict] = None, **kwargs
    ) -> pd.DataFrame:
        """reads the file into pandas DataFrame"""

    def _cast_pandas_table_to_schema(
        self, df: pd.DataFrame, metadata: Union[Metadata, dict]
    ):
        metadata = validate_and_enrich_metadata(metadata)
        df = cast_pandas_table_to_schema(
            df=df,
            metadata=metadata,
            ignore_columns=self.ignore_columns,
            drop_columns=self.drop_columns,
            pd_integer=self.pd_integer,
            pd_string=self.pd_string,
            pd_boolean=self.pd_boolean,
            pd_date_type=self.pd_date_type,
            pd_timestamp_type=self.pd_timestamp_type,
            bool_map=self.bool_map,
        )
        return df


@dataclass
class PandasCsvReader(DataFrameFileReader):
    """reader for CSV files"""

    def read(
        self,
        input_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Reads a CSV file and returns a Pandas DataFrame
        input_path: File to read either local or S3.
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

        if is_s3_filepath(input_path):
            df = wr.s3.read_csv(input_path, **kwargs)
        else:
            df = pd.read_csv(input_path, **kwargs)

        if metadata is None and (
            self.pd_string or self.pd_integer or self.pd_boolean
        ):
            df = df.convert_dtypes(
                infer_objects=True,
                convert_string=self.pd_string,
                convert_integer=self.pd_integer,
                convert_boolean=self.pd_boolean,
                convert_floating=False,
            )
        else:
            df = self._cast_pandas_table_to_schema(df, metadata)
        return df


@dataclass
class PandasJsonReader(DataFrameFileReader):
    """reader for json files"""

    def read(
        self,
        input_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Reads a JSONL file and returns a Pandas DataFrame
        input_path: File to read either local or S3.
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

        if is_s3_filepath(input_path):
            df = wr.s3.read_json(input_path, **kwargs)
        else:
            df = pd.read_json(input_path, **kwargs)

        if metadata is None and (
            self.pd_string or self.pd_integer or self.pd_boolean
        ):
            df = df.convert_dtypes(
                infer_objects=True,
                convert_string=self.pd_string,
                convert_integer=self.pd_integer,
                convert_boolean=self.pd_boolean,
                convert_floating=False,
            )
        else:
            df = self._cast_pandas_table_to_schema(df, metadata)

        return df


@dataclass
class ArrowParquetReader(DataFrameFileReader):
    """reader for parquet files"""

    expect_full_schema: bool = True

    def read(
        self, input_path: str, metadata: Metadata = None, **kwargs
    ) -> pd.DataFrame:
        """
        Reads a Parquet file and returns a Pandas DataFrame
        input_path: File to read either local or S3.
        metadata: A metadata object or dict
        **kwargs (optional): Additional kwargs are passed to the arrow reader
            arrow.parquet.read_table
        """

        arrow_tab = pq.read_table(input_path, **kwargs)

        if metadata:
            meta = validate_and_enrich_metadata(metadata)
            schema = ArrowConverter().generate_from_meta(meta)
            arrow_tab = cast_arrow_table_to_schema(
                arrow_tab,
                schema=schema,
                expect_full_schema=self.expect_full_schema,
            )

        df = arrow_to_pandas(
            arrow_tab,
            pd_boolean=self.pd_boolean,
            pd_integer=self.pd_integer,
            pd_string=self.pd_string,
            pd_date_type=self.pd_date_type,
            pd_timestamp_type=self.pd_timestamp_type,
        )

        return df


def get_default_reader_from_file_format(
    file_format: Union[FileFormat, str],
    engine: str = None,
) -> DataFrameFileReader:
    # Convert to enum
    if isinstance(file_format, str):
        file_format = FileFormat.from_string(file_format)

    if engine is not None:
        raise EngineNotImplementedError(
            "We plan to support engine choice in the future. "
            "For now we only support one engine per file type. "
            "CSV: Pandas, JSON: Pandas, Parquet: Arrow"
        )

    # Get reader
    if file_format == FileFormat.CSV:
        reader = PandasCsvReader()
    elif file_format == FileFormat.JSON:
        reader = PandasJsonReader()
    elif file_format == FileFormat.PARQUET:
        reader = ArrowParquetReader()
    else:
        raise ValueError(f"Unsupported file_format {file_format}")

    return reader
