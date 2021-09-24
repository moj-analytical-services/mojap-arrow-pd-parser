from abc import ABC, abstractmethod
from typing import List, Union, Dict, IO
import warnings
from dataclasses import dataclass
import datetime


import pandas as pd
import numpy as np

import awswrangler as wr

import pyarrow as pa
from pyarrow import parquet as pq

from mojap_metadata import Metadata
from mojap_metadata.converters.arrow_converter import ArrowConverter

from arrow_pd_parser.utils import (
    FileFormat,
    is_s3_filepath,
    EngineNotImplementedError,
    validate_and_enrich_metadata,
)


@dataclass
class DataFrameFileWriter(ABC):
    """
    Abstract class for writer functions used by writer API
    Should just have a write method.
    """

    copy = False
    ignore_columns: List = None
    drop_columns: List = None
    pd_integer: bool = True
    pd_string: bool = True
    pd_boolean: bool = True
    pd_date_type: str = "datetime_object"
    pd_timestamp_type: str = "datetime_object"
    bool_map: Dict = None

    @abstractmethod
    def write(
        self,
        df: pd.DataFrame,
        output_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """writes the Dataframe to the output file"""


@dataclass
class PandasCsvWriter(DataFrameFileWriter):
    """write for CSV files"""

    drop_index = True

    def write(
        self,
        df: pd.DataFrame,
        output_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ):
        """
        Writes a pandas DataFrame to CSV
        output_path: File to read either local or S3.
        metadata: A metadata object or dict to cast the dataframe to before writing
          (not necessarily needed for writing especially for CSV)
        **kwargs (optional): Additional kwargs are passed to pandas or awswrangler
            to_csv method.
        """
        if self.copy:
            df_out = df.copy()
        else:
            df_out = df

        if kwargs is None:
            kwargs = {}

        for col in df_out.columns:
            # Convert period columns to strings so they're exported in a way
            # Arrow can read
            if pd.api.types.is_period_dtype(df_out[col]):
                df_out[col] = df_out[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        kwargs_index = kwargs.get("index", (not self.drop_index))
        if kwargs_index != (not self.drop_index):
            warning_msg = (
                f"Your kwargs for index ({kwargs_index}) mismatches the writer's "
                f"settings self.drop_index ({self.drop_index}). "
                "In this instance kwargs superseeds the writer settings."
            )
            warnings.warn(warning_msg)
        else:
            kwargs["index"] = not self.drop_index

        if is_s3_filepath(output_path):
            wr.s3.to_csv(df_out, output_path, **kwargs)
        else:
            df_out.to_csv(output_path, **kwargs)


@dataclass
class PandasJsonWriter(DataFrameFileWriter):
    """write for CSV files"""

    def write(
        self,
        df: pd.DataFrame,
        output_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ):
        """
        Writes a pandas DataFrame to CSV
        output_path: File to read either local or S3.
        metadata: A metadata object or dict to cast the dataframe to before writing
          (not necessarily needed for writing especially for CSV)
        **kwargs (optional): Additional kwargs are passed to pandas or awswrangler
            to_csv method.
        """

        if self.copy:
            df_out = df.copy()
        else:
            df_out = df

        if kwargs is None:
            kwargs = {}

        # Convert date-related columns to strings Arrow can read consistently
        for col in df_out.columns:
            if pd.api.types.is_period_dtype(df_out[col]):
                df_out[col] = df_out[col].dt.strftime("%Y-%m-%d %H:%M:%S")
            elif any(
                [
                    pd.api.types.is_datetime64_any_dtype(df_out[col]),
                    isinstance(
                        df_out[col][df_out[col].notnull()].iloc[0],
                        (datetime.datetime, datetime.date),
                    ),
                ]
            ):
                df_out[col] = df_out[col].astype(pd.StringDtype())
                # Convert pd_timestamp string 'NaT' to NaN so PyArrow can read them
                df_out[col].replace("NaT", np.nan, regex=False, inplace=True)

        for col in df_out.columns:
            # Convert period columns to strings so they're exported in a way
            # Arrow can read
            if pd.api.types.is_period_dtype(df_out[col]):
                df_out[col] = df_out[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        if kwargs.get("orient", "records") != "records":
            error_msg = (
                "You are not allowed to specify orient in your kwargs "
                "as anything other than `orient='records'`. This is a "
                "jsonl writer and requires this setting."
                ""
            )
            raise ValueError(error_msg)

        if not kwargs.get("lines", True):
            error_msg = (
                "You are not allowed to specify lines in your kwargs "
                "as anything other than `lines='records'`. This is a "
                "jsonl writer and requires this setting."
            )
            raise ValueError(error_msg)

        if is_s3_filepath(output_path):
            wr.s3.to_json(df_out, output_path, orient="records", lines=True, **kwargs)
        else:
            df_out.to_json(output_path, orient="records", lines=True, **kwargs)


@dataclass
class ArrowParquetWriter(DataFrameFileWriter):
    """write for Parquet files"""

    version: str = "2.0"
    compression: str = "snappy"

    def write(
        self,
        df: pd.DataFrame,
        output_path: str,
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ):
        """
        Writes a pandas DataFrame to CSV
        output_path: File to read either local or S3.
        metadata: A metadata object or dict to cast the dataframe to before writing
          (not necessarily needed for writing especially for CSV)
        **kwargs (optional): Additional kwargs are passed to
          pyarrow.parquet.write_table
        """
        if kwargs is None:
            kwargs = {}

        if metadata:
            meta = validate_and_enrich_metadata(metadata)
            arrow_schema = ArrowConverter().generate_from_meta(meta)

        else:
            arrow_schema = None

        kwargs["version"] = kwargs.get("version", self.version)
        kwargs["compression"] = kwargs.get("compression", self.compression)

        if kwargs["version"] != self.version:
            warning_msg = (
                f"Your kwargs for version ({kwargs.get('version')}) mismatches "
                f"the writer's settings self.version ({self.version})."
                "In this instance kwargs superseeds the writer settings."
            )
            warnings.warn(warning_msg)

        if kwargs["compression"] != self.compression:
            warning_msg = (
                f"Your kwargs for compression ({kwargs.get('compression')}) mismatches "
                f"the writer's settings self.compression ({self.compression}). "
                "In this instance kwargs superseeds the writer settings."
            )
            warnings.warn(warning_msg)

        table = pa.Table.from_pandas(df, schema=arrow_schema)
        pq.write_table(table, output_path, **kwargs)


def get_default_writer_from_file_format(
    file_format: Union[FileFormat, str],
    engine: str = None,
) -> DataFrameFileWriter:
    # Convert to enum
    if isinstance(file_format, str):
        file_format = FileFormat.from_string(file_format)

    if engine is not None:
        raise EngineNotImplementedError(
            "We plan to support engine choice in the future. "
            "For now we only support one engine per file type. "
            "CSV: Pandas, JSON: Pandas, Parquet: Arrow"
        )

    # Get writer
    if file_format == FileFormat.CSV:
        writer = PandasCsvWriter()
    elif file_format == FileFormat.JSON:
        writer = PandasJsonWriter()
    elif file_format == FileFormat.PARQUET:
        writer = ArrowParquetWriter()
    else:
        raise ValueError(f"Unsupported file_format {file_format}")

    return writer
