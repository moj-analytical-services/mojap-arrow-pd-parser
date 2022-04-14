from abc import ABC, abstractmethod
from typing import List, Union, Dict, IO, Iterable
import warnings
from dataclasses import dataclass
import datetime
import os
import io

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

    copy = True
    ignore_columns: List = None
    drop_columns: List = None
    pd_integer: bool = True
    pd_string: bool = True
    pd_boolean: bool = True
    pd_date_type: str = "datetime_object"
    pd_timestamp_type: str = "datetime_object"
    bool_map: Dict = None

    def write(
        self,
        df: Union[pd.DataFrame, Iterable[pd.DataFrame]],
        output_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ) -> None:
        """writes a DataFrame or iterator of DataFrames to the output file
        output_path: File to write either local or S3.
        metadata: A metadata object or dict to cast the dataframe to before writing
          (not necessarily needed for writing especially for CSV)
        **kwargs (optional): Additional kwargs are passed to write method."""

        if "mode" in kwargs:
            mode = kwargs["mode"]
            del kwargs["mode"]
        else:
            mode = "overwrite"

        if isinstance(df, pd.DataFrame):
            # When writing a single dataframe as an overwrite, ensure that
            # a single file is produced when writing to S3, matching the
            # behaviour when writing to a file (Note: needed for s3_data_packer)
            single_file = mode == "overwrite"
            self._write(
                df, output_path, metadata, mode=mode, single_file=single_file, **kwargs
            )
        else:
            # Write first chunk from iterable using selected mode
            self._write(next(df), output_path, metadata, mode=mode, **kwargs)
            # then append the rest
            for chunk in df:
                self._write(chunk, output_path, metadata, mode="append", **kwargs)

    @abstractmethod
    def _write(
        df: pd.DataFrame,
        output_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        mode: str = "overwrite",
        single_file: bool = False,  # write one file rather than dataset directory
        **kwargs,
    ) -> None:
        """abstract method to write a Dataframe to the output file using
        the specific output"""


@dataclass
class PandasCsvWriter(DataFrameFileWriter):
    """write for CSV files"""

    drop_index = True

    def _write(
        self,
        df: pd.DataFrame,
        output_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        mode: str = "overwrite",
        single_file: bool = False,
        **kwargs,
    ):
        """
        Writes a pandas DataFrame to CSV
        output_path: File to write either local or S3.
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
                "In this instance kwargs supersedes the writer settings."
            )
            warnings.warn(warning_msg)
        else:
            kwargs["index"] = not self.drop_index

        if is_s3_filepath(output_path):
            if single_file:
                wr.s3.to_csv(df_out, output_path, **kwargs)
            else:
                wr.s3.to_csv(df_out, output_path, dataset=True, mode=mode, **kwargs)
        else:
            dirs = os.path.dirname(output_path)
            if dirs:
                os.makedirs(dirs, exist_ok=True)
            write_mode = "a" if mode == "append" else "w"
            # Don't write header row again when appending
            write_headers = write_mode != "a"
            df_out.to_csv(output_path, header=write_headers, mode=write_mode, **kwargs)


@dataclass
class PandasJsonWriter(DataFrameFileWriter):
    """write for JSON files"""

    def _write(
        self,
        df: pd.DataFrame,
        output_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        mode: str = "overwrite",
        single_file: bool = False,
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
                    len(df_out[col][df_out[col].notnull()]) > 0
                    and isinstance(
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
                "as anything other than `lines=True`. This is a "
                "jsonl writer and requires this setting."
            )
            raise ValueError(error_msg)

        if is_s3_filepath(output_path):
            if single_file:
                wr.s3.to_json(
                    df_out, output_path, orient="records", lines=True, **kwargs
                )
            else:
                wr.s3.to_json(
                    df_out,
                    output_path,
                    orient="records",
                    lines=True,
                    dataset=True,
                    mode=mode,
                    **kwargs,
                )
        else:
            dirs = os.path.dirname(output_path)
            if dirs:
                os.makedirs(dirs, exist_ok=True)

            if mode == "append":
                with io.StringIO() as df_json, open(output_path, "a") as f:
                    df_out.to_json(df_json, orient="records", lines=True, **kwargs)
                    f.write(df_json.getvalue())
            else:
                df_out.to_json(output_path, orient="records", lines=True, **kwargs)


@dataclass
class ArrowParquetWriter(DataFrameFileWriter):
    """write for Parquet files"""

    version: str = "2.0"
    compression: str = "snappy"

    def write(
        self,
        df: Union[pd.DataFrame, Iterable[pd.DataFrame]],
        output_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ) -> None:
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

        kwargs["version"] = kwargs.get("version", self.version)
        kwargs["compression"] = kwargs.get("compression", self.compression)

        if kwargs["version"] != self.version:
            warning_msg = (
                f"Your kwargs for version ({kwargs.get('version')}) mismatches "
                f"the writer's settings self.version ({self.version})."
                "In this instance kwargs supersedes the writer settings."
            )
            warnings.warn(warning_msg)

        if kwargs["compression"] != self.compression:
            warning_msg = (
                f"Your kwargs for compression ({kwargs.get('compression')}) mismatches "
                f"the writer's settings self.compression ({self.compression}). "
                "In this instance kwargs supersedes the writer settings."
            )
            warnings.warn(warning_msg)

        if not output_path.startswith("s3://"):
            dirs = os.path.dirname(output_path)
            if dirs:
                os.makedirs(dirs, exist_ok=True)

        if metadata:
            meta = validate_and_enrich_metadata(metadata)
            arrow_schema = ArrowConverter().generate_from_meta(meta)
        else:
            arrow_schema = None

        chunked = not isinstance(df, pd.DataFrame)
        table = (
            pa.Table.from_pandas(next(df), schema=arrow_schema)
            if chunked
            else pa.Table.from_pandas(df, schema=arrow_schema)
        )
        with pq.ParquetWriter(
            output_path, schema=table.schema, **kwargs
        ) as parquet_writer:
            self._write(table, parquet_writer)
            if chunked:
                for chunk in df:
                    self._write(
                        pa.Table.from_pandas(chunk, arrow_schema),
                        parquet_writer,
                    )

    def _write(
        self,
        table: pa.Table,
        parquet_writer: pq.ParquetWriter,
        **kwargs,
    ):
        """
        Writes a pandas DataFrame to CSV
        parquet_writer: pyarrow.parquet.ParquetWriter to write this and
            potentially other DataFrames
        **kwargs (optional): Additional kwargs are passed to
          pyarrow.parquet.write_table
        """

        parquet_writer.write_table(table, **kwargs)


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
