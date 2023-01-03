import datetime
import io
import os
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import IO, Dict, Iterable, List, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import smart_open
from mojap_metadata import Metadata
from mojap_metadata.converters.arrow_converter import ArrowConverter

from arrow_pd_parser.utils import (
    EngineNotImplementedError,
    FileFormat,
    is_s3_filepath,
    validate_and_enrich_metadata,
)


@dataclass
class DataFrameFileWriter(ABC):
    """
    Abstract class for writer functions used by writer API.
    Should just have a write method.
    """

    compression: str = None
    copy: bool = True
    ignore_columns: List[str] = field(default_factory=list)
    drop_columns: List[str] = field(default_factory=list)
    pd_integer: bool = True
    pd_string: bool = True
    pd_boolean: bool = True
    pd_date_type: str = "datetime_object"
    pd_timestamp_type: str = "datetime_object"
    bool_map: Dict = None

    @abstractmethod
    def write(
        self,
        df: Union[pd.DataFrame, Iterable[pd.DataFrame]],
        output_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ) -> None:
        """
        Writes a DataFrame or iterator of DataFrames to the output file
        output_path: File to write either local or S3.
        metadata: A metadata object or dict to cast the dataframe to before writing
          (not necessarily needed for writing especially for CSV)
        **kwargs (optional): Additional kwargs are passed to write method.
        """
        return


@dataclass
class DataFrameTextFileWriter(DataFrameFileWriter):
    def write(
        self,
        df: Union[pd.DataFrame, Iterable[pd.DataFrame]],
        output_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ) -> None:
        """
        Writes a DataFrame or iterator of DataFrames to the output file
        output_path: File to write either local or S3.
        metadata: A metadata object or dict to cast the dataframe to before writing
          (not necessarily needed for writing especially for CSV)
        **kwargs (optional): Additional kwargs are passed to write method.
        """

        if kwargs.get("mode", "w") != "w":
            raise ValueError("Only writing is supported, so 'mode' needs to equal 'w'")

        if isinstance(df, pd.DataFrame):
            # Convert single dataframe to iterator
            df = iter([df])

        # Create directory, if needed
        if not is_s3_filepath(output_path):
            dirs = os.path.dirname(output_path)
            if dirs:
                os.makedirs(dirs, exist_ok=True)

        with smart_open.open(output_path, "w") as f:
            # Write first chunk from iterable
            self._write(next(df), f, metadata, first_chunk=True, **kwargs)
            # then write the rest
            for chunk in df:
                self._write(chunk, f, metadata, first_chunk=False, **kwargs)

    @abstractmethod
    def _write(
        self,
        df: pd.DataFrame,
        f: io.TextIOWrapper,
        metadata: Union[Metadata, dict] = None,
        first_chunk: bool = True,
        **kwargs,
    ):
        return


@dataclass
class PandasCsvWriter(DataFrameTextFileWriter):
    """Write a DataFrame to CSV file using pandas."""

    drop_index = True

    def _write(
        self,
        df: pd.DataFrame,
        f: io.TextIOWrapper,
        metadata: Union[Metadata, dict] = None,
        first_chunk: bool = True,
        **kwargs,
    ) -> None:
        """
        Writes a pandas DataFrame to CSV
        f: File-like object to write either local or S3.
        metadata: A metadata object or dict to cast the dataframe to before writing
          (not necessarily needed for writing especially for CSV)
        first_chunk: Is this the first part of the dataframe, potentially
          needing headers?
        **kwargs (optional): Additional kwargs are passed to pandas to_csv
            method.
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

        df_out.to_csv(f, header=first_chunk, **kwargs)


@dataclass
class PandasJsonWriter(DataFrameTextFileWriter):
    """Write to JSONL file using pandas"""

    def _write(
        self,
        df: pd.DataFrame,
        f: io.TextIOWrapper,
        metadata: Union[Metadata, dict] = None,
        first_chunk: bool = True,
        **kwargs,
    ) -> None:
        """
        Writes a pandas DataFrame to JSONL
        output_path: File to write either to local or S3.
        metadata: A metadata object or dict to cast the dataframe to before writing
          (not necessarily needed for writing)
        **kwargs (optional): Additional kwargs are passed to pandas or awswrangler
            to_json method.
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

        df_out.to_json(f, orient="records", lines=True, **kwargs)


@dataclass
class ArrowBaseWriter(DataFrameFileWriter):
    """
    Base writer class for arrow engine writers.
    """

    def write(
        self,
        df: Union[pd.DataFrame, Iterable[pd.DataFrame]],
        output_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ) -> None:
        """
        Writes a pandas DataFrame
        output_path: File to write to either local or S3.
        metadata: A metadata object or dict to cast the dataframe to before writing
          (not necessarily needed for writing)
        **kwargs (optional): Additional kwargs are passed to
          pyarrow writers
        """

        if kwargs is None:
            kwargs = {}

        kwargs["version"] = kwargs.get("version", self.version)
        kwargs["compression"] = kwargs.get("compression", self.compression).upper()

        if kwargs["version"] != self.version:
            warning_msg = (
                f"Your kwargs for version ({kwargs.get('version')}) mismatches "
                f"the writer's settings self.version ({self.version})."
                "In this instance kwargs supersedes the writer settings."
            )
            warnings.warn(warning_msg)

        if kwargs["compression"].casefold() != self.compression.casefold():
            warning_msg = (
                f"Your kwargs for compression ({kwargs.get('compression')}) mismatches "
                f"the writer's settings self.compression ({self.compression}). "
                "In this instance kwargs supersedes the writer settings."
            )
            warnings.warn(warning_msg)

        if not is_s3_filepath(output_path):
            dirs = os.path.dirname(output_path)
            if dirs:
                os.makedirs(dirs, exist_ok=True)

        if metadata:
            meta = validate_and_enrich_metadata(metadata)
            arrow_schema = ArrowConverter().generate_from_meta(meta)
        else:
            arrow_schema = None

        if isinstance(df, pd.DataFrame):
            # Convert single dataframe to iterator
            df = iter([df])

        #######

        self._write(df, output_path, arrow_schema, **kwargs)

    @abstractmethod
    def _write(
        self,
        df: Iterable[pd.DataFrame],
        output_path: Union[IO, str],
        arrow_schema: pa.Schema = None,
        **kwargs,
    ) -> None:
        return


@dataclass
class ArrowParquetWriter(ArrowBaseWriter):
    """Writer for Parquet files using pyarrow."""

    # TODO limit to valid compression options #####

    compression: str = "SNAPPY"
    version: str = "2.6"

    def _write(
        self,
        df: Iterable[pd.DataFrame],
        output_path: Union[IO, str],
        arrow_schema: pa.Schema = None,
        **kwargs,
    ) -> None:
        table = pa.Table.from_pandas(next(df), schema=arrow_schema)

        with pq.ParquetWriter(
            where=output_path, schema=table.schema, **kwargs
        ) as parquet_writer:
            parquet_writer.write_table(table)
            for chunk in df:
                table = pa.Table.from_pandas(chunk, schema=arrow_schema)
                parquet_writer.write_table(table)
        written_arrow_schema = pq.read_schema(output_path)
        if arrow_schema:
            mismatched_types = {}
            for i, written_col in enumerate(written_arrow_schema):
                schema_col = arrow_schema[i]
                if not written_col.equals(schema_col):
                    mismatched_types[written_col.name] = {
                        "type_in_schema": schema_col.type,
                        "type_in_written_file": written_col.type,
                    }
            if mismatched_types:
                warnings.warn(
                    f"""
                    Arrow has converted the types of some columns.
                    Consider updating your metadata to match the data more accurately.
                    {mismatched_types}"""
                )


@dataclass
class ArrowCsvWriter(PandasCsvWriter):
    """Use pandas engine choice to write CSV files using pyarrow."""

    def arrow_write(self, **kwargs) -> None:
        self._write(engine="pyarrow", **kwargs)


def get_writer_for_file_format(
    file_format: Union[FileFormat, str],
    writer_engine: str = None,
) -> DataFrameFileWriter:

    # Convert to enum
    if isinstance(file_format, str):
        file_format = FileFormat.from_string(file_format)

    implemented_engines = ["pandas", "arrow"]
    default_engines = {
        FileFormat.CSV: "pandas",
        FileFormat.JSON: "pandas",
        FileFormat.PARQUET: "arrow",
    }
    writers_dict = {
        "pandas": {
            FileFormat.CSV: PandasCsvWriter(),
            FileFormat.JSON: PandasJsonWriter(),
        },
        "arrow": {
            FileFormat.CSV: ArrowCsvWriter(),
            FileFormat.PARQUET: ArrowParquetWriter(),
        },
    }

    if file_format not in FileFormat:
        raise ValueError(f"Unsupported file_format {file_format}")

    default_engine = default_engines[file_format]
    default_writer = writers_dict[default_engine][file_format]

    if writer_engine is None:
        writer = default_writer

    elif writer_engine.casefold() in implemented_engines:
        writers_for_format = writers_dict[writer_engine]
        try:
            writer = writers_for_format[file_format]
        except KeyError:
            raise KeyError(
                f"""
                {writer_engine} is not currently supported for the {str(file_format).split('.')[-1].upper()} format.
                The default engine for this {str(file_format).split('.')[-1].upper()} is {default_engine}.

                To use the default engine either pass no argument to reader for
                writer_engine, or pass "{default_engine}".
                """  # noqa: E501
            )

    elif writer_engine.casefold() not in implemented_engines:
        raise EngineNotImplementedError(
            f"""
            {writer_engine} is not currently supported.
            The default for {str(file_format).split('.')[-1].upper()} file type is {default_engine}.

            We plan to support more engine choice in the future. For now we support
            pyarrow ('arrow') and pandas engines. Default engines are:
            CSV: {default_engines[FileFormat.CSV]}, JSON: {default_engines[FileFormat.JSON]}, Parquet: {default_engines[FileFormat.PARQUET]}.
            """  # noqa: E501
        )

    return writer
