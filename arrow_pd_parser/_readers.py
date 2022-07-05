import warnings
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import IO, Callable, Dict, Iterable, List, Optional, Union

import awswrangler as wr
import pandas as pd
import pyarrow as pa
from mojap_metadata import Metadata
from mojap_metadata.converters.arrow_converter import ArrowConverter
from pyarrow import dataset as ds
from pyarrow import parquet as pq

from arrow_pd_parser._arrow_parsers import cast_arrow_table_to_schema
from arrow_pd_parser.caster import cast_pandas_table_to_schema
from arrow_pd_parser.pa_pd import arrow_to_pandas
from arrow_pd_parser.utils import (
    EngineNotImplementedError,
    FileFormat,
    is_s3_filepath,
    validate_and_enrich_metadata,
)


@dataclass
class DataFrameFileReader(ABC):
    """
    Abstract class for reader functions used by reader API
    Should just have a read method returning a pandas DataFrame.
    """

    ignore_columns: List[str] = field(default_factory=list)
    drop_columns: List[str] = field(default_factory=list)
    pd_integer: bool = True
    pd_string: bool = True
    pd_boolean: bool = True
    pd_date_type: str = "datetime_object"
    pd_timestamp_type: str = "datetime_object"
    bool_map: Dict = None

    @abstractmethod
    def read(
        self,
        input_path: str,
        metadata: Union[Metadata, dict] = None,
        is_iterable: bool = False,
        chunksize: Optional[int] = None,
        **kwargs,
    ) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
        """Reads the file into pandas DataFrame or an iterator of DataFrames."""
        return

    def _cast_pandas_table_to_schema(
        self, df: pd.DataFrame, metadata: Union[Metadata, dict]
    ) -> pd.DataFrame:
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

    def _convert_or_cast_frame(
        self, df: pd.DataFrame, metadata: Union[Metadata, dict] = None
    ) -> pd.DataFrame:
        if metadata is None and (self.pd_string or self.pd_integer or self.pd_boolean):
            df = df.convert_dtypes(
                infer_objects=True,
                convert_string=self.pd_string,
                convert_integer=self.pd_integer,
                convert_boolean=self.pd_boolean,
                convert_floating=False,
            )
        else:
            df = self._cast_pandas_table_to_schema(df=df, metadata=metadata)

        return df

    @abstractmethod
    def _read(
        self,
        input_path: str,
        reader: Callable,
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ):
        return

    @abstractmethod
    def _read_iterable(
        self,
        input_path: str,
        chunksize: int,
        reader: Callable,
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ):
        return


class PandasBaseReader(DataFrameFileReader):
    """Base class for pandas readers."""

    expect_full_schema: bool = True

    @abstractmethod
    def read(
        self,
        input_path: str,
        metadata: Metadata = None,
        is_iterable: bool = False,
        chunksize: Optional[int] = None,
        **kwargs,
    ) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
        """
        Reads a CSV file and returns a Pandas DataFrame
        input_path: File to read either local or S3.
        metadata: A metadata object or dict
        **kwargs (optional): Additional kwargs are passed to pandas or awswrangler
            read_csv. Note if metadata is not None then kwargs: low_memory=False
            and dtype=str are set in order to properly cast CSV to metadata schema.
        """

    def _read(
        self,
        input_path: str,
        reader: Callable,
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ):
        df = reader(input_path, **kwargs)
        df = self._convert_or_cast_frame(df=df, metadata=metadata)

        return df

    def _read_iterable(
        self,
        input_path: str,
        chunksize: int,
        reader: Callable,
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ):
        df_iter = reader(input_path, chunksize=chunksize, **kwargs)
        for chunk in df_iter:
            chunk = self._convert_or_cast_frame(df=chunk, metadata=metadata)
            yield chunk


@dataclass
class PandasCsvReader(PandasBaseReader):
    """Reader for CSV files using pandas."""

    def read(
        self,
        input_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        is_iterable: bool = False,
        chunksize: Optional[int] = None,
        **kwargs,
    ) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
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
            reader = wr.s3.read_csv
        else:
            reader = pd.read_csv

        if is_iterable:
            df = self._read_iterable(
                input_path=input_path,
                chunksize=chunksize,
                reader=reader,
                metadata=metadata,
                **kwargs,
            )
        else:
            df = self._read(
                input_path=input_path, reader=reader, metadata=metadata, **kwargs
            )

        return df


@dataclass
class PandasJsonReader(PandasBaseReader):
    """Reader for JSONL files."""

    def read(
        self,
        input_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        is_iterable: bool = False,
        chunksize: Optional[int] = None,
        **kwargs,
    ) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
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
            reader = wr.s3.read_json
        else:
            reader = pd.read_json

        if is_iterable:
            df = self._read_iterable(
                input_path=input_path,
                metadata=metadata,
                chunksize=chunksize,
                reader=reader,
                **kwargs,
            )
        else:
            df = self._read(
                input_path=input_path, metadata=metadata, reader=reader, **kwargs
            )

        return df


class ArrowBaseReader(DataFrameFileReader):
    """Base class for arrow readers."""

    expect_full_schema: bool = True
    read_format: str = None

    def read(
        self,
        input_path: str,
        metadata: Metadata = None,
        is_iterable: bool = False,
        chunksize: Optional[int] = None,
        **kwargs,
    ) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
        """
        Reads a Parquet file and returns a Pandas DataFrame
        input_path: File to read either local or S3.
        metadata: A metadata object or dict
        **kwargs (optional): Additional kwargs are passed to the arrow reader
            arrow.parquet.read_table
        """

        if kwargs is None:
            kwargs = {}

        # filesystem handles determining local vs. s3 path handling
        if "filesystem" not in kwargs:
            # from uri doesn't like relative paths
            input_path = (
                input_path if is_s3_filepath(input_path)
                else os.path.abspath(input_path)
            )
            reader_fs, abstract_path = pa.fs.FileSystem.from_uri(input_path)
            kwargs["filesystem"] = reader_fs
            input_path = abstract_path

        if is_iterable:
            return self._read_iterable(
                input_path=input_path,
                chunksize=chunksize,
                metadata=metadata,
                **kwargs,
            )
        else:
            return self._read(input_path=input_path, metadata=metadata, **kwargs)

    @abstractmethod
    def _read_to_table(self, input_path, **kwargs):
        return

    def _process_schema_and_cast(self, metadata, arrow_table):
        if metadata:
            meta = validate_and_enrich_metadata(metadata)
            schema_from_meta = ArrowConverter().generate_from_meta(meta)
            # validate schema for arrow
            arrow_table = cast_arrow_table_to_schema(
                source_table=arrow_table,
                schema=schema_from_meta,
                expect_full_schema=self.expect_full_schema,
            )
        return arrow_table

    def _cast_arrow_to_pandas(self, arrow_table):
        df = arrow_to_pandas(
            arrow_table,
            pd_boolean=self.pd_boolean,
            pd_integer=self.pd_integer,
            pd_string=self.pd_string,
            pd_date_type=self.pd_date_type,
            pd_timestamp_type=self.pd_timestamp_type,
        )
        return df

    def _read(
        self,
        input_path: str,
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ):

        arrow_table = self._read_to_table(input_path, **kwargs)
        arrow_table = self._process_schema_and_cast(metadata, arrow_table)
        df = self._cast_arrow_to_pandas(arrow_table)

        return df

    def _read_iterable(
        self,
        input_path: str,
        chunksize: int,
        metadata: Union[Metadata, dict] = None,
        **kwargs,
    ):
        if self.read_format == "csv" and self.reader_options:
            self.read_format = ds.CsvFileFormat(convert_options=self.reader_options)

        pa_ds = ds.dataset(source=input_path, format=self.read_format, **kwargs)
        batch_iter = pa_ds.to_batches(batch_size=chunksize)

        for batch in batch_iter:
            arrow_table = pa.Table.from_batches([batch])
            arrow_table = self._process_schema_and_cast(metadata, arrow_table)
            df = self._cast_arrow_to_pandas(arrow_table)

            yield df


@dataclass
class ArrowParquetReader(ArrowBaseReader):
    """Reader for Parquet files."""

    read_format = "parquet"

    def _read_to_table(
        self,
        input_path,
        **kwargs,
    ) -> pa.Table:
        table = pq.read_table(input_path, **kwargs)
        return table


@dataclass
class ArrowCsvReader(ArrowBaseReader):
    """Reader for CSV files using arrow."""

    expect_full_schema = False
    read_format = "csv"
    reader_options = pa.csv.ConvertOptions(strings_can_be_null=True)

    def _read_to_table(
        self,
        input_path,
        **kwargs,
    ) -> pa.Table:

        reader_fs = kwargs.pop("filesystem")

        with reader_fs.open_input_file(input_path) as csv_file:
            table = pa.csv.read_csv(
                csv_file, convert_options=self.reader_options, **kwargs
            )

        return table


def get_reader_for_file_format(
    file_format: Union[FileFormat, str],
    reader_engine: str = None,
) -> DataFrameFileReader:
    # Convert to enum
    if isinstance(file_format, str):
        file_format = FileFormat.from_string(file_format)

    implemented_engines = ["pandas", "arrow"]
    default_engines = {
        FileFormat.CSV: "pandas",
        FileFormat.JSON: "pandas",
        FileFormat.PARQUET: "arrow",
    }
    readers_dict = {
        "pandas": {
            FileFormat.CSV: PandasCsvReader(),
            FileFormat.JSON: PandasJsonReader(),
        },
        "arrow": {
            FileFormat.CSV: ArrowCsvReader(),
            FileFormat.PARQUET: ArrowParquetReader(),
        },
    }

    if file_format not in FileFormat:
        raise ValueError(f"Unsupported file_format {file_format}")

    default_engine = default_engines[file_format]
    default_reader = readers_dict[default_engine][file_format]

    if reader_engine is None:
        reader = default_reader

    elif reader_engine.casefold() in implemented_engines:
        readers_for_format = readers_dict[reader_engine]
        try:
            reader = readers_for_format[file_format]
        except KeyError:
            raise KeyError(
                f"""
                {reader_engine} is not currently supported for the {str(file_format).split('.')[-1].upper()} format.
                The default engine for this {str(file_format).split('.')[-1].upper()} is {default_engine}.

                To use the default engine either pass no argument to reader for
                reader_engine, or pass "{default_engine}".
                """  # noqa: E501
            )

    elif reader_engine.casefold() not in implemented_engines:
        raise EngineNotImplementedError(
            f"""
            {reader_engine} is not currently supported.
            The default for {str(file_format).split('.')[-1].upper()} file type is {default_engine}.

            We plan to support more engine choice in the future. For now we support
            pyarrow ('arrow') and pandas engines. Default engines are:
            CSV: {default_engines[FileFormat.CSV]}, JSON: {default_engines[FileFormat.JSON]}, Parquet: {default_engines[FileFormat.PARQUET]}.
            """  # noqa: E501
        )

    return reader
