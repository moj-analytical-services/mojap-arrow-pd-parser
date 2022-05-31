import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import IO, Dict, Iterable, Optional, Union

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

    ignore_columns: list[str] = field(default_factory=list)
    drop_columns: list[str] = field(default_factory=list)
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
        self, df: pd.DataFrame, metadata: Union[Metadata, dict]
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


@dataclass
class PandasCsvReader(DataFrameFileReader):
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

        if is_iterable:
            if is_s3_filepath(input_path):
                df_iter = wr.s3.read_csv(input_path, chunksize=chunksize, **kwargs)
            else:
                df_iter = pd.read_csv(input_path, chunksize=chunksize, **kwargs)

            for df in df_iter:
                df = self._convert_or_cast_frame(df=df)

            yield df
        else:
            if is_s3_filepath(input_path):
                df = wr.s3.read_csv(input_path, **kwargs)
            else:
                df = pd.read_csv(input_path, **kwargs)

            df = self._convert_or_cast_frame(df=df)

            return df


@dataclass
class PandasJsonReader(DataFrameFileReader):
    """Reader for JSON files."""

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

        if is_iterable:
            if is_s3_filepath(input_path):
                df_iter = wr.s3.read_json(input_path, chunksize=chunksize, **kwargs)
            else:
                df_iter = pd.read_json(input_path, chunksize=chunksize, **kwargs)

            for df in df_iter:
                df = self._convert_or_cast_frame(df=df)

            yield df
        else:
            if is_s3_filepath(input_path):
                df = wr.s3.read_json(input_path, **kwargs)
            else:
                df = pd.read_json(input_path, **kwargs)

            df = self._convert_or_cast_frame(df=df)

            return df


class ArrowBaseReader(DataFrameFileReader):
    """Reader for Parquet files."""

    expect_full_schema: bool = True

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

        arrow_table = self._read_to_table(input_path, **kwargs)

        if metadata:
            meta = validate_and_enrich_metadata(metadata)
            schema = ArrowConverter().generate_from_meta(meta)
            arrow_table = cast_arrow_table_to_schema(
                arrow_table,
                schema=schema,
                expect_full_schema=self.expect_full_schema,
            )

        df = arrow_to_pandas(
            arrow_table,
            pd_boolean=self.pd_boolean,
            pd_integer=self.pd_integer,
            pd_string=self.pd_string,
            pd_date_type=self.pd_date_type,
            pd_timestamp_type=self.pd_timestamp_type,
        )

        return df

    @abstractmethod
    def _read_to_table(self, input_path, **kwargs):
        return


@dataclass
class ArrowParquetReader(ArrowBaseReader):
    """Reader for Parquet files."""

    def _read_to_table(
        self,
        input_path,
        is_iterable: bool = False,
        chunksize: Optional[int] = None,
        **kwargs,
    ) -> pa.Table:
        table = pq.read_table(input_path, **kwargs)
        return table


@dataclass
class ArrowCsvReader(ArrowBaseReader):
    """Reader for CSV files using arrow."""

    def _read_to_table(
        self,
        input_path,
        is_iterable: bool = False,
        chunksize: Optional[int] = None,
        **kwargs,
    ) -> pa.Table:
        table = pa.csv.read_csv(
            input_path,
            convert_options=pa.csv.ConvertOptions(strings_can_be_null=True),
            **kwargs,
        )
        return table


@dataclass
class DataFrameFileReaderIterator(DataFrameFileReader):
    """
    Abstract class for reader functions used by reader API.
    Should just have a read method returning an iterable of pandas DataFrames.
    """

    # Number of lines to read in per pass
    chunksize: int = 65536

    @abstractmethod
    def read(
        self,
        input_path: str,
        metadata: Union[Metadata, dict] = None,
        chunksize: int = 65536,
        **kwargs,
    ) -> Iterable[pd.DataFrame]:
        """Reads the file into pandas DataFrame."""


@dataclass
class PandasCsvReaderIterator(DataFrameFileReaderIterator):
    """Reader for CSV files."""

    def read(
        self,
        input_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        chunksize: int = 65536,
        **kwargs,
    ) -> Iterable[pd.DataFrame]:
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
            df_iter = wr.s3.read_csv(input_path, chunksize=chunksize, **kwargs)
        else:
            df_iter = pd.read_csv(input_path, chunksize=chunksize, **kwargs)

        for df in df_iter:
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

            yield df


@dataclass
class PandasJsonReaderIterator(DataFrameFileReaderIterator):
    """Reader for JSON files."""

    def read(
        self,
        input_path: Union[IO, str],
        metadata: Union[Metadata, dict] = None,
        chunksize: int = 65536,
        **kwargs,
    ) -> Iterable[pd.DataFrame]:
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
            df_iter = wr.s3.read_json(input_path, chunksize=chunksize, **kwargs)
        else:
            df_iter = pd.read_json(input_path, chunksize=chunksize, **kwargs)

        for df in df_iter:
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

            yield df


@dataclass
class ArrowParquetReaderIterator(DataFrameFileReaderIterator):
    """Reader for Parquet files."""

    expect_full_schema: bool = True

    def read(
        self, input_path: str, metadata: Metadata = None, **kwargs
    ) -> Iterable[pd.DataFrame]:
        """
        Reads a Parquet file and returns a Pandas DataFrame
        input_path: File to read either local or S3.
        metadata: A metadata object or dict
        **kwargs (optional): Additional kwargs are passed to the arrow reader
            arrow.parquet.read_table
        """

        pa_ds = ds.dataset(input_path, **kwargs)
        batch_iter = pa_ds.to_batches(batch_size=self.chunksize)

        for batch in batch_iter:
            arrow_tab = pa.Table.from_batches([batch])

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

            yield df


def get_default_reader_from_file_format(
    file_format: Union[FileFormat, str],
    is_iterable: bool = False,
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
        # reader = PandasCsvReaderIterator() if is_iterable else PandasCsvReader()
        reader = PandasCsvReader()
    elif file_format == FileFormat.JSON:
        reader = PandasJsonReader()
    elif file_format == FileFormat.PARQUET:
        reader = ArrowParquetReader()
    else:
        raise ValueError(f"Unsupported file_format {file_format}")

    # implemented_engines = ["pandas", "arrow"]
    # default_engines = {
    #     FileFormat.CSV: "pandas",
    #     FileFormat.JSON: "pandas",
    #     FileFormat.PARQUET: "arrow",
    # }
    # readers_dict = {
    #     "pandas": {
    #         FileFormat.CSV: PandasCsvReader(),
    #         FileFormat.JSON: PandasJsonReader(),
    #     },
    #     "arrow": {
    #         FileFormat.CSV: ArrowCsvReader(),
    #         FileFormat.PARQUET: ArrowParquetReader(),
    #     },
    # }

    # if file_format not in FileFormat:
    #     raise ValueError(f"Unsupported file_format {file_format}")

    # default_engine = default_engines[file_format]
    # default_reader = readers_dict[default_engine][file_format]

    # if reader_engine is None:
    #     reader = default_reader

    # elif reader_engine.casefold() in implemented_engines:
    #     readers_for_format = readers_dict[reader_engine]
    #     try:
    #         reader = readers_for_format[file_format]
    #     except KeyError:
    #         raise KeyError(
    #             f"""
    #             {reader_engine} is not currently supported for the {str(file_format).split('.')[-1].upper()} format.
    #             This {str(file_format).split('.')[-1].upper()} file will use {default_engine}.
    #             """  # noqa: E501
    #         )

    # elif reader_engine.casefold() not in implemented_engines:
    #     raise EngineNotImplementedError(
    #         f"""
    #         {reader_engine} is not currently supported.
    #         The default for {str(file_format).split('.')[-1].upper()} file type is {default_engine}.

    #         We plan to support more engine choice in the future. For now we support
    #         pyarrow ('arrow') and pandas engines. Default engines are:
    #         CSV: {default_engines[FileFormat.CSV]}, JSON: {default_engines[FileFormat.JSON]}, Parquet: {default_engines[FileFormat.PARQUET]}.
    #         """  # noqa: E501
    #     )

    return reader
