import pandas as pd
from mojap_metadata import Metadata
from typing import Union, Iterable

from arrow_pd_parser._readers import (
    ArrowParquetReader,
    PandasCsvReader,
    PandasJsonReader,
    ArrowParquetReaderIterator,
    PandasCsvReaderIterator,
    PandasJsonReaderIterator,
    get_default_reader_from_file_format,
)
from arrow_pd_parser.utils import infer_file_format, FileFormat


def read(
    input_path: str,
    metadata: Union[Metadata, dict] = None,
    file_format: Union[FileFormat, str] = None,
    parquet_expect_full_schema: bool = True,
    **kwargs,
) -> pd.DataFrame:
    """
    A function wrapper for csv.read(), json.read() or parquet.read().
    Has an additional parameter "file_format". Will use the appropriate
    reader based on the value of file_format (can be a str or FileFormat
    Enum (PARQUET, CSV or JSON)).

    If file_format=None, then will try to infer file format from input_path
    and failing that metadata. Will error if no file type can be achieved.

    See csv.read(), json.read() or parquet.read() for docsctring on
    other params.
    """
    if file_format is None:
        file_format = infer_file_format(input_path, metadata)
    elif isinstance(file_format, str):
        file_format = FileFormat.from_string(file_format)
    else:
        pass

    reader = get_default_reader_from_file_format(file_format=file_format)
    if file_format == FileFormat.PARQUET:
        reader.expect_full_schema = parquet_expect_full_schema

    return reader.read(
        input_path=input_path,
        metadata=metadata,
        **kwargs,
    )


def read_iter(
    input_path: str,
    chunksize: int = 65536,
    metadata: Union[Metadata, dict] = None,
    file_format: Union[FileFormat, str] = None,
    parquet_expect_full_schema: bool = True,
    **kwargs,
) -> Iterable[pd.DataFrame]:
    """
    A function wrapper for iterable versions of csv.read() and json.read(), and
    parquet.ParquetFile.iter_batches(). The parameter "chunksize" controls the
    number of rows per batch.

    Has an additional parameter "file_format". Will use the appropriate
    reader based on the value of file_format (can be a str or FileFormat
    Enum (PARQUET, CSV or JSON)).

    If file_format=None, then will try to infer file format from input_path
    and failing that metadata. Will error if no file type can be achieved.

    See csv.read(), json.read() or parquet.ParquetFile.iter_batches() for
    docstring on other params.
    """
    if file_format is None:
        file_format = infer_file_format(input_path, metadata)
    elif isinstance(file_format, str):
        file_format = FileFormat.from_string(file_format)
    else:
        pass

    reader = get_default_reader_from_file_format(file_format=file_format, iterator=True)
    if file_format == FileFormat.PARQUET:
        reader.expect_full_schema = parquet_expect_full_schema

    yield from reader.read_iter(
        input_path=input_path,
        chunksize=chunksize,
        metadata=metadata,
        **kwargs,
    )


csv = PandasCsvReader()
json = PandasJsonReader()
parquet = ArrowParquetReader()
csv_iter = PandasCsvReaderIterator()
json_iter = PandasJsonReaderIterator()
parquet_iter = ArrowParquetReaderIterator()
