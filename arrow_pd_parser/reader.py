from typing import Iterable, Optional, Union

import pandas as pd
from mojap_metadata import Metadata

from arrow_pd_parser._readers import (
    ArrowParquetReader,
    PandasCsvReader,
    PandasJsonReader,
    get_reader_for_file_format,
)
from arrow_pd_parser.utils import FileFormat, human_to_bytes, infer_file_format


def read(
    input_path: str,
    metadata: Union[Metadata, dict] = None,
    file_format: Union[FileFormat, str] = None,
    parquet_expect_full_schema: bool = True,
    chunksize: Optional[Union[int, str]] = None,
    reader_engine: str = None,
    **kwargs,
) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
    """
    A function wrapper for csv.read(), json.read() or parquet.read().
    Has an additional parameter "file_format". Will use the appropriate
    reader based on the value of file_format (can be a str or FileFormat
    Enum (PARQUET, CSV or JSON)).

    If file_format=None, then will try to infer file format from input_path
    and failing that metadata. Will error if no file type can be achieved.

    If chunksize is not None, will return an Iterator of dataframes.
    If chunksize is an int, each dataframe will have chunksize rows.
    chunksize can be also set to a string representing memory, e.g. "2GB",
    "500 MB". Chunksize will then be set to the number of rows that will fill
    the given memory. Do not set this value to the amount of memory available,
    there will need to be plenty of overhead for reading and writing the data
    format.

    See csv.read(), json.read() or parquet.read() for docsctring on
    other params.
    """
    if file_format is None:
        file_format = infer_file_format(input_path, metadata)
    elif isinstance(file_format, str):
        file_format = FileFormat.from_string(file_format)
    else:
        pass

    is_iterable = chunksize is not None

    if isinstance(chunksize, str):
        max_bytes = human_to_bytes(chunksize)
        test_reader = get_reader_for_file_format(file_format=file_format)
        df = next(test_reader.read(input_path))
        bytes_per_1000 = df.memory_usage(deep=True).sum()
        chunksize = int(1000 * max_bytes / bytes_per_1000)

    reader = get_reader_for_file_format(
        file_format=file_format, reader_engine=reader_engine
    )
    kwargs.pop("reader_engine", None)

    if file_format == FileFormat.PARQUET:
        reader.expect_full_schema = parquet_expect_full_schema

    return reader.read(
        input_path=input_path,
        metadata=metadata,
        is_iterable=is_iterable,
        chunksize=chunksize,
        **kwargs,
    )


csv = PandasCsvReader()
json = PandasJsonReader()
parquet = ArrowParquetReader()
