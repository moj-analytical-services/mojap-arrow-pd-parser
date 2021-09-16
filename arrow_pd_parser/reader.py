import pandas as pd
from mojap_metadata import Metadata
from typing import Union

from arrow_pd_parser._readers import (
    ArrowParquetReader,
    PandasCsvReader,
    PandasJsonReader,
    get_default_reader_from_file_format,
)
from arrow_pd_parser.utils import infer_file_format, FileFormat


def read(
    input_file: str,
    metadata: Union[Metadata, dict] = None,
    file_format: Union[FileFormat, str] = None,
    parquet_cast_post_read: bool = True,
    **kwargs,
) -> pd.DataFrame:
    """
    A function wrapper for csv.read(), json.read() or parquet.read().
    Has an additional parameter "file_format". Will use the appropriate
    reader based on the value of file_format (can be a str or FileFormat
    Enum (PARQUET, CSV or JSON)).

    If file_format=None, then will try to infer file format from input_file
    and failing that metadata. Will error if no file type can be achieved.

    See csv.read(), json.read() or parquet.read() for docsctring on
    other params.
    """
    if file_format is None:
        file_format = infer_file_format(input_file, metadata)
    elif isinstance(file_format, str):
        file_format = FileFormat.from_string(file_format)
    else:
        pass

    reader = get_default_reader_from_file_format(file_format=file_format)
    if file_format == FileFormat.PARQUET and not parquet_cast_post_read:
        reader.cast_post_read = False

    return reader.read(
        input_file=input_file,
        metadata=metadata,
        **kwargs,
    )


csv = PandasCsvReader()
json = PandasJsonReader()
parquet = ArrowParquetReader()
