from typing import Union

import pandas as pd
from mojap_metadata import Metadata

from arrow_pd_parser._writers import (
    ArrowParquetWriter,
    PandasCsvWriter,
    PandasJsonWriter,
    get_default_writer_from_file_format,
)
from arrow_pd_parser.utils import infer_file_format, FileFormat


def write(
    df: pd.DataFrame,
    output_path: str,
    metadata: Union[Metadata, dict] = None,
    file_format: Union[FileFormat, str] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    A function wrapper for csv.write(), json.write() or parquet.write().
    Has an additional parameter "file_format". Will use the appropriate
    reader based on the value of file_format (can be a str or FileFormat
    Enum (PARQUET, CSV or JSON)).

    If file_format=None, then will try to infer file format from output_path
    and failing that metadata. Will error if no file type can be achieved.

    See csv.write(), json.write() or parquet.write() for docsctring on
    other params.
    """
    if file_format is None:
        file_format = infer_file_format(output_path, metadata)
    elif isinstance(file_format, str):
        file_format = FileFormat.from_string(file_format)
    else:
        pass

    writer = get_default_writer_from_file_format(file_format=file_format)

    return writer.write(
        df,
        output_path,
        metadata=metadata,
        **kwargs,
    )


csv = PandasCsvWriter()
json = PandasJsonWriter()
parquet = ArrowParquetWriter()
