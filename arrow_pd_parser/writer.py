from typing import Iterator, Union

import pandas as pd
from mojap_metadata import Metadata

from arrow_pd_parser._writers import (
    ArrowParquetWriter,
    PandasCsvWriter,
    PandasJsonWriter,
    get_writer_for_file_format,
)
from arrow_pd_parser.utils import FileFormat, infer_file_format


def write(
    df: Union[pd.DataFrame, Iterator[pd.DataFrame]],
    output_path: str,
    metadata: Union[Metadata, dict] = None,
    file_format: Union[FileFormat, str] = None,
    writer_engine: str = None,
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

    writer = get_writer_for_file_format(
        file_format=file_format, writer_engine=writer_engine
    )

    return writer.write(
        df=df,
        output_path=output_path,
        metadata=metadata,
        **kwargs,
    )


csv = PandasCsvWriter()
json = PandasJsonWriter()
parquet = ArrowParquetWriter()
