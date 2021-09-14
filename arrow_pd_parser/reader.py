import pandas as pd
from mojap_metadata import Metadata
from arrow_pd_parser._readers import (
    ReaderAPI,
    get_reader_from_file_format,
)
from utils import infer_file_format, FileFormat
from typing import List, Union


def read(
    self,
    input_file: str,
    metadata: Union[Metadata, dict] = None,
    file_format: Union[FileFormat, str] = None,
    ignore_columns: List = None,
    drop_columns: List = None,
    pd_integer: bool = True,
    pd_string: bool = True,
    pd_boolean: bool = True,
    pd_date_type: str = "datetime_object",
    pd_timestamp_type: str = "datetime_object",
    bool_map=None,
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
    else:
        reader = get_reader_from_file_format(file_format)

    r = ReaderAPI(reader=reader)
    return r.read(
        input_file=input_file,
        metadata=metadata,
        file_format=file_format,
        ignore_columns=ignore_columns,
        drop_columns=drop_columns,
        pd_integer=pd_integer,
        pd_string=pd_string,
        pd_boolean=pd_boolean,
        pd_date_type=pd_date_type,
        pd_timestamp_type=pd_timestamp_type,
        bool_map=bool_map,
        **kwargs,
    )


csv = ReaderAPI("csv")
json = ReaderAPI("json")
parquet = ReaderAPI("parquet")
