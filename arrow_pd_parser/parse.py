import pyarrow as pa
from pyarrow import csv, json

from arrow_pd_parser.pa_pd import arrow_to_pandas

from typing import Union, IO


def pa_read_csv(
    input_file: Union[IO, str],
    schema: Union[pa.Schema, None] = None,
    convert_options: dict = None,
    parse_options: dict = None,
    read_options: dict = None,
):
    """Read a csv file into an Arrow table.

    Args:
        input_file (Union[IO, str]): the CSV you want to read. string, path or
            file-like object.
        schema (pyarrow.Schema): pyarrow Schema with the expected columns wanted.
            The schema is passed to the column_types parameter to the
            csv.ConvertOptions. If unset pyarrow will infer datatypes.
            Defaults to None.
        convert_options (dict, optional): dictionary of arguments for pyarrow
            csv.ConvertOptions. Includes options for which columns to include
            and leave out. Will raise an error if dict has a column_types key and
            schema parameter is not None. This is because the schema parameter of
            this function will be used for column_types in the csv.ConvertOptions.
        parse_options (dict, optional): dictionary of arguments for pyarrow
            csv.ParseOptions. Includes delimiters, quote characters and escape
            characters. Defaults to None.
        read_options (dict, optional): dictionary of arguments for
            pyarrow csv.ReadOptions. Includes options for file encoding
            and skipping rowsDefaults to None.

    Returns:
        pyarrow.Table: the csv file in pyarrow format
    """
    if convert_options is None:
        convert_options = {}
    if parse_options is None:
        parse_options = {}
    if read_options is None:
        read_options = {}

    if ("column_types" in convert_options) and (schema is not None):
        raise KeyError(
            "column_types cannot be set as schema parameter"
            "is passed to this parameter in the Convert Option"
        )
    csv_convert = csv.ConvertOptions(column_types=schema, **convert_options)
    csv_parse = csv.ParseOptions(**parse_options)
    csv_read = csv.ReadOptions(**read_options)

    pa_csv_table = csv.read_csv(
        input_file=input_file,
        convert_options=csv_convert,
        parse_options=csv_parse,
        read_options=csv_read,
    )
    return pa_csv_table


def pa_read_json(
    input_file: Union[IO, str],
    schema: pa.Schema = None,
    parse_options: dict = None,
    read_options: dict = None,
):
    """Read a jsonlines file into an Arrow table.

    Args:
        input_file (Union[IO, str]): the JSONL you want to read. string, path or
            file-like object.
        schema (pyarrow.Schema, optional): pyarrow Schema with the expected columns
            wanted. The schema is passed to the explicit_schema parameter to the
            json.ParseOptions. If unset pyarrow will infer datatypes. Defaults to None.
        parse_options (dict, optional): dictionary of arguments for
            pyarrow json.ParseOptions. Defaults to None.
        read_options (dict, optional): dictionary of arguments for
            pyarrow json.ReadOptions. Defaults to None.

    Returns:
        pyarrow.Table: the jsonl file in pyarrow format
    """
    if parse_options is None:
        parse_options = {}
    if read_options is None:
        read_options = {}

    if ("explicit_schema" in parse_options) and (schema is not None):
        raise KeyError(
            "column_types cannot be set as schema parameter"
            "is passed to this parameter in the Convert Option"
        )
    json_parse = json.ParseOptions(explicit_schema=schema, **parse_options)
    json_read = json.ReadOptions(**read_options)

    pa_json_table = json.read_json(
        input_file, parse_options=json_parse, read_options=json_read
    )
    return pa_json_table


def pa_read_csv_to_pandas(
    input_file: Union[IO, str],
    schema: pa.Schema = None,
    pd_boolean: bool = True,
    pd_integer: bool = True,
    pd_string: bool = True,
    pd_date_type: str = "datetime_object",
    pd_timestamp_type: str = "datetime_object",
    convert_options=None,
    parse_options=None,
    read_options=None,
):
    """Read a csv file into an Arrow table and convert it to a Pandas DataFrame.

    Args:
        input_file (Union[IO, str]): the CSV you want to read. string, path or
            file-like object.
        schema (pyarrow.Schema): pyarrow Schema with the expected columns wanted.
            The schema is passed to the column_types parameter to the
            csv.ConvertOptions. If unset pyarrow will infer datatypes.
            Defaults to None.
        pd_boolean: whether to use the new pandas boolean format. Defaults to True.
            When set to False, uses a custom boolean format to coerce object type.
        pd_integer: if True, converts integers to Pandas int64 format.
            If False, uses float64. Defaults to True.
        pd_string: Defaults to True.
        pd_date_type: spcifies the date type. Defaults to "datetime_object".
        pd_timestamp_type: spcifies the timestamp type. Defaults to "datetime_object".
        convert_options (dict, optional): dictionary of arguments for pyarrow
            csv.ConvertOptions. Includes options for which columns to include
            and leave out. Will raise an error if dict has a column_types key and
            schema parameter is not None. This is because the schema parameter of
            this function will be used for column_types in the csv.ConvertOptions.
        parse_options (dict, optional): dictionary of arguments for pyarrow
            csv.ParseOptions. Includes delimiters, quote characters and escape
            characters. Defaults to None.
        read_options (dict, optional): dictionary of arguments for
            pyarrow csv.ReadOptions. Includes options for file encoding
            and skipping rowsDefaults to None.

    Returns:
        Pandas DataFrame: the csv data as a dataframe, with the specified data types
    """
    arrow_table = pa_read_csv(
        input_file, schema, convert_options, parse_options, read_options
    )
    df = arrow_to_pandas(
        arrow_table,
        pd_boolean=pd_boolean,
        pd_integer=pd_integer,
        pd_string=pd_string,
        pd_date_type=pd_date_type,
        pd_timestamp_type=pd_timestamp_type,
    )

    return df


def pa_read_json_to_pandas(
    input_file: Union[IO, str],
    schema: pa.Schema = None,
    pd_boolean: bool = True,
    pd_integer: bool = True,
    pd_string: bool = True,
    pd_date_type: str = "datetime_object",
    pd_timestamp_type: str = "datetime_object",
    parse_options=None,
    read_options=None,
):
    """Read a jsonlines file into an Arrow table and convert it to a Pandas DataFrame.

    Args:
        input_file (Union[IO, str]): the CSV you want to read. string, path or
            file-like object.
        schema (pyarrow.Schema, optional): pyarrow Schema with the expected columns
            wanted. The schema is passed to the explicit_schema parameter to the
            json.ParseOptions. If unset pyarrow will infer datatypes. Defaults to None.
        pd_boolean (bool, optional): if True, converts booleans to Pandas BooleanDtype.
            If False, leaves in the Pandas default bool format. Defaults to True.
        pd_integer (bool, optional): if True, converts integers to Pandas Int64Dtype.
            If False, uses float64. Defaults to True.
        pd_string (bool, optional): if True, converts integers to Pandas StringDtype.
            If False, leaves in the Pandas default object format. Defaults to True.
        pd_date_type (str, optional): specifies the date type.
            Defaults to "datetime_object".
        pd_timestamp_type (str, optional): specifies the datetime type.
            Defaults to "datetime_object".
        parse_options (dict, optional): dictionary of arguments for pyarrow
            json.ParseOptions. Defaults to None.
        read_options (dict, optional): dictionary of arguments for pyarrow
            json.ReadOptions. Defaults to None.

    Returns:
        Pandas DataFrame: the csv data as a dataframe, with the specified data types
    """
    arrow_table = pa_read_json(input_file, schema, parse_options, read_options)
    df = arrow_to_pandas(
        arrow_table,
        pd_boolean=pd_boolean,
        pd_integer=pd_integer,
        pd_string=pd_string,
        pd_date_type=pd_date_type,
        pd_timestamp_type=pd_timestamp_type,
    )

    return df
