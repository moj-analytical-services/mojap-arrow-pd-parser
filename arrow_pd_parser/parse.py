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
            If unset pyarrow will infer datatypes.
        convert_options (dict, optional): dictionary of arguments for pyarrow
            csv.ConvertOptions. Will raise an error if dict has a 'column_types' and
            and schema is provided. This is because the schema will be used as the
            column_types of csv.ConvertOptions.
        parse_options (dict, optional): dictionary of arguments for pyarrow
            csv.ParseOptions. Includes delimiters, quote characters and escape
            characters.
        read_options (dict, optional): dictionary of arguments for
            pyarrow csv.ReadOptions. Includes options for file encoding
            and skipping rows.

    Returns:
        pyarrow.Table: the csv file in pyarrow format.
    """
    if convert_options is None:
        convert_options = {}
    if parse_options is None:
        parse_options = {}
    if read_options is None:
        read_options = {}

    if schema is not None:
        if "column_types" in convert_options:
            raise KeyError(
                "column_types cannot be a set option as schema "
                "is used as this parameter"
            )
        else:
            convert_options["column_types"] = schema
    csv_convert = csv.ConvertOptions(**convert_options)
    csv_parse = csv.ParseOptions(**parse_options)
    csv_read = csv.ReadOptions(**read_options)

    pa_csv_table = csv.read_csv(
        input_file=input_file,
        convert_options=csv_convert,
        parse_options=csv_parse,
        read_options=csv_read,
    )
    return pa_csv_table


def pa_read_json(json_path, parse_options=None, read_options=None):
    """Read a jsonlines file into an Arrow table.

    Args:
        json_path (str): the file path for the jsonl file you want to read.
        parse_options (dict, optional): dictionary of arguments for
            pyarrow json.ParseOptions. Defaults to None.
        read_options (dict, optional): dictionary of arguments for
            pyarrow json.ReadOptions. Defaults to None.

    Returns:
        pyarrow.Table: the jsonl file in pyarrow format.
    """
    if parse_options is None:
        parse_options = {}
    if read_options is None:
        read_options = {}

    json_parse = json.ParseOptions(**parse_options)
    json_read = json.ReadOptions(**read_options)

    pa_json_table = json.read_json(
        json_path, parse_options=json_parse, read_options=json_read,
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
            If unset pyarrow will infer datatypes.
        pd_boolean: whether to use the new pandas boolean format. Defaults to True.
            When set to False, uses a custom boolean format to coerce object type.
        pd_integer: if True, converts integers to Pandas int64 format.
            If False, uses float64. Defaults to True.
        pd_string: Defaults to True.
        pd_date_type (str, optional): specifies the date type. Can be one of: "datetime_object",
            "pd_timestamp" or "pd_period".
        pd_timestamp_type (str, optional): specifies the datetime type. Can be one of: "datetime_object",
            "pd_timestamp" or "pd_period".
        convert_options (dict, optional): dictionary of arguments for pyarrow
            csv.ConvertOptions. Will raise an error if dict has a 'column_types' and
            and schema is provided. This is because the schema will be used as the
            column_types of csv.ConvertOptions.
        parse_options (dict, optional): dictionary of arguments for pyarrow
            csv.ParseOptions. Includes delimiters, quote characters and escape
            characters. Defaults to None.
        read_options (dict, optional): dictionary of arguments for
            pyarrow csv.ReadOptions. Includes options for file encoding
            and skipping rows. Defaults to None.

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
        schema (pyarrow.Schema): pyarrow Schema with the expected columns wanted.
            If unset pyarrow will infer datatypes.
        parse_options (dict, optional): dictionary of arguments for
            pyarrow json.ParseOptions. Will raise an error if dict has an
            'explicit_schema' key and a schema is provided. This is because
            the schema is used as the explicit_schema.
        read_options (dict, optional): dictionary of arguments for
            pyarrow json.ReadOptions.

    Returns:
        pyarrow.Table: the jsonl file in pyarrow format
    """
    if parse_options is None:
        parse_options = {}
    if read_options is None:
        read_options = {}

    if schema is not None:
        if "explicit_schema" in parse_options:
            raise KeyError(
                "explicit_schema cannot be a set option as schema "
                "is used as this parameter"
            )
        else:
            parse_options["explicit_schema"] = schema

    json_parse = json.ParseOptions(**parse_options)
    json_read = json.ReadOptions(**read_options)

    pa_json_table = json.read_json(
        input_file, parse_options=json_parse, read_options=json_read
    )
    return pa_json_table


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
        input_file (Union[IO, str]): the JSONL you want to read. string, path or
            file-like object.
        schema (pyarrow.Schema): pyarrow Schema with the expected columns wanted.
            If unset pyarrow will infer datatypes.
        pd_boolean (bool, optional): if True, converts booleans to Pandas BooleanDtype.
            If False, leaves in the Pandas default bool format.
        pd_integer (bool, optional): if True, converts integers to Pandas Int64Dtype.
            If False, uses float64.
        pd_string (bool, optional): if True, converts integers to Pandas StringDtype.
            If False, leaves in the Pandas default object format.
        pd_date_type (str, optional): specifies the date type. Can be one of: "datetime_object",
            "pd_timestamp" or "pd_period".
        pd_timestamp_type (str, optional): specifies the datetime type. Can be one of: "datetime_object",
            "pd_timestamp" or "pd_period".
        parse_options (dict, optional): dictionary of arguments for
            pyarrow json.ParseOptions. Will raise an error if dict has an
            'explicit_schema' key and a schema is provided. This is because
            the schema is used as the explicit_schema.
        read_options (dict, optional): dictionary of arguments for pyarrow
            json.ReadOptions.

    Returns:
        Pandas DataFrame: the jsonl data as a dataframe, with the specified data types
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
