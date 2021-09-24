from mojap_metadata.metadata.metadata import Metadata
from mojap_metadata.converters.arrow_converter import ArrowConverter
import pyarrow as pa
from pyarrow import csv, json, parquet

from arrow_pd_parser.pa_pd import arrow_to_pandas

from typing import Union, IO


def _get_arrow_schema(schema: Union[pa.schema, Metadata, dict]):
    ac = ArrowConverter()
    if isinstance(schema, Metadata):
        schema = ac.generate_to_meta(schema)
    elif isinstance(schema, dict):
        schema = Metadata.from_dict(schema)
        schema = ac.generate_to_meta(schema)
    elif isinstance(schema, pa.Schema):
        pass
    else:
        raise TypeError(f"schema type not allowed: {type(schema)}")

    return schema


def update_existing_schema(
    current_schema: pa.Schema, new_schema: pa.Schema
) -> pa.Schema:
    """
    Takes the current schema and updates any fields in the current
    schema with fields from the new_schema. If current_schema has
    fields that do not exist in new_schema then they are unchanged.
    If current_schema has fields that also exist in new_schema then
    the field in new_schema is chosen. If fields exist in new_schema
    but not in current, these will be ignored.
    Args:
        current_schema (pa.Schema): Schema to update
        new_schema (pa.Schema): Schema with fields that you wish to be
          used to update current_schema
    Returns:
        pa.Schema: Returns a schema with the same column order as
        current_schema but with the fields updated for any fields
        that matched new_schema.
    """

    updated_schema = pa.schema([])

    for field in current_schema:
        if field.name in new_schema.names:
            updated_schema = updated_schema.append(new_schema.field(field.name))
        else:
            updated_schema = updated_schema.append(field)
    return updated_schema


def cast_arrow_table_to_schema(
    tab: pa.Table,
    schema: Union[pa.Schema, None] = None,
    expect_full_schema: bool = True,
):
    """Casts an arrow schema to a new or partial schema
    Args:
        tab (pa.Table): An arrow table
        schema (Union[pa.Schema, None], optional): [description]. Defaults to None.
        expect_full_schema (bool, optional): if True, pyarrow reader will
            expect the input schema to have fields for every col in the
            input file. If False, then will only cast columns that
            are listed in the schema, leaving all other columns to their
            default type on read.
    """

    if expect_full_schema:
        update_schema = schema
    else:
        update_schema = update_existing_schema(tab.schema, schema)

    new_tab = tab.cast(update_schema)

    return new_tab


def pa_read_csv(
    input_file: Union[IO, str],
    schema: Union[pa.Schema, None] = None,
    expect_full_schema: bool = True,
    **kwargs,
):
    """Read a csv file into an Arrow table.
    Args:
        input_file (Union[IO, str]): the CSV you want to read. string, path or
            file-like object.
        schema (pyarrow.Schema): pyarrow Schema with the expected columns wanted.
            If unset pyarrow will infer datatypes.
        expect_full_schema (bool, optional): if True, pyarrow reader will
            expect the input schema to have fields for every col in the
            input file. If False, then will only cast columns that
            are listed in the schema, leaving all other columns to their
            default type on read.
        **kwargs (optional): Additional kwargs are passed to pyarrow.csv.read_csv
    Returns:
        pyarrow.Table: the csv file in pyarrow format.
    """

    if schema:
        schema = _get_arrow_schema(schema)

    pa_csv_table = csv.read_csv(input_file=input_file, **kwargs)
    if schema:
        pa_csv_table = cast_arrow_table_to_schema(
            pa_csv_table, schema=schema, expect_full_schema=expect_full_schema
        )

    return pa_csv_table


def pa_read_csv_to_pandas(
    input_file: Union[IO, str],
    schema: Union[pa.Schema, Metadata, dict] = None,
    expect_full_schema: bool = True,
    pd_boolean: bool = True,
    pd_integer: bool = True,
    pd_string: bool = True,
    pd_date_type: str = "datetime_object",
    pd_timestamp_type: str = "datetime_object",
    **kwargs,
):
    """Read a csv file into an Arrow table and convert it to a Pandas DataFrame.
    Args:
        input_file (Union[IO, str]): the CSV you want to read. string, path or
            file-like object.
        schema (pyarrow.Schema): pyarrow Schema with the expected columns wanted.
            If unset pyarrow will infer datatypes.
        expect_full_schema (bool, optional): if True, pyarrow reader will
            expect the input schema to have fields for every col in the
            input file. If False, then will only cast columns that
            are listed in the schema, leaving all other columns to their
            default type on read.
        pd_boolean: whether to use the new pandas boolean format. Defaults to True.
            When set to False, uses a custom boolean format to coerce object type.
        pd_integer: if True, converts integers to Pandas int64 format.
            If False, uses float64. Defaults to True.
        pd_string: Defaults to True.
        pd_date_type (str, optional): specifies the date type. Can be one of:
            "datetime_object", "pd_timestamp" or "pd_period".
        pd_timestamp_type (str, optional): specifies the datetime type. Can be one of:
            "datetime_object", "pd_timestamp" or "pd_period".
        **kwargs (optional): Additional kwargs are passed to pyarrow.csv.read_csv
    Returns:
        Pandas DataFrame: the csv data as a dataframe, with the specified data types
    """
    arrow_table = pa_read_csv(input_file, schema, expect_full_schema, **kwargs)

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
    schema: Union[pa.Schema, Metadata, dict] = None,
    expect_full_schema: bool = True,
    **kwargs,
):
    """Read a jsonlines file into an Arrow table.
    Args:
        input_file (Union[IO, str]): the JSONL you want to read. string, path or
            file-like object.
        schema (pyarrow.Schema): pyarrow Schema with the expected columns wanted.
            If unset pyarrow will infer datatypes.
        expect_full_schema (bool, optional): if True, pyarrow reader will
            expect the input schema to have fields for every col in the
            input file. If False, then will only cast columns that
            are listed in the schema, leaving all other columns to their
            default type on read.
        **kwargs (optional): Additional kwargs are passed to pyarrow.json.read_json
    Returns:
        pyarrow.Table: the jsonl file in pyarrow format casted to the specified schema
    """

    if schema:
        schema = _get_arrow_schema(schema)

    pa_json_table = json.read_json(input_file, **kwargs)

    if schema:
        pa_json_table = cast_arrow_table_to_schema(
            pa_json_table, schema=schema, expect_full_schema=expect_full_schema
        )

    return pa_json_table


def pa_read_json_to_pandas(
    input_file: Union[IO, str],
    schema: Union[pa.Schema, Metadata, dict] = None,
    expect_full_schema: bool = True,
    pd_boolean: bool = True,
    pd_integer: bool = True,
    pd_string: bool = True,
    pd_date_type: str = "datetime_object",
    pd_timestamp_type: str = "datetime_object",
    **kwargs,
):
    """Read a jsonlines file into an Arrow table and convert it to a Pandas DataFrame.
    Args:
        input_file (Union[IO, str]): the JSONL you want to read. string, path or
            file-like object.
        schema (pyarrow.Schema): pyarrow Schema with the expected columns wanted.
            If unset pyarrow will infer datatypes.
        expect_full_schema (bool, optional): if True, pyarrow reader will
            expect the input schema to have fields for every col in the
            input file. If False, then will only cast columns that
            are listed in the schema, leaving all other columns to their
            default type on read.
        pd_boolean (bool, optional): if True, converts booleans to Pandas BooleanDtype.
            If False, leaves in the Pandas default bool format.
        pd_integer (bool, optional): if True, converts integers to Pandas Int64Dtype.
            If False, uses float64.
        pd_string (bool, optional): if True, converts integers to Pandas StringDtype.
            If False, leaves in the Pandas default object format.
        pd_date_type (str, optional): specifies the date type. Can be one of:
            "datetime_object", "pd_timestamp" or "pd_period".
        pd_timestamp_type (str, optional): specifies the datetime type. Can be one of:
            "datetime_object", "pd_timestamp" or "pd_period".
        **kwargs (optional): Additional kwargs are passed to pyarrow.json.read_json
    Returns:
        Pandas DataFrame: the jsonl data as a dataframe, with the specified data types
    """
    arrow_table = pa_read_json(input_file, schema, expect_full_schema, **kwargs)

    df = arrow_to_pandas(
        arrow_table,
        pd_boolean=pd_boolean,
        pd_integer=pd_integer,
        pd_string=pd_string,
        pd_date_type=pd_date_type,
        pd_timestamp_type=pd_timestamp_type,
    )

    return df


def pa_read_parquet(
    input_file: str,
    schema: Union[pa.Schema, Metadata, dict] = None,
    expect_full_schema: bool = True,
    **kwargs,
):

    """
    reads parquet file to in memory arrow table
    Args:
        input_file (str): path (s3 or local) to the parquet file to read in
        schema (pa.Schema, optional): schema to cast the data to. Defaults to None.
        expect_full_schema (bool, optional): expect full schema. Defaults to True.
        kwargs (optional): kwargs to pass to pyarrow.parquet.read_table
    Returns:
        pyarrow table: data in an in memory arrow table
    """

    if not isinstance(input_file, str):
        raise TypeError("currently only supports string paths for input")
    if schema:
        schema = _get_arrow_schema(schema)

    pa_parquet_table = parquet.read_table(input_file, **kwargs)

    if schema:
        pa_parquet_table = cast_arrow_table_to_schema(
            pa_parquet_table, schema=schema, expect_full_schema=expect_full_schema
        )

    return pa_parquet_table


def pa_read_parquet_to_pandas(
    input_file: str,
    schema: Union[pa.Schema, Metadata, dict] = None,
    expect_full_schema: bool = True,
    pd_boolean: bool = True,
    pd_integer: bool = True,
    pd_string: bool = True,
    pd_date_type: str = "datetime_object",
    pd_timestamp_type: str = "datetime_object",
    **kwargs,
):
    """
    reads a parquet file to pandas dataframe with various type casting options
    Args:
        input_file (str): path (s3 or local) to the parquet file to read in
        schema (pa.Schema, optional): schema to cast the data to. Defaults to None.
        expect_full_schema (bool, optional): expect full schema. Defaults to True.
        pd_boolean (bool, optional): [description]. Defaults to True.
        pd_integer (bool, optional): [description]. Defaults to True.
        pd_string (bool, optional): [description]. Defaults to True.
        pd_date_type (str, optional): [description]. Defaults to "datetime_object".
        pd_timestamp_type (str, optional): [description]. Defaults to "datetime_object".
        kwargs (optional) : kwargs to pass to pyarrow.parquet.read_table
    Returns:
        pandas dataframe: pandas dataframe of the given input data
    """

    if not isinstance(input_file, str):
        raise TypeError("currently only supports string paths for input")

    arrow_table = pa_read_parquet(input_file, schema, expect_full_schema, **kwargs)

    df = arrow_to_pandas(
        arrow_table,
        pd_boolean=pd_boolean,
        pd_integer=pd_integer,
        pd_string=pd_string,
        pd_date_type=pd_date_type,
        pd_timestamp_type=pd_timestamp_type,
    )

    return df
