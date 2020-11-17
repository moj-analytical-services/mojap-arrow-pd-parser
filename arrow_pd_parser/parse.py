from pyarrow import csv

from arrow_pd_parser.pa_pd import arrow_to_pandas


def pa_read_csv(
    csv_path,
    test_col_types,
    convert_options=None,
    parse_options=None,
    read_options=None,
):
    if convert_options is None:
        convert_options = {}
    if parse_options is None:
        parse_options = {}
    if read_options is None:
        read_options = {}

    csv_convert = csv.ConvertOptions(column_types=test_col_types, **convert_options)
    csv_parse = csv.ParseOptions(**parse_options)
    csv_read = csv.ReadOptions(**read_options)

    pa_csv_table = csv.read_csv(
        csv_path,
        convert_options=csv_convert,
        parse_options=csv_parse,
        read_options=csv_read,
    )
    return pa_csv_table


def pa_read_csv_to_pandas(
    csv_path,
    test_col_types,
    pd_boolean: bool = True,
    pd_integer: bool = True,
    pd_string: bool = True,
    pd_date_type: str = "datetime_object",
    pd_timestamp_type: str = "datetime_object",
    convert_options=None,
    parse_options=None,
    read_options=None,
):

    arrow_table = pa_read_csv(
        csv_path, test_col_types, convert_options, parse_options, read_options
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
