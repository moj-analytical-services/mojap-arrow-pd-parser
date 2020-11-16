import numpy as np
import pyarrow as pa
import pandas as pd


def generate_type_mapper(pd_boolean, pd_integer, pd_string):
    tm = {}
    if pd_boolean:
        bool_map = {pa.bool_(): pd.BooleanDtype()}
        tm = {**tm, **bool_map}

    if pd_integer:
        int_map = {
            pa.int8(): pd.Int64Dtype(),
            pa.int16(): pd.Int64Dtype(),
            pa.int32(): pd.Int64Dtype(),
            pa.int64(): pd.Int64Dtype(),
            pa.uint8(): pd.Int64Dtype(),
            pa.uint16(): pd.Int64Dtype(),
            pa.uint32(): pd.Int64Dtype(),
            pa.uint64(): pd.Int64Dtype(),
        }
        tm = {**tm, **int_map}
    else:
        int_map = {
            pa.int8: np.float64,
            pa.int16: np.float64,
            pa.int32: np.float64,
            pa.int64: np.float64,
            pa.uint8: np.float64,
            pa.uint16: np.float64,
            pa.uint32: np.float64,
            pa.uint64: np.float64,
        }
        tm = {**tm, **int_map}

    if tm:
        return tm.get
    else:
        return None


def arrow_to_pandas(
    arrow_table,
    pd_boolean=True,
    pd_integer=True,
    pd_string=True,
    date_type: str = None,
    datetime_type: str = None,
):
    """Converts arrow table to stricter pandas datatypes based on options.

    Args:
        arrow_table (pa.Table): An arrow table

        new_integer (bool, optional): converts bools to the new pandas BooleanDtype.
        Otherwise will convert to bool (if not nullable) and object of (True, False, None) if nulls exist. Defaults to True.

        new_string (bool, optional): [description]. Defaults to True.

    Returns:
        [type]: [description]
    """
    tm = generate_type_mapper(pd_boolean, pd_integer, pd_string)
    return arrow_table.to_pandas(types_mapper=tm, datetime_as_object=True)
