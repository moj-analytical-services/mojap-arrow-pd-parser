import pandas as pd
from pyarrow import csv
import pyarrow as pa

csv_to_pyarrow = csv.read_csv(input_file="tests/data/string_type.csv")

print(csv_to_pyarrow.schema)

tm = {pa.string(): pd.StringDtype()}

pyarrow_to_pandas = csv_to_pyarrow.to_pandas(types_mapper=tm.get)
pyarrow_to_pandas.dtypes
