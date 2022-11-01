import dask.dataframe as dd
import pandas as pd
import pandera as pa
from pandera.typing.dask import DataFrame, Series

df_master = pd.DataFrame(list(range(14, 100000)), columns=['data'])
ddf_master = dd.from_pandas(df_master, npartitions=3)

# define check function with dask
def in_master_table1(series):
    return series.isin(ddf_master['data'].compute())

def in_number(series):
    return series.isin([1,2,3])

def in_str_dummy(series):
    return series.isin(['str1', 'str2'])

##
# format: check รูปแบบถูกไหม
# consistency: เช็ค fk
# completeness: เช็ค null


# Rule Define
rules = {
    # 1 : pa.Check.in_range(min_value=10, max_value=20),
    1 : {
        'func': pa.Check(in_number),
        'type': 'consistency',
    },
    2 : {
        'func': pa.Check(in_str_dummy),
        'type': 'consistency',
    },
    3 : {
        'func':pa.Check(in_master_table1),
        'type': 'consistency',
    },
}