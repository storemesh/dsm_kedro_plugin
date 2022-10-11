import dask.dataframe as dd
import pandas as pd
import pandera as pa
from pandera.typing.dask import DataFrame, Series

df_master = pd.DataFrame(list(range(14, 100000)), columns=['data'])
ddf_master = dd.from_pandas(df_master, npartitions=3)

# define check function with dask
def in_master_table1(series):
    return series.isin(ddf_master['data'].compute())


# Rule Define
rules = {
    # 1 : pa.Check.in_range(min_value=10, max_value=20),
    1 : pa.Check.isin(['1B', '1A']),
    2 : pa.Check.isin(['rule2_1', 'rule2_2']),
    3 : pa.Check(in_master_table1),
}