from typing import Any, Dict, Tuple, List

import numpy as np
import sys
import os
sys.path.append(os.getcwd())

from kedro.io import AbstractDataSet
import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import pandas as pd
from dsmlibrary.clickhouse import ClickHouse


def get_info(ddf: dd.DataFrame):
    datas = [{'column_name': col, 'data_type': str(ddf[col].dtype)} for col in ddf.columns]
    return datas

def cast_datatype_for_clickhouse(ddf: dd.DataFrame):
    info = get_info(ddf)
    map_new_type_dict = {
        'string': 'object',
    }
    
    dict_keys = map_new_type_dict.keys()    
    
    for col in ddf.columns:
        current_dtype = str(ddf[col].dtype)
        if current_dtype in dict_keys:
            new_type = map_new_type_dict[current_dtype]
            ddf[col] = ddf[col].astype(new_type)
    
    return ddf    


class ClickHouseDataset(AbstractDataSet[dd.DataFrame, dd.DataFrame]):
    def __init__(
            self, 
            credentials: Dict[str, Any],   
            table_name: str,
            partitioned_column: str, 
            if_exist: str = 'drop', 
        ):
        self._connection = credentials['connection']
        self._table_name = table_name
        self._partitioned_column = partitioned_column
        self._if_exist = if_exist    

    def _load(self) -> pd.DataFrame:
        warehouse = ClickHouse(connection=self._connection)
        df = warehouse.read(f"""
            SELECT *
            FROM {self._table_name}
        """)        
        return df
            
    def _save(self, ddf: dd.DataFrame) -> None:
        ddf = cast_datatype_for_clickhouse(ddf)
        warehouse = ClickHouse(connection=self._connection)
        
        if self._if_exist == 'drop':
            warehouse.dropTable(tableName=self._table_name)
        else:
            raise NotImplementedError

        tableName = warehouse.get_or_createTable(df=ddf, tableName=self._table_name, partition_by=self._partitioned_column)
        warehouse.write(df=ddf, tableName=tableName)


    def _describe(self) -> Dict[str, Any]:
        pass
        
        
    