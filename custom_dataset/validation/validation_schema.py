import sys
import os
sys.path.append(os.getcwd())

import pandera as pa
import pandas as pd
from pydantic import BaseModel
import dask.dataframe as dd
import datetime 
from typing import Literal, List, Dict

from src.config.validation_rules import rules
import time

## pydantic schema
class Column(BaseModel):    
    data_type: Literal['int', 'str', 'string', 'float','float64', 'int32', 'Int32','Int64', 'Float32', 'datetime','datetime64[ns]','datetime64[ms]','datetime64[s]', 'bool','long']
    is_required: bool = False
    nullable: bool = False    
    validation_rule: List[int] = []

class SchemaConfigs(BaseModel):
    columns: Dict[str, Column] = []
    is_strict: bool = False
    validation_rule: List[int] = []
    pk_column: str


# validate_schema
def validate_schema(df, schema, pk=None):
    """Validate pd.DataFrame with pandera schema.

    Args:
        df (pd.DataFrame): pandas dataframe to validate
        schema (pa.DataFrameSchema): pandera schema
        pk (str): primary key column

    Returns:
        pd.DataFrame: Error dataframe that columns name are as follows.
                   'schema_context', 'pk', 'column', 'input', 'rule_name'
    
    Examples
        --------
        Example returns results        
        
        >>> df_result
             schema_context   pk column  input          rule_name
        0.0           Column  1.0   city      1  in_range(10, 20)
        0.0           Column  1.0  price      1  in_range(10, 20)
        1.0           Column  2.0   city      1  in_range(10, 20)
        NaN            Index  NaN   None  int64      dtype('str')
        NaN  DataFrameSchema  NaN   None     pk  column_in_schema   

    """

    try:
        schema.validate(df, lazy=True)
        df_empty = pd.DataFrame([], columns=['schema_context', 'pk', 'column', 'input', 'rule_name'])        
        return df_empty 
    except pa.errors.SchemaErrors as err:
        df_error = err.failure_cases
        df_error = df_error.set_index('index')
        df_result = df_error.join(df)
        df_result = df_result[['column', 'failure_case', pk, 'check', 'schema_context']]
        df_result = df_result.rename(columns={
            'failure_case': 'input',
            pk: 'pk',
            'check': 'rule_name' 
        })
        
        df_result['pk'] = df_result['pk'].astype('string')
        df_result['column'] = df_result['column'].astype('string')
        df_result['input'] = df_result['input'].astype('string')
        df_result['rule_name'] = df_result['rule_name'].astype('string')
        df_result = df_result[['schema_context', 'pk', 'column', 'input', 'rule_name']]
        
        
        return df_result


class ValidationException(Exception):
    pass

def generate_schema(config):
    schema_dict = {}
    for key, value in config['columns'].items():
        validation_rules = [rules[validation_id]['func'] for validation_id in value['validation_rule']]
        schema_dict[key] = pa.Column(value['data_type'], nullable=value['nullable'], checks=validation_rules)

    
    dataframe_validation_rules = [rules[validation_id]['func'] for validation_id in config['validation_rule']]
    schema = pa.DataFrameSchema(
        schema_dict, 
        strict=config['is_strict'], 
        checks=dataframe_validation_rules,
    )

    return schema

# @profile 
def validate_data(ddf, config):
    config_validated = SchemaConfigs(**config)
    validated_config = config_validated.dict()
    schema = generate_schema(validated_config)

    ddf_result = ddf.map_partitions(validate_schema, schema, pk=validated_config['pk_column'])
    ddf_critical_error = ddf_result[ddf_result['pk'].isnull()].drop_duplicates()
    ddf_critical_error = ddf_critical_error.drop(columns=['pk'])

    ddf_rule_error = ddf_result[~ddf_result['pk'].isnull()]
    ddf_rule_error = ddf_rule_error.drop(columns=['schema_context'])
        
    ddf_critical_error = ddf_critical_error[['schema_context', 'column', 'input', 'rule_name']]
    ddf_rule_error = ddf_rule_error[['pk', 'column', 'input', 'rule_name']]

    # add is_required
    is_required_list = [ {'column': key, 'is_required': value['is_required'] } for key, value in validated_config['columns'].items()]
    df_is_required = pd.DataFrame(is_required_list)
    ddf_is_required = dd.from_pandas(df_is_required, chunksize=100000)  
    ddf_rule_error = ddf_rule_error.merge(ddf_is_required, on='column', how='left')
    ddf_rule_error['is_required'] = ddf_rule_error['is_required'].fillna(False)
    
    return ddf_critical_error, ddf_rule_error
