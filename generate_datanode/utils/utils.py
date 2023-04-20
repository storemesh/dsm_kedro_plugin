from re import sub
from sqlalchemy.sql.sqltypes import Integer, Unicode, DateTime, Float, String, BigInteger, Numeric, Boolean, NCHAR, TIMESTAMP, Date, Text, DECIMAL, SmallInteger, CHAR, Enum
from sqlalchemy.dialects.mssql.base import MONEY as mysql_MONEY
from sqlalchemy.dialects.mysql.types import INTEGER as mysql_INTEGER, VARCHAR as mysql_VARCHAR, TINYINT as mysql_TINYINT, TEXT as mysql_TEXT
from sqlalchemy.dialects.mssql.base import TINYINT
from sqlalchemy import sql
import pandas as pd
import dask.dataframe as dd
import hashlib
import yaml
import os
import time
from re import sub
import inflection
from dask.diagnostics import ProgressBar

import sys
import os
sys.path.append(os.getcwd())

from src.dsm_kedro_plugin.generate_datanode.generate_setting import KEDRO_PROJECT_BASE

map_dict = {
    Float: 'float',
    Numeric: 'float',
    Integer: 'Int64',
    BigInteger: 'Int64',
    Unicode: 'string',
    String: 'string',
    # DateTime: 'datetime64',
    NCHAR: 'string',
    DateTime: 'string',
    Boolean: 'bool',   
    TIMESTAMP: 'string',
    Date: 'string',
    Text: 'string',
    DECIMAL: 'float',
    SmallInteger: 'Int32',
    TINYINT: 'Int64',
    CHAR: 'string',
    Enum: 'string',

    
    ## mysql
    mysql_INTEGER: 'Int64',
    mysql_VARCHAR: 'string',
    mysql_TINYINT: 'Int32',
    mysql_MONEY: 'float',
    mysql_TEXT: 'string',

}


def md5hash(x):
    return hashlib.md5(x.encode('utf-8')).hexdigest()

def get_numpy_schema(class_obj):
    schema = {}
    pk_column = None
    for property, value in vars(class_obj).items():        
        if not property.startswith('_'):
            try:
                if value.primary_key:
                    pk_column = property
                
                class_dtype = type(value.type)
                
                if class_dtype not in map_dict:
                    raise KeyError(f"The data type of column '{property}' is {class_dtype} that's not exist in 'map_dict' (use for auto generate schema). Please import and add it in dsm_kedro_plugin/generate_datanode/utils/utils.py")
                
                schema[property] = map_dict[class_dtype]
                
            except AttributeError as e:
                # not do anything yet for InstrumentedAttribute Warning
                pass
                
    if not pk_column:
        raise Exception(f'table "{class_obj}" has no primary key')
        
    return schema, pk_column


def get_numpy_schema_table_alchemy(class_obj):
    schema = {}
    pk_column = None

    list_columns = class_obj.columns.items()
    pk_column = str(list_columns[0][0])
    for column in list_columns:
        column_obj = column[1]
        column_name = column[0]
        class_dtype = type(column_obj.type)
        try:
            schema[column_name] = map_dict[class_dtype]
        except Exception as e:
            print(f'Datatype Mapping Error! --> {column_name}: {e}')
        
    return schema, pk_column


def find_pk_column(class_obj):
    pk_column = None
    for property, value in vars(class_obj).items():        
        if not property.startswith('_'):
            try:
                if value.primary_key:
                    return property
            except Exception as e:
                print(f'{property}: {e}')
    raise Exception(f'table "{class_obj}" has no primary key')

def find_schema(database_id):
    pk_column = None
    return pk_column

def camel_case(ident):
    return ''.join(x[:1].upper() + x[1:] for x in ident.split('_'))

def snake_case(text):
    result = inflection.underscore(text)
    return result

class MissingEnvironmentVariable(Exception):
    pass

def get_env_var(var_name):
    try:
        return os.environ[var_name]
    except KeyError:
        raise MissingEnvironmentVariable(f"{var_name} does not exist")

def get_token():
    credential_path = os.path.join(KEDRO_PROJECT_BASE, "conf/local/credentials.yml")
    with open(credential_path) as f:
        credentials = yaml.safe_load(f)
        credential_dict = credentials['dsmlibrary']
        return credential_dict


def write_dummy_file(file_name, directory_id, data_node):
    df = pd.DataFrame([], columns=['_hash'])
    ddf_mock = dd.from_pandas(df, chunksize=100)
    data_node.write(
        df=ddf_mock,
        directory=directory_id,
        name=file_name, 
        replace=True
    )
    time.sleep(2)
    file_id = data_node.get_file_id(name=f'{file_name}.parquet', directory_id=directory_id)
    return file_id

def create_file_if_not_exist(file_name, directory_id, data_node, overwrite_exist_node=False):
    if overwrite_exist_node:
        file_id = write_dummy_file(file_name, directory_id, data_node)
    else:
        try:
            file_id = data_node.get_file_id(name=f'{file_name}.parquet', directory_id=directory_id)
        except Exception as e:
            print(e, file_name, directory_id)
            file_id = write_dummy_file(file_name, directory_id, data_node)
        
    return file_id