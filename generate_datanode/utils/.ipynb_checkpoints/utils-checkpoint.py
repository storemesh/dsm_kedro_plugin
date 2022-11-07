from re import sub
from sqlalchemy.sql.sqltypes import Integer, Unicode, DateTime, Float, String, BigInteger, Numeric, Boolean, NCHAR, TIMESTAMP, Date, Text, DECIMAL
from sqlalchemy.dialects.mysql.types import INTEGER as mysql_INTEGER, VARCHAR as mysql_VARCHAR, TINYINT as mysql_TINYINT
from sqlalchemy import sql
import pandas as pd
import dask.dataframe as dd
import hashlib
import yaml
import os
import time
from dask.diagnostics import ProgressBar

import sys
import os
sys.path.append(os.getcwd())

from src.dsm_kedro_plugin.generate_datanode.generate_setting import KEDRO_PROJECT_BASE
from src.config.config_database import db_connection, db_schema

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
    
    
    ## mysql
    mysql_INTEGER: 'Int64',
    mysql_VARCHAR: 'string',
    mysql_TINYINT: 'Int32',
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
                schema[property] = map_dict[class_dtype]
            except Exception as e:
                print(f'{property}: {e}')
                
    if not pk_column:
        raise Exception(f'table "{class_obj}" has no primary key')
        
    return schema, pk_column



def find_system_detail(database_id):
    return db_connection[database_id]

def get_database_schema(database_id):
    return db_schema[database_id]

def find_pk_column(class_obj):
    pk_column = None
    for property, value in vars(class_obj).items():        
        if not property.startswith('_'):
            try:
                # import pdb; pdb.set_trace()
                if value.primary_key:
                    return property
            except Exception as e:
                print(f'{property}: {e}')
    raise Exception(f'table "{class_obj}" has no primary key')

def find_schema(database_id):
    pk_column = None
    return pk_column


# def camel_case(s):
#   s = sub(r"(_|-)+", " ", s).title().replace(" ", "")
#   return ''.join([s[0].upper(), s[1:]])

def camel_case(ident):
    return ''.join(x[:1].upper() + x[1:] for x in ident.split('_'))

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
        token = credentials['dsmlibrary']['token']
        return token


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