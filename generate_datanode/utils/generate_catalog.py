from dsmlibrary.datanode import DataNode, DatabaseManagement
from jinja2 import Environment, FileSystemLoader, BaseLoader
from sqlalchemy import sql
import datetime as dt
import os
import pandas as pd
import dask.dataframe as dd
import time 
import inspect
import tempfile
from importlib import util
import importlib.machinery

from .utils import camel_case, snake_case, find_pk_column, get_numpy_schema, get_numpy_schema_table_alchemy, md5hash, get_env_var, create_file_if_not_exist
from generate_setting import KEDRO_PROJECT_BASE, JINJA_PATH, SQL_DATANODE_CATALOG_PATH, LANDING_CATALOG_PATH, INTEGRATION_CATALOG_PATH

from src.config.project_setting import DATAPLATFORM_API_URI, OBJECT_STORAGE_URI, OBJECT_STORAGE_SECUE

def generate_sql_datanode(source_table, project_folder_id, sql_datanode_folder_name, token, write_mode=False):
    if write_mode:
        with open(SQL_DATANODE_CATALOG_PATH, 'w') as f:
            f.write('')

    database = DatabaseManagement(
        token=token,
        dataplatform_api_uri=DATAPLATFORM_API_URI,
        object_storage_uri=OBJECT_STORAGE_URI,
        object_storage_secue= OBJECT_STORAGE_SECUE,
    )
    data_node = DataNode(
        token, 
        dataplatform_api_uri=DATAPLATFORM_API_URI,
        object_storage_uri=OBJECT_STORAGE_URI,
        object_storage_secue= OBJECT_STORAGE_SECUE,
    )
    sql_datanode_folder_id = data_node.get_directory_id(parent_dir_id=project_folder_id, name=sql_datanode_folder_name)
    
    node_list = []
    for database_id, table_list in source_table.items():
        print(f'--- dabase id: {database_id} ----- ')
        
        # exec to define class object                 
        database_meta, schema = database.get_database_schema(database_id=database_id)
        exec(schema, globals())

        database_name = database_meta['name']
        database_name = database_name.replace(' ', '_')

        for table_name in table_list:
            snake_table_name = snake_case(table_name)
            catalog_name = f'sql.{database_name}_{snake_table_name}'
            file_name = f'{database_name}_{snake_table_name}'   

            if table_name[:2] == 't_':
                object_table_name = table_name
            else:
                object_table_name = camel_case(table_name) 
            
            # get query function
            query_template = f'''
def query():
    return sql.select(['*']).select_from({object_table_name})
'''  
            
            table_class_obj = eval(object_table_name)            
            if table_name[:2] == 't_':
                meta, pk_column = get_numpy_schema_table_alchemy(table_class_obj)
            else:
                meta, pk_column = get_numpy_schema(table_class_obj)    
            
            database.write_sql_query(
                query_function=query_template,
                directory_id=sql_datanode_folder_id, 
                database_id=database_id, 
                pk_column=pk_column, 
                name=file_name, 
                meta=meta,
                replace=True
            )

            data = {
                'catalog_name': catalog_name,
                'file_name': file_name,
                'folder_id': sql_datanode_folder_id,
            }

            node_list.append(data)
        
    if write_mode:
        env_node = Environment(loader=FileSystemLoader(JINJA_PATH))  
        template = env_node.get_template(f'sql_datanode.yml')           
        with open(SQL_DATANODE_CATALOG_PATH, 'a') as f:
            f.write(template.render({ "node_list": node_list}))

# Landing
def generate_landing_pipeline(
        source_table, 
        project_folder_id,
        sql_datanode_folder_name, 
        landing_folder_name, 
        query_landing_pipeline_path, 
        token, 
        write_mode=True,
        overwrite_exist_node=False,
        generate_source_dict=None
    ):
    if write_mode:

        env_node = Environment(loader=FileSystemLoader(JINJA_PATH))

        with open(LANDING_CATALOG_PATH, 'w') as f:
            f.write('')
            
        

        landing_node_path = os.path.join(query_landing_pipeline_path, 'nodes.py')
        template = env_node.get_template(f'landing_nodes.py')     
        with open(landing_node_path, 'w') as f:
            f.write(template.render({}))

        landing_pipeline_path = os.path.join(query_landing_pipeline_path, 'pipeline.py')
        with open(landing_pipeline_path, 'w') as f:
            f.write('')

    node_list = []
    database = DatabaseManagement(
        token=token,
        dataplatform_api_uri=DATAPLATFORM_API_URI,
        object_storage_uri=OBJECT_STORAGE_URI,
        object_storage_secue=OBJECT_STORAGE_SECUE,
    )
    data_node = DataNode(
        token, 
        dataplatform_api_uri=DATAPLATFORM_API_URI,
        object_storage_uri=OBJECT_STORAGE_URI,
        object_storage_secue=OBJECT_STORAGE_SECUE,
    )

    sql_datanode_folder_id = data_node.get_directory_id(parent_dir_id=project_folder_id, name=sql_datanode_folder_name)
    landing_folder_id = data_node.get_directory_id(parent_dir_id=project_folder_id, name=landing_folder_name)
    
    for database_id, table_list in source_table.items():
        database_meta, schema = database.get_database_schema(database_id=database_id)
        database_name = database_meta['name']
        database_name = database_name.replace(' ', '_')
        for table_name in table_list:     
            check_generate_file = True     
            if generate_source_dict != None:
                if (not database_id in generate_source_dict) or (not table_name in generate_source_dict[database_id]):
                    check_generate_file = False
            
            snake_table_name = snake_case(table_name)
            sql_query_catalog_name = f'sql.{database_name}_{snake_table_name}'
            landing_file_name = f'{database_name}_{snake_table_name}'
            landing_catalog_name = f'l.{landing_file_name}'
            print(landing_catalog_name)

            data = {
                'file_name': table_name,
                'sql_query_catalog_name': sql_query_catalog_name,
                'landing_catalog_name': landing_catalog_name,
                'landing_file_name': landing_file_name,
                'folder_id': landing_folder_id,
                'database_name': database_name,               
            }
            node_list.append(data)

    if write_mode:                  
        template = env_node.get_template(f'landing_catalog.yml')           
        with open(LANDING_CATALOG_PATH, 'a') as f:
            f.write(template.render({ "node_list": node_list}))                

        template = env_node.get_template(f'landing_pipeline.py')     
        with open(landing_pipeline_path, 'w') as f:
            f.write(template.render({ "node_list": node_list}))


# integration
def generate_integration_catalogs(
    integration_table, 
    project_folder_id,
    integration_folder_name, 
    token, 
    append=False
):  
    with open(INTEGRATION_CATALOG_PATH, 'w') as f:
        f.write('')    

    node_list = []
    data_node = DataNode(
        token, 
        dataplatform_api_uri=DATAPLATFORM_API_URI,
        object_storage_uri=OBJECT_STORAGE_URI,
        object_storage_secue=OBJECT_STORAGE_SECUE,
    )
    integration_folder_id = data_node.get_directory_id(parent_dir_id=project_folder_id, name=integration_folder_name)
    
    for table_name, config in integration_table.items():  
        catalog_name = f'integration___{table_name}'
        data = {
            'catalog_name': catalog_name,
            'file_name': table_name,            
            'folder_id': integration_folder_id,
            'config': config,
        }
        node_list.append(data)

    env_node = Environment(loader=FileSystemLoader(JINJA_PATH))  
    if append:
        template_name = 'datanode_append.yml'
    else:
        template_name = 'datanode.yml'
        
    template = env_node.get_template(template_name)           
    with open(INTEGRATION_CATALOG_PATH, 'a') as f:
        f.write(template.render({ "node_list": node_list}))
