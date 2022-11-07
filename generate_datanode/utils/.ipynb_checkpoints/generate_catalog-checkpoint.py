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

from .utils import find_system_detail, camel_case, find_pk_column, get_numpy_schema, get_database_schema, md5hash, get_env_var, create_file_if_not_exist
from generate_setting import KEDRO_PROJECT_BASE, JINJA_PATH, SQL_DATANODE_CATALOG_PATH, LANDING_CATALOG_PATH, INTEGRATION_CATALOG_PATH


def generate_sql_datanode(source_table, project_folder_id, sql_datanode_folder_name, token, write_mode=False):
    if write_mode:
        with open(SQL_DATANODE_CATALOG_PATH, 'w') as f:
            f.write('')

    database = DatabaseManagement(token=token)
    data_node = DataNode(token)
    sql_datanode_folder_id = data_node.get_directory_id(parent_dir_id=project_folder_id, name=sql_datanode_folder_name)
    
    for database_id, table_list in source_table.items():
        # exec to define class object         
        table_id = get_database_schema(database_id)
        meta, schema = database.get_table_schema(table_id=table_id) # get_database_schema(database_id)
        exec(schema, globals())
        
        for table_name in table_list:
            database_name = find_system_detail(database_id)
            database_name = database_name.replace(' ', '_')
            catalog_name = f'SQLDataNode___{database_name}___{table_name}'
            camel_case_table_name = camel_case(table_name)   

            # get query function
            query_template = f'''
def query():
    return sql.select(['*']).select_from({camel_case_table_name})
'''  
            
            table_class_obj = eval(camel_case_table_name)
            meta, pk_column = get_numpy_schema(table_class_obj)   

            database.write_sql_query(
                # df=ddf,
                query_function=query_template,
                directory_id=sql_datanode_folder_id, 
                table_id=table_id, 
                pk_column=pk_column, 
                name=catalog_name, 
                meta=meta,
                replace=True
            )

            data = {
                'catalog_name': catalog_name,
                'file_name': table_name,
                'folder_id': sql_datanode_folder_id,
            }
            if write_mode:
                env_node = Environment(loader=FileSystemLoader(JINJA_PATH))  
                template = env_node.get_template(f'sql_datanode.yml')           
                with open(SQL_DATANODE_CATALOG_PATH, 'a') as f:
                    f.write(template.render(data))

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

#         # update_latest_landing
#         update_latest_landing_node_path = os.path.join(project_path, 'pipelines/update_latest_landing/nodes.py')
#         template = env_node.get_template(f'landing_nodes.py')     
#         with open(update_latest_landing_node_path, 'w') as f:
#             f.write(template.render({}))

#         update_latest_landing_pipeline_path = os.path.join(project_path, 'pipelines/update_latest_landing/pipeline.py')
#         with open(update_latest_landing_pipeline_path, 'w') as f:
#             f.write('')

    node_list = []
    data_node = DataNode(token)
    sql_datanode_folder_id = data_node.get_directory_id(parent_dir_id=project_folder_id, name=sql_datanode_folder_name)
    landing_folder_id = data_node.get_directory_id(parent_dir_id=project_folder_id, name=landing_folder_name)
    
    for database_id, table_list in source_table.items():
        for table_name in table_list:     
            check_generate_file = True     
            if generate_source_dict != None:
                if (not database_id in generate_source_dict) or (not table_name in generate_source_dict[database_id]):
                    check_generate_file = False

            database_name = find_system_detail(database_id)
            database_name = database_name.replace(' ', '_')
            sql_query_catalog_name = f'SQLDataNode___{database_name}___{table_name}'
            landing_catalog_name = f'landing___{database_name}___{table_name}'
            # landing_latest_catalog_name = f'landing___{database_name}___{table_name}___latest'
            # landing_temp_catalog_name = f'landing___{database_name}___{table_name}___temp'
            # landing_change_catalog_name = f'landing___{database_name}___{table_name}___change'
            print(landing_catalog_name)

            # allocate file id (if not exist)
            # if check_generate_file:
#             landing_file_id = create_file_if_not_exist(
#                 file_name=table_name, 
#                 directory_id=landing_folder_id[database_id],
#                 data_node=data_node,
#                 overwrite_exist_node=overwrite_exist_node,

#             )
            # landing_temp_file_id = create_file_if_not_exist(
            #     file_name=table_name, 
            #     directory_id=landing_folder_id[database_id]['temp'],
            #     data_node=data_node,
            #     overwrite_exist_node=overwrite_exist_node,
            # )
            # landing_change_file_id = create_file_if_not_exist(
            #     file_name=table_name, 
            #     directory_id=landing_folder_id[database_id]['change'],
            #     data_node=data_node,
            #     overwrite_exist_node=overwrite_exist_node,
            # )
           
            data = {
                'file_name': table_name,
                'sql_query_catalog_name': sql_query_catalog_name,
                'landing_catalog_name': landing_catalog_name,
                # 'landing_latest_catalog_name': landing_latest_catalog_name,
                # 'landing_temp_catalog_name': landing_temp_catalog_name,
                # 'landing_change_catalog_name': landing_change_catalog_name,
                'folder_id': landing_folder_id,
                'database_name': database_name,
                # 'landing_file_id': landing_file_id,
                # 'landing_latest_file_id': landing_latest_file_id,
                # 'landing_temp_file_id': landing_temp_file_id,
                # 'landing_change_file_id': landing_change_file_id,                
            }
            node_list.append(data)

    if write_mode:                  
        template = env_node.get_template(f'landing_catalog.yml')           
        with open(LANDING_CATALOG_PATH, 'a') as f:
            f.write(template.render({ "node_list": node_list}))                

        template = env_node.get_template(f'landing_pipeline.py')     
        with open(landing_pipeline_path, 'w') as f:
            f.write(template.render({ "node_list": node_list}))

        # template = env_node.get_template(f'landing_pipeline_move.py')     
        # with open(update_latest_landing_pipeline_path, 'w') as f:
        #     f.write(template.render({ "node_list": node_list}))


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
    data_node = DataNode(token)
    integration_folder_id = data_node.get_directory_id(parent_dir_id=project_folder_id, name=integration_folder_name)
    
    for table_name, config in integration_table.items():  
        catalog_name = f'integration___{table_name}'
        # integraton_file_id = create_file_if_not_exist(
        #     file_name=catalog_name, 
        #     directory_id=integration_folder_id,
        #     data_node=data_node
        # )
        data = {
            'catalog_name': catalog_name,
            'file_name': table_name,            
            'folder_id': integration_folder_id,
            # 'file_id': integraton_file_id,
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
