import sys
import os
sys.path.append(os.getcwd())

import pandas as pd
import yaml
from pathlib import Path
import dask.dataframe as dd
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from dsmlibrary.datanode import DataNode, DatabaseManagement
from src.config.config_source_table import source_table
from src.config.project_setting import DATAPLATFORM_API_URI, OBJECT_STORAGE_URI, OBJECT_STORAGE_SECUE

from src.dsm_kedro_plugin.generate_datanode.utils.utils import get_token



######## Get Database Description ########
credentials = get_token()
database = DatabaseManagement(
        dataplatform_api_uri=DATAPLATFORM_API_URI,
        object_storage_uri=OBJECT_STORAGE_URI,
        object_storage_secue= OBJECT_STORAGE_SECUE,
        **credentials,
    )

database_list = []
for database_id, table_list in source_table.items():    
    # exec to define class object                 
    database_meta, schema = database.get_database_schema(database_id=database_id)

    database_meta['name'] = database_meta['name'].replace(' ', '_')
    database_list.append(database_meta)

df_database = pd.DataFrame(database_list)[['name', 'description']]
df_database = df_database.rename(columns={'name': 'source_app', 'description': 'source_app_description'})



######## Get Landing Statistic ########
# integration file
with open('conf/base/catalogs/generated/catalog_02_landing.yml') as f:
    integration_dict = yaml.safe_load(f)
    
    
# kedro load catalog 
project_path = Path.cwd()
bootstrap_project(project_path)
session = KedroSession.create(project_path)
catalog = session.load_context().catalog

# loop all catalog
result_list = []
for key, value in integration_dict.items():    
    dataset_type = value.get('type').split('.')[-1]
    try:
        # if dataset_type == 'DsmDataNode': 
        ddf, meta = catalog.load(key)
        # stat = meta.get('meta_discovery').get('context', {}).get('statistics', {}).get('after_validate_stat', {}) # no statistic in landing yet
        
        dataset_name = key.split('.')[-1]
        source_app = dataset_name.split('_')[0]
        source_table = dataset_name[len(source_app)+1:]
        result_list.append({
            'last_modified': meta.get('meta_discovery').get('last_modified'),
            'source_app': source_app, 
            'source_table': source_table,
            'file_id': meta.get('file_id'),
            'file_name': meta.get('file_name'),
            'all_record': len(ddf),
            # 'all_null_value': stat.get('all_null_value'), # in progress
        })        
    except Exception as e:
        print('Error: ', e)
    

df = pd.DataFrame(result_list)
df = df.merge(df_database, on='source_app', how='left')
df = df[[ 'last_modified', 'source_app', 'source_app_description', 'source_table', 'file_id', 'file_name', 'all_record']]
df = df.drop_duplicates('file_name')
ddf = dd.from_pandas(df, npartitions=1)

# save result to report folder
catalog.save('report.landing_report', (ddf, []))