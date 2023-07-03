import sys
import os
sys.path.append(os.getcwd())

import pandas as pd
import yaml
from pathlib import Path
import dask.dataframe as dd
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

# integration file
with open('conf/base/catalogs/manual/integration.yml') as f:
    integration_dict = yaml.safe_load(f)
    
    
# kedro load catalog 
project_path = Path.cwd()
bootstrap_project(project_path)
session = KedroSession.create(project_path)
catalog = session.load_context().catalog

# loop all catalog
result_list = []
for key, value in integration_dict.items():
    ddf, meta = catalog.load(key)
    
    stat = meta.get('meta_discovery').get('context', {}).get('statistics', {}).get('after_validate_stat', {})
    result_list.append({
        'last_modified': meta.get('meta_discovery').get('last_modified'),
        # 'source_app': , 
        # 'source_table': ,
        'file_id': meta.get('file_id'),
        'file_name': meta.get('file_name'),
        'all_record': stat.get('all_record'),
        'all_null_value': stat.get('all_null_value'),
    })    
    

df = pd.DataFrame(result_list)
df = df.drop_duplicates('file_name')
ddf = dd.from_pandas(df, npartitions=1)

# save result to report folder
catalog.save('report.integration_report', (ddf, []))