import yaml
import sys
import os
sys.path.append(os.getcwd())

from src.dsm_kedro_plugin.generate_datanode.utils.utils import get_token
from src.dsm_kedro_plugin.generate_datanode.utils.generate_catalog import generate_landing_pipeline
from src.dsm_kedro_plugin.generate_datanode.generate_setting import QUERY_LANDING_PIPELINE_PATH
from src.config.config_source_table import source_table
from src.config.project_setting import PROJECT_FOLDER_ID


credentials = get_token()

WRITE_MODE = True

sql_datanode_folder_name = "Landing"
landing_folder_name = "Landing"
load_only_updated = False

if __name__ == "__main__":
    if not os.path.exists(QUERY_LANDING_PIPELINE_PATH):
        os.makedirs(QUERY_LANDING_PIPELINE_PATH, exist_ok=True)
    
    overwrite_exist_node = False
    generate_landing_pipeline(
        source_table=source_table, 
        project_folder_id=PROJECT_FOLDER_ID, 
        sql_datanode_folder_name=sql_datanode_folder_name,
        landing_folder_name=landing_folder_name,
        query_landing_pipeline_path=QUERY_LANDING_PIPELINE_PATH, 
        write_mode=WRITE_MODE,
        overwrite_exist_node=overwrite_exist_node,
        load_only_updated=load_only_updated,
        credentials=credentials,
    )
    print('\ngenerate landing is successful!!')