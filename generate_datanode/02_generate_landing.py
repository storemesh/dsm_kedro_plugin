import yaml
import os
from utils.utils import get_token, get_env_var
from config.config_source_table import source_table #, sql_datanode_folder_id, landing_folder_id
from config.project_setting import PROJECT_FOLDER_ID
from utils.generate_catalog import generate_landing_pipeline
from generate_setting import QUERY_LANDING_PIPELINE_PATH

token = get_token()

WRITE_MODE = True

sql_datanode_folder_name = "Landing"
landing_folder_name = "Landing"

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
        token=token, 
        write_mode=WRITE_MODE,
        overwrite_exist_node=overwrite_exist_node,
    )