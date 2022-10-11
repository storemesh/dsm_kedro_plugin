import yaml
import os
from utils.utils import get_token, get_env_var
from config.config_source_table import source_table, sql_datanode_folder_id, landing_folder_id
from utils.generate_catalog import generate_landing_pipeline
from config.config_project_path import KEDRO_PROJECT_BASE, KEDRO_PROJECT_NAME

token = get_token()

WRITE_MODE = True

if __name__ == "__main__":
    project_path = os.path.join(KEDRO_PROJECT_BASE, 'src', KEDRO_PROJECT_NAME)

    generate_change_landing_path = os.path.join(project_path, 'pipelines/generate_change_landing')
    update_latest_landing_path = os.path.join(project_path, 'pipelines/update_latest_landing')

    if not os.path.exists(generate_change_landing_path):
        os.makedirs(generate_change_landing_path, exist_ok=True)

    if not os.path.exists(update_latest_landing_path):
        os.makedirs(update_latest_landing_path, exist_ok=True)
    
    overwrite_exist_node = False
    generate_landing_pipeline(
        source_table=source_table, 
        sql_datanode_folder_id=sql_datanode_folder_id, 
        landing_folder_id=landing_folder_id, 
        project_path=project_path, 
        token=token, 
        write_mode=WRITE_MODE,
        overwrite_exist_node=overwrite_exist_node,
        # generate_source_dict={
        #     1: ['Dim_Zone']
        # },
    )