import yaml
import os
from utils.utils import get_token
from config.config_source_table import source_table, sql_datanode_folder_id, landing_folder_id
from config.project_setting import PROJECT_FOLDER_ID
from utils.generate_catalog import generate_sql_datanode
from generate_setting import GENERATE_CATALOG_PATH

token = get_token()

WRITE_MODE = True

sql_datanode_folder_name = "SQLDataNode"

if __name__ == "__main__":    
    if not os.path.exists(GENERATE_CATALOG_PATH):
        os.makedirs(generate_catalog_path, exist_ok=True)

    generate_sql_datanode(
        source_table=source_table, 
        project_folder_id=PROJECT_FOLDER_ID, 
        sql_datanode_folder_name=folder_name, 
        token=token, 
        write_mode=WRITE_MODE
    )