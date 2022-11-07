import yaml
import sys
import os
sys.path.append(os.getcwd())

from src.dsm_kedro_plugin.generate_datanode.utils.utils import get_token
from src.dsm_kedro_plugin.generate_datanode.utils.generate_catalog import generate_sql_datanode
from src.dsm_kedro_plugin.generate_datanode.generate_setting import GENERATE_CATALOG_PATH
from src.config.config_source_table import source_table
from src.config.project_setting import PROJECT_FOLDER_ID


token = get_token()

WRITE_MODE = True

sql_datanode_folder_name = "SQLDataNode"

if __name__ == "__main__":    
    if not os.path.exists(GENERATE_CATALOG_PATH):
        os.makedirs(generate_catalog_path, exist_ok=True)

    generate_sql_datanode(
        source_table=source_table, 
        project_folder_id=PROJECT_FOLDER_ID, 
        sql_datanode_folder_name=sql_datanode_folder_name, 
        token=token, 
        write_mode=WRITE_MODE
    )