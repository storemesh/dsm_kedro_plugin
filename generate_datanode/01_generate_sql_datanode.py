import yaml
import os
from utils.utils import get_token
from config.config_source_table import source_table, sql_datanode_folder_id, landing_folder_id
from utils.generate_catalog import generate_sql_datanode
from config.config_project_path import KEDRO_PROJECT_BASE

token = get_token()

WRITE_MODE = True

if __name__ == "__main__":
    generate_catalog_path = os.path.join(KEDRO_PROJECT_BASE, 'conf/base/catalogs/generated')

    if not os.path.exists(generate_catalog_path):
        os.makedirs(generate_catalog_path, exist_ok=True)

    generate_sql_datanode(source_table, sql_datanode_folder_id, token=token, write_mode=WRITE_MODE)