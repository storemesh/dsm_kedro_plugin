import yaml
from utils.utils import get_token
from config.config_integration_table import integration_table
from config.project_setting import PROJECT_FOLDER_ID
from utils.generate_catalog import generate_integration_catalogs


token = get_token()

WRITE_MODE = True
APPEND_MODE = False

integration_folder_name = "Integration"

if __name__ == "__main__":
    generate_integration_catalogs(
        integration_table=integration_table, 
        project_folder_id=PROJECT_FOLDER_ID, 
        integration_folder_name=integration_folder_name, 
        token=token, 
        append=APPEND_MODE
    )