import yaml
from utils.utils import get_token
from config.config_integration_table import integration_table, integration_folder_id
from utils.generate_catalog import generate_integration_catalogs


token = get_token()

WRITE_MODE = True
APPEND_MODE = False

if __name__ == "__main__":
    generate_integration_catalogs(integration_table, integration_folder_id, token, append=APPEND_MODE)