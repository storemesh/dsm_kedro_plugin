import sys
import os
sys.path.append(os.getcwd())

from dsmlibrary.datanode import DataNode
from src.config.project_setting import REQUIRED_FOLDER_LIST, PROJECT_FOLDER_ID, DATAPLATFORM_API_URI, OBJECT_STORAGE_URI, OBJECT_STORAGE_SECUE
from src.dsm_kedro_plugin.generate_datanode.utils.utils import get_token

token = get_token()
datanode = DataNode(
            token, 
            dataplatform_api_uri=DATAPLATFORM_API_URI,
            object_storage_uri=OBJECT_STORAGE_URI,
            object_storage_secue=OBJECT_STORAGE_SECUE,
        )

for folder_name in REQUIRED_FOLDER_LIST:
    # create folder 
    try:
        datanode.createDirectory(directory_id=PROJECT_FOLDER_ID, name=folder_name, description=folder_name)
    except Exception as e:
        print(f'Exception: {e}')
              
        # get_directory_id(self, parent_dir_id=project_folder_id, name="Landing")