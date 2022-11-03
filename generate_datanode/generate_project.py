from dsmlibrary.datanode import DataNode

# 

required_folder_list = ["Landing", "Staging", "Integration", "Logs"]

optional_folder_list = ["Factable", "Master"]


project_folder_id = 10

datanode = DataNode(token)

for folder_name in required_folder_list:
    # create folder 
    try:
        datanode.createDirectory(self, directory_id=project_folder_id, name=folder_name, description=None)
    except Exception as e:
        print(f'Exception: {e})
              
        # get_directory_id(self, parent_dir_id=project_folder_id, name="Landing")