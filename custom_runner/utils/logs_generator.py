import git
import uuid
import json
import os
import requests
from datetime import datetime
from dsmlibrary.datanode import DataNode

from src.etl_pipeline.pipeline_registry import register_pipelines
from src.dsm_kedro_plugin.generate_datanode.utils.utils import get_token
from src.dsm_kedro_plugin.custom_dataset.validation.validation_rules import rules
from src.dsm_kedro_plugin.generate_datanode.generate_setting import PIPELINE_PROJECT_PATH, KEDRO_PROJECT_BASE 
from src.config.project_setting import PROJECT_FOLDER_ID, PROJECT_NAME, DATAPLATFORM_API_URI, OBJECT_STORAGE_URI

start_pipeline_folder = 'logs/validation_logs/start'

def gen_log_start(pipeline_name):
    '''
        parameter 
            pipeline_name
        input
            token
            log_folder_id
            validation_log_dir
            df_val_types

        write  logs/validation_logs/start/<PIPELINE_NAME>.json
        send post to discovery backend
            - start_time
            - uuid
            - pipeline_id
            - status ('RUNNING')
    '''

    project_name = PROJECT_NAME

    # get token
    datanode, token = get_dsm_datanode()

    # get project_id, pipeline_id
    project_id, pipeline_id, run_pipeline_url, headers = get_pipeline_id(project_name, pipeline_name, token)

    # get git detail
    git_detail = get_git_detail(pipeline_name)

    # prepare data
    start_run_all = datetime.now()
    result_status = "RUNNING"

    output_dict = {
        "pipeline": {
            "name": pipeline_name,
            "project": project_name,
            "git": git_detail,
        },
    }

    run_all_result = {
        'uuid': str(uuid.uuid4()),
        'pipeline': pipeline_id,
        'start_time': start_run_all,
        'end_time': start_run_all,
        'status': result_status,
        'result': output_dict,
        'last_editor': git_detail['last_editor'],
    }
    json_str = json.dumps(run_all_result, indent=4, default=str)
    json_data = json.loads(json_str)


    ### write to file
    start_pipeline_path = os.path.join(start_pipeline_folder, pipeline_name)
    # gen logs folder
    if not os.path.exists(start_pipeline_folder):
        os.makedirs(start_pipeline_path, exist_ok=True)

    with open(start_pipeline_path, 'w') as f:
        json.dump(json_data, f)

    ### post to server
    res = requests.post(run_pipeline_url, json=json_data, headers=headers)
    print(res)
    

def gen_log_finish():
    '''
        parameter 
            pipeline_name
        
    '''
    
    log_folder_id = datanode.get_directory_id(parent_dir_id=PROJECT_FOLDER_ID, name="Logs")
    validation_log_dir = 'logs/validation_logs/'
    
    
    val_types = [ { 'rule_name': value['func'].name, 'rule_type': value['type'] } for key, value in rules.items() ]
    df_val_types = pd.DataFrame(val_types)
    
    start_run_all = datetime.now()
    is_run_all_success = True

def get_pipeline_id(project_name, pipeline_name, token):
    ## get project detail    
    base_url = "https://api.discovery.dev.data.storemesh.com/api"
    url = f"{base_url}/logs/run-pipeline/"
    headers = {'Authorization': f'Bearer {token}'}


    res = requests.get(f'{base_url}/logs/project/?search={project_name}', headers=headers)
    project_list = res.json()
    
    try:
        project_id = [ item['id'] for item in project_list if item['name'] == project_name ][0]
    except:
        raise Exception(f"Your PROJECT_NAME ('{project_name}') in 'src/config/project_setting.py' is not match with any project name in Data Discovery")
        

    ## get pipeline detail
    res = requests.get(f'{base_url}/logs/pipeline/?search={pipeline_name}&project={project_id}', headers=headers)
    pipeline_list = res.json()
    try:
        pipeline_id = [ item['id'] for item in pipeline_list if item['name'] == pipeline_name ][0]
    except:
        raise Exception(f"Your pipeline_name('{pipeline_name}') in 'src/etl_pipeline/pipeline_registry.py' is not match with any pipeline name in Data Discovery")
    
    return project_id, pipeline_id, url, headers


def get_pipeline_name(pipeline):
    pipeline_detail = register_pipelines()
    output_dataset_name = pipeline.all_outputs()
    pipeline_name = None
    for key, value in pipeline_detail.items():
        
        if output_dataset_name == value.all_outputs():
            pipeline_name = key
            break
    
    if pipeline_name == None:
        raise Exception("To generate log, you cannot run sub-pipeline or specific node. Please remove --node param")

    return pipeline_name    

def get_dsm_datanode():
    token = get_token()
    datanode = DataNode(
        token,
        dataplatform_api_uri=DATAPLATFORM_API_URI,
        object_storage_uri=OBJECT_STORAGE_URI,
    )

    return datanode, token

def get_git_detail(pipeline_name):
    ## pipeline logs
    repo = git.Repo(search_parent_directories=True)
    branch = repo.active_branch
    current_branch_name = branch.name

    pipeline_path = os.path.join(PIPELINE_PROJECT_PATH, 'pipelines/', pipeline_name)
    commits = list(parse_commit_log(repo, pipeline_path))
    
    repo_path = repo.remotes.origin.url.split('.git')[0]
    last_commit_url = os.path.join(repo_path, '-/commit/', commits[0]['commit'])
    
    last_editor = commits[0]['Author']   

    git_detail = {
        "commits": commits,
        "current_branch_name": current_branch_name,
        "last_commit_url": last_commit_url,
        "last_editor": last_editor,
    }
    return git_detail

def parse_commit_log(repo, *params):
    commit = {}
    try:
        log = repo.git.log(*params).split("\n")
    except git.GitCommandError:
        return

    for line in log:
        if line.startswith("    "):
            if not 'message' in commit:
                commit['message'] = ""
            else:
                commit['message'] += "\n"
            commit['message'] += line[4:]
        elif line:
            if 'message' in commit:
                yield commit
                commit = {}
            else:
                field, value = line.split(None, 1)
                commit[field.strip(":")] = value
    if commit:
        yield commit