import git
import uuid
import json
import os
import requests
from datetime import datetime
from pathlib import Path
import pandas as pd
import inspect
import time
import dask.dataframe as dd
from dsmlibrary.datanode import DataNode

from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

from src.etl_pipeline.pipeline_registry import register_pipelines
from src.dsm_kedro_plugin.generate_datanode.utils.utils import get_token

from src.dsm_kedro_plugin.generate_datanode.generate_setting import PIPELINE_PROJECT_PATH, KEDRO_PROJECT_BASE 
from src.config.project_setting import PROJECT_FOLDER_ID, PROJECT_NAME, DATAPLATFORM_API_URI, OBJECT_STORAGE_URI, OBJECT_STORAGE_SECUE
from src.config.validation_rules import rules

start_pipeline_folder = 'logs/validation_logs/start'
validation_log_dir = 'logs/validation_logs/'

base_url = os.path.join(DATAPLATFORM_API_URI, 'api') #"https://api.discovery.dev.data.storemesh.com/api"
run_pipeline_url = f"{base_url}/logs/run-pipeline/"
file_meta_url = f"{base_url}/v2/file"

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
    project_id, pipeline_id = get_pipeline_id(project_name, pipeline_name, token)

    headers = _get_header(token)

    # get git detail
    git_detail = get_git_detail(pipeline_name)

    # prepare data
    start_run_all = datetime.utcnow()
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
        'project_id': project_id,
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
    start_pipeline_path = os.path.join(start_pipeline_folder, f'{pipeline_name}.json')
    # gen logs folder
    if not os.path.exists(start_pipeline_folder):
        os.makedirs(start_pipeline_folder, exist_ok=True)

    with open(start_pipeline_path, 'w') as f:
        json.dump(json_data, f)
        
    print(json_data)
    ### post to server
    res = requests.post(run_pipeline_url, json=json_data, headers=headers)
    print(res)
    if res.status_code > 201:
        print(res.json())
        raise Exception('cannot send start logs to server')
    

def gen_log_finish(pipeline_name):
    '''
        parameter 
            pipeline_name
        
    '''
    project_path = Path.cwd()
    bootstrap_project(project_path)

    session = KedroSession.create(project_path)
    catalog = session.load_context().catalog

    pipeline_detail = register_pipelines()
    pipeline = pipeline_detail[pipeline_name]

    # get token
    datanode, token = get_dsm_datanode()

    headers = _get_header(token)

    # load start data
    start_pipeline_path = os.path.join(start_pipeline_folder, f'{pipeline_name}.json')
    with open(start_pipeline_path) as f:
        start_data = json.load(f)

    # load function data
    save_func_path = os.path.join(validation_log_dir, f'func_result_{pipeline_name}.json')
    with open(save_func_path) as f:
        run_function_result = json.load(f)


    val_types = []
    for key, value in rules.items():
        if value['func'].error:
            name_error_func = value['func'].error
        else:
            name_error_func = value['func'].name
        val_types.append({ 
            'rule_name': name_error_func, 
            'rule_type': value['type'] 
        })

    val_types.append({ 'rule_name': 'not_nullable', 'rule_type': 'completeness'})
    df_val_types = pd.DataFrame(val_types)

    ## data nodes detail        
    dataset_name_list = list(pipeline.data_sets())      
    dataset_output_list = list(pipeline.all_outputs())  
    datanode_detail = {}
    
    for dataset_name in dataset_name_list:
        try:
            dataset_meta = catalog.exists(dataset_name)        
            datanode_detail[dataset_name] = {
                'id': dataset_name,
                'file_id': dataset_meta['file_id'],
                'meta': dataset_meta,
            }
            
            # change context in meta data
            if dataset_name in dataset_output_list:
                res = requests.get(f"{file_meta_url}/{dataset_meta['file_id']}", headers=headers)
                context = res.json()['context']
                context.update({
                    "pipeline_log": { 
                        "run_pipeline_id": start_data['uuid'],
                        "run_pipeline_link": f"https://logs.discovery.dev.data.storemesh.com/project/{start_data['project_id']}/pipeline/run-pipeline/{start_data['uuid']}",
                    },
                })
                json_data = { "context": context }
                res = requests.patch(f"{file_meta_url}/{dataset_meta['file_id']}/", json=json_data, headers=headers)
                print(res)
                
        except Exception as e:
            print('Exception :', e)
            datanode_detail[dataset_name] = {
                'id': dataset_name,
                'file_id': None,
                'meta': {
                    "config": {},
                    "file_id": None,
                    "file_name": None,
                    "folder_id": None
                },
            }
    

    ## function detail, input_edges, output_edges, monad input and monad output
    functions_detail = {}        
    monad_read_list = {}
    monad_write_list = {}
    input_edges = []
    output_edges = []
    
    for node in pipeline.nodes:
        
        ## function detail
        func_obj = node.func
        func_source_code = inspect.getsource(func_obj)
        
        functions_detail[node.name] = {
            'func_id': node.name,
            'func_name': node.name,
            'python_code': func_source_code,
            'input_ids': node.inputs,
            'output_ids': node.outputs,
        }
        
        ## input_edges & monad input
        for dataset_name in list(node.inputs):
            edge_id = f'{dataset_name}_____{node.name}'
            
            # input_edges
            input_edges.append({
                'edge_id': edge_id,
                'source': dataset_name,
                'target': node.name,
            })

            monad_read_list[edge_id] = _read_monad_logs(
                df_val_types=df_val_types,
                type='read',
                datanode_detail=datanode_detail,
                dataset_name=dataset_name,
                start_run_all=start_data['start_time'],
                validation_log_dir=validation_log_dir,
                datanode=datanode,
            )
        
        ## output_edges & monad output
        for dataset_name in list(node.outputs):
            edge_id = f'{node.name}_____{dataset_name}'
            
            # output_edges
            output_edges.append({
                'edge_id': edge_id,
                'source': node.name,
                'target': dataset_name,
            })

            monad_write_list[edge_id] = _read_monad_logs(
                df_val_types=df_val_types,
                type='write',
                datanode_detail=datanode_detail,
                dataset_name=dataset_name,
                start_run_all=start_data['start_time'],
                validation_log_dir=validation_log_dir,
                datanode=datanode
            )
            
    output_dict = {
        "pipeline": start_data['result']['pipeline'],
        "datanodes": datanode_detail,
        "functions": functions_detail,
        "input_edges": input_edges,
        "output_edges": output_edges,
        # "monad_log": monad_log_list,
        "monad_log_read": monad_read_list,
        "monad_log_write": monad_write_list,
        "function_log": run_function_result['function_result'],
    }
    
    print('------------')
    end_run_all = datetime.utcnow()
    
    result_status = run_function_result['status']
    result_data = {
        'status': result_status,
        'end_time': end_run_all,
        'result': output_dict,
    }
    
    json_str = json.dumps(result_data, indent=4, default=str)
    json_data = json.loads(json_str)
    print(json_str)

    patch_url = f"{run_pipeline_url}{start_data['uuid']}/"
    res = requests.patch(patch_url, json=json_data, headers=headers)
    
    print(res)

    if res.status_code > 201:
        print(res.json())
        raise Exception('cannot send end logs to server')
    
def _get_header(token):
    headers = {'Authorization': f'Bearer {token}'}
    return headers

def get_pipeline_id(project_name, pipeline_name, token):
    ## get project detail    
    
    headers = _get_header(token)

    res = requests.get(f'{base_url}/logs/project/?search={project_name}', headers=headers)
    if res.status_code > 201:
        raise Exception('Exception: ', res.json())
    
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
    
    return project_id, pipeline_id


def get_pipeline_name(pipeline):
    pipeline_detail = register_pipelines()
    output_dataset_name = pipeline.all_outputs()
    pipeline_name = None

    for key, value in pipeline_detail.items():
        item_outputs = value.all_outputs()
        if output_dataset_name.intersection(item_outputs) == output_dataset_name:
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
        object_storage_secue=OBJECT_STORAGE_SECUE,
    )

    return datanode, token

def get_git_detail(pipeline_name):
    ## pipeline logs

    try:

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
    except Exception as e:
        print(' Exception:', e)
        git_detail = {
            "commits": [],
            "current_branch_name": "-",
            "last_commit_url": "-",
            "last_editor": "-",
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


def _read_monad_logs(
        df_val_types,
        type,
        datanode_detail,
        dataset_name,
        start_run_all,
        validation_log_dir,
        datanode,
    ):
        if datanode_detail[dataset_name]['meta']['file_name'] == None:
            return None

        file_id = datanode_detail[dataset_name]['file_id']
        folder_id = datanode_detail[dataset_name]['meta']['folder_id']
        file_name = datanode_detail[dataset_name]['meta']['file_name']
        
        log_filename = f'{folder_id}_{file_name}_{type}.parquet'
        log_path = os.path.join(validation_log_dir, log_filename)
        data_statistic_path = os.path.join(validation_log_dir, f'{folder_id}_{file_name}_data_statistic.json')
                
        log_folder_id = datanode.get_directory_id(parent_dir_id=PROJECT_FOLDER_ID, name="Logs")
        
        with open(data_statistic_path) as data_file:
            data_statistic = json.load(data_file)['all_record']
            
        completeness_percent = 1 - ( data_statistic['all_null_value'] / (data_statistic['all_record'] * data_statistic['all_column']) )

        if os.path.exists(log_path):
            ddf_log = dd.read_parquet(log_path)
            
            ddf_merge = ddf_log.merge(df_val_types, on='rule_name')
            
            df_type_count = ddf_merge.groupby(['rule_type'])['pk'].nunique().compute()
            count_format = df_type_count['format'] if 'format' in df_type_count else 0
            count_consistency = df_type_count['consistency'] if 'consistency' in df_type_count else 0
            count_completeness = df_type_count['completeness'] if 'completeness' in df_type_count  else 0 # count from null error logs
            
            number_error = (ddf_merge['is_required'] == True).sum() > 0
            number_warning = (ddf_merge['is_required'] == False).sum() > 0
            if number_error > 0:
                status = 'fail'
            elif number_warning > 0:
                status = 'warning'
            else:
                status = 'success'
            # with open(all_record_path) as data_file:
            #     all_record = json.load(data_file)['all_record']
            
            res = datanode.writeListDataNode(df=ddf_merge, directory_id=log_folder_id, name=log_filename, replace=True)
            listdatanode_file_id = res['file_id']
            log_file_id = datanode.get_file_version(file_id=listdatanode_file_id)[0]['file_id']            

            # monad output
            return {
                'file_id': datanode_detail[dataset_name]['file_id'],
                'name': dataset_name,
                'type': type,
                'data_type': 'Parquet',
                'run_datetime': start_run_all,
                'n_error_format': int(count_format),
                'n_error_consistency': int(count_consistency),
                'n_error_completeness': int(count_completeness),
                'completeness_percent': completeness_percent,
                'all_record': data_statistic['all_record'],
                'logs_file_id': log_file_id,
                'status': status,
            }
            
        else:
            # return None
            return {
                'file_id': datanode_detail[dataset_name]['file_id'],
                'name': dataset_name,
                'type': type,
                'data_type': 'Parquet',
                'run_datetime': start_run_all,
                'n_error_format': 0,
                'n_error_consistency': 0,
                'n_error_completeness': 0,
                'completeness_percent': completeness_percent,
                'all_record': data_statistic['all_record'], # mock number
                'logs_file_id': None,
                'status': 'success',
            }        