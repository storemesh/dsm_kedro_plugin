"""``SequentialRunner`` is an ``AbstractRunner`` implementation. It can be
used to run the ``Pipeline`` in a sequential manner using a topological sort
of provided nodes.
"""
import sys
import os
sys.path.append(os.getcwd())

from collections import Counter
from itertools import chain

from pluggy import PluginManager

from kedro.io import AbstractDataSet, DataCatalog, MemoryDataSet
from kedro.pipeline import Pipeline
from kedro.runner.runner import AbstractRunner, run_node

import requests
import git
import inspect
import traceback
from datetime import datetime
import uuid
import json
import pandas as pd
import time
import os
import dask.dataframe as dd
from dsmlibrary.datanode import DataNode

from etl_pipeline.pipeline_registry import register_pipelines
from src.dsm_kedro_plugin.generate_datanode.utils.utils import get_token
from src.dsm_kedro_plugin.custom_dataset.validation.validation_rules import rules
from src.dsm_kedro_plugin.generate_datanode.generate_setting import PIPELINE_PROJECT_PATH, KEDRO_PROJECT_BASE 
from src.config.project_setting import PROJECT_FOLDER_ID, PROJECT_NAME, DATAPLATFORM_API_URI, OBJECT_STORAGE_URI

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
        

class DsmRunner(AbstractRunner):
    """``SequentialRunner`` is an ``AbstractRunner`` implementation. It can
    be used to run the ``Pipeline`` in a sequential manner using a
    topological sort of provided nodes.
    """

    def __init__(self, is_async: bool = False):
        """Instantiates the runner classs.

        Args:
            is_async: If True, the node inputs and outputs are loaded and saved
                asynchronously with threads. Defaults to False.

        """
        super().__init__(is_async=is_async)


    def create_default_data_set(self, ds_name: str) -> AbstractDataSet:
        """Factory method for creating the default data set for the runner.

        Args:
            ds_name: Name of the missing data set

        Returns:
            An instance of an implementation of AbstractDataSet to be used
            for all unregistered data sets.

        """
        return MemoryDataSet()


                

    def _read_monad_logs(
        self, 
        df_val_types,
        type,
        datanode_detail,
        dataset_name,
        start_run_all,
        validation_log_dir,
        datanode,
    ):
        file_id = datanode_detail[dataset_name]['file_id']
        folder_id = datanode_detail[dataset_name]['meta']['folder_id']
        file_name = datanode_detail[dataset_name]['meta']['file_name']
        
        log_filename = f'{folder_id}_{file_name}_{type}.csv'
        log_path = os.path.join(validation_log_dir, log_filename)
        all_record_path = os.path.join(validation_log_dir, f'{folder_id}_{file_name}_{type}_all_record.json')
                
        log_folder_id = datanode.get_directory_id(parent_dir_id=PROJECT_FOLDER_ID, name="Logs")
        
                # 
                # url_path = f"{LOG_FOLDER}/{file_id}.parquet"
                
                # import pdb;pdb.set_trace()
                
                # ddf_log = datanode.read_ddf(file_id=log_file_id)

        if os.path.exists(log_path):
            ddf_log = dd.read_csv(log_path)
            
            
            ddf_merge = ddf_log.merge(df_val_types, on='rule_name')
            
            df_type_count = ddf_merge.groupby(['rule_type'])['pk'].nunique().compute()
            count_format = df_type_count['format'] if 'format' in df_type_count else 0
            count_consistency = df_type_count['consistency'] if 'consistency' in df_type_count else 0
            count_completeness = df_type_count['completeness'] if 'completeness' in df_type_count  else 0
            
            with open(all_record_path) as data_file:
                all_record = json.load(data_file)['all_record']
                
            # write log file
            datanode.upload_file(
                directory_id=log_folder_id, 
                file_path=log_path, 
                description=f"log file of '{file_name}' ({file_id})", 
                lineage=[file_id]
            )
            # print(log_filename)
            # import pdb; pdb.set_trace()
            time.sleep(1)
            log_file_id = datanode.get_file_id(name=log_filename, directory_id=log_folder_id)
            # datanode.write(df=ddf_merge, directory=log_folder_id, name=f'{folder_id}_{file_name}_{type}', replace=True, lineage=)
                
            

            # monad output
            return {
                'file_id': datanode_detail[dataset_name]['file_id'],
                'name': dataset_name,
                'type': type,
                'run_datetime': start_run_all,
                'n_error_format': count_format,
                'n_error_consistency': count_consistency,
                'n_error_completeness': count_completeness,
                'all_record': all_record,
                'logs_file_id': log_file_id,
            }
            
        else:
            return None

        

        # return count_format, count_consistency, count_completeness, all_record

        


    def _run(
        self,
        pipeline: Pipeline,
        catalog: DataCatalog,
        hook_manager: PluginManager,
        session_id: str = None,
    ) -> None:
        """The method implementing sequential pipeline running.

        Args:
            pipeline: The ``Pipeline`` to run.
            catalog: The ``DataCatalog`` from which to fetch data.
            hook_manager: The ``PluginManager`` to activate hooks.
            session_id: The id of the session.

        Raises:
            Exception: in case of any downstream node failure.
        """
        token = get_token()
        datanode = DataNode(
            token,
            dataplatform_api_uri=DATAPLATFORM_API_URI,
            object_storage_uri=OBJECT_STORAGE_URI,
        )
        log_folder_id = datanode.get_directory_id(parent_dir_id=PROJECT_FOLDER_ID, name="Logs")
        validation_log_dir = 'logs/validation_logs/'
        
        
        val_types = [ { 'rule_name': value['func'].name, 'rule_type': value['type'] } for key, value in rules.items() ]
        df_val_types = pd.DataFrame(val_types)
        
        start_run_all = datetime.now()
        is_run_all_success = True
        
        nodes = pipeline.nodes
        done_nodes = set()
        
        
        pipeline_detail = register_pipelines()
        
        # import pdb; pdb.set_trace()
        output_dataset_name = pipeline.all_outputs()
        # pipeline_detail['payment_integration'].all_outputs()
        # output_dataset_names
        # import pdb; pdb.set_trace()
        pipeline_name = None
        for key, value in pipeline_detail.items():
            
            if output_dataset_name == value.all_outputs():
                pipeline_name = key
                break
        
        if pipeline_name == None:
            raise Exception("To generate log, you cannot run sub-pipeline or specific node. Please remove --node param")
        
                
        ## pipeline logs
        repo = git.Repo(search_parent_directories=True)
        branch = repo.active_branch
        current_branch_name = branch.name
        # hexsha = repo.head.object.hexsha        
        
        
        # import pdb;pdb.set_trace()
        pipeline_path = os.path.join(PIPELINE_PROJECT_PATH, 'pipelines/', pipeline_name)
        commits = list(parse_commit_log(repo, pipeline_path))
        # import pdb;pdb.set_trace()
        repo_path = repo.remotes.origin.url.split('.git')[0]
        last_commit_url = os.path.join(repo_path, '-/commit/', commits[0]['commit'])
        
        last_editor = commits[0]['Author']   
        
        ## get project detail
        project_name = PROJECT_NAME
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
               
        
        load_counts = Counter(chain.from_iterable(n.inputs for n in nodes))
        
        func_log_list = {}

        
        for exec_index, node in enumerate(nodes):
            start_time = datetime.now()
            
            is_success = True
            error_log = None
            try:
                run_node(node, catalog, hook_manager, self._is_async, session_id)
                done_nodes.add(node)
            except Exception:
                is_success = False
                is_run_all_success = False
                self._suggest_resume_scenario(pipeline, done_nodes, catalog)
                
#                 print('-------------------------------')
                error_log = traceback.format_exc()
                # import pdb;pdb.set_trace()
                # print(self._suggest_resume_scenario(pipeline, done_nodes, catalog))
                raise
                
            ##### function log
            # import pdb;pdb.set_trace()
            end_time = datetime.now()
            delta = end_time - start_time
            print('Difference is:', delta)

            
            func_log_list[node.name] = {
                "func_id": node.name,
                "name": node.name,
                "type": 'Dask',
                "start": start_time,
                "end": end_time,
                "duration": str(delta),
                "is_success": is_success,
                "log_error": error_log,
            }
            #####
            
            

            # decrement load counts and release any data sets we've finished with
            for data_set in node.inputs:
                load_counts[data_set] -= 1
                if load_counts[data_set] < 1 and data_set not in pipeline.inputs():
                    catalog.release(data_set)
            for data_set in node.outputs:
                if load_counts[data_set] < 1 and data_set not in pipeline.outputs():
                    catalog.release(data_set)

            self._logger.info(
                "Completed %d out of %d tasks", exec_index + 1, len(nodes)
            )
            
        print("------------------SFinish run all node!!-----------------")

        print("Write logs")

        ## data nodes detail        
        dataset_name_list = list(pipeline.data_sets())        
        datanode_detail = {}
        
        for dataset_name in dataset_name_list:
            _, dataset_meta = catalog.load(dataset_name)
            
            datanode_detail[dataset_name] = {
                'id': dataset_name,
                'file_id': dataset_meta['file_id'],
                'meta': dataset_meta,
            }
            
            
        ## function detail, input_edges, output_edges, monad input and monad output
        functions_detail = {}        
        monad_log_list = {}
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

                monad_log_list[edge_id] = self._read_monad_logs(
                    df_val_types=df_val_types,
                    type='read',
                    datanode_detail=datanode_detail,
                    dataset_name=dataset_name,
                    start_run_all=start_run_all,
                    validation_log_dir=validation_log_dir,
                    datanode=datanode,
                )

                # file_id = datanode_detail[dataset_name]['file_id']
                # folder_id = datanode_detail[dataset_name]['meta']['folder_id']
                # file_name = datanode_detail[dataset_name]['meta']['file_name']
                
                
                # log_path = os.path.join(validation_log_dir, f'{folder_id}_{file_name}_read.csv')
                # all_record_path = os.path.join(validation_log_dir, f'{folder_id}_{file_name}_read_all_record.json')
                
                # url_path = f"{LOG_FOLDER}/{datanode_detail[dataset_name]['file_id']}.parquet"
                
                
                # # monad input
                # monad_log_list[edge_id] = {
                #     'file_id': datanode_detail[dataset_name]['file_id'],
                #     'name': dataset_name,
                #     'type': 'read',
                #     'run_datetime': start_run_all,
                #     'format_summary': None,
                #     'consistency_summary': None,
                #     'completeness_summary': None,
                #     'log_url': url_path,
                # }
            
            ## output_edges & monad output
            for dataset_name in list(node.outputs):
                edge_id = f'{node.name}_____{dataset_name}'
                
                # output_edges
                output_edges.append({
                    'edge_id': edge_id,
                    'source': node.name,
                    'target': dataset_name,
                })

                monad_log_list[edge_id] = self._read_monad_logs(
                    df_val_types=df_val_types,
                    type='write',
                    datanode_detail=datanode_detail,
                    dataset_name=dataset_name,
                    start_run_all=start_run_all,
                    validation_log_dir=validation_log_dir,
                    datanode=datanode
                )
                
                # file_id = datanode_detail[dataset_name]['file_id']
                # folder_id = datanode_detail[dataset_name]['meta']['folder_id']
                # file_name = datanode_detail[dataset_name]['meta']['file_name']
                
                
                # log_path = os.path.join(validation_log_dir, f'{folder_id}_{file_name}_write.csv')
                # all_record_path = os.path.join(validation_log_dir, f'{folder_id}_{file_name}_write_all_record.json')
                
                
                # # log_file_id = datanode.get_file_id(name=f"{file_id}_write.parquet", directory_id=LOG_FOLDER)
                # url_path = f"{LOG_FOLDER}/{file_id}.parquet"
                
                # # import pdb;pdb.set_trace()
                
                # # ddf_log = datanode.read_ddf(file_id=log_file_id)

                # if os.path.exists(log_path):
                #     count_format, count_consistency, count_completeness, all_record = self._read_monad_logs(
                #         log_path=log_path, 
                #         all_record_path=all_record_path, 
                #         df_val_types=df_val_types,
                #         type='write'
                #         datanode_detail=datanode_detail,
                #         dataset_name=dataset_name,
                #         start_run_all=start_run_all,
                #         edge_id=edge_id,
                #     )

                #     # monad output
                #     monad_log_list[edge_id] = {
                #         'file_id': datanode_detail[dataset_name]['file_id'],
                #         'name': dataset_name,
                #         'type': 'write',
                #         'run_datetime': start_run_all,
                #         'n_error_format': count_format,
                #         'n_error_consistency': count_consistency,
                #         'n_error_completeness': count_completeness,
                #         'all_record': all_record,
                #     }
                    
                # else:
                #     monad_log_list[edge_id] = None
                
        
        output_dict = {
            "pipeline": {
                "name": pipeline_name,
                "project": project_name,
                "git": {
                    "commits": commits,
                    "current_branch_name": current_branch_name,
                    "last_commit_url": last_commit_url,
                }
            },
            "datanodes": datanode_detail,
            "functions": functions_detail,
            "input_edges": input_edges,
            "output_edges": output_edges,
            "monad_log": monad_log_list,
            "function_log": func_log_list,
        }
        # print(output_dict)
        print(json.dumps(output_dict, indent=4, default=str))
        
        print('------------')
        end_run_all = datetime.now()
        
        result_status = "SUCESS" if is_run_all_success else "FAILED"
        run_all_result = {
            'uuid': str(uuid.uuid4()),
            'pipeline': pipeline_id,
            'start_time': start_run_all,
            'end_time': end_run_all,
            # 'duration': str(end_run_all - start_run_all),
            'status': result_status,
            'result': output_dict,
            'last_editor': last_editor,
        }
        # print(json.dumps(run_all_result, indent=4, default=str))
        
        json_str = json.dumps(run_all_result, indent=4, default=str)
        json_data = json.loads(json_str)
        # print(json_data)
        
        res = requests.post(url, json=json_data, headers=headers)

        print('ddd')
        
        
        # {
        #     "uuid": "xxxyyyzzz1",
        #     "start_time": "2022-10-01 12:00:01",
        #     "end_time": "2022-10-01 13:00:01",
        #     "status": "SUCESS",
        #     "result": {
        #         "id": 1
        #     },
        #     "last_editor": "sothanav",
        #     "pipeline": 1
        # }
