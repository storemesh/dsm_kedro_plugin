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

from src.etl_pipeline.pipeline_registry import register_pipelines
from src.dsm_kedro_plugin.generate_datanode.utils.utils import get_token
from src.dsm_kedro_plugin.custom_dataset.validation.validation_rules import rules
from src.dsm_kedro_plugin.generate_datanode.generate_setting import PIPELINE_PROJECT_PATH, KEDRO_PROJECT_BASE 
from src.config.project_setting import PROJECT_FOLDER_ID, PROJECT_NAME, DATAPLATFORM_API_URI, OBJECT_STORAGE_URI
from .utils.logs_generator import parse_commit_log, gen_log_start, gen_log_finish, get_pipeline_name, get_dsm_datanode, validation_log_dir

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


    def _write_function_json(self, path, json_data, status):
        json_result = {
            "function_result": json_data,
            "status": status,
        }
        json_str = json.dumps(json_result, indent=4, default=str)
        json_result = json.loads(json_str)
        with open(path, 'w') as f:
            json.dump(json_result, f)
                

    # def _read_monad_logs(
    #     self, 
    #     df_val_types,
    #     type,
    #     datanode_detail,
    #     dataset_name,
    #     start_run_all,
    #     validation_log_dir,
    #     datanode,
    # ):
    #     file_id = datanode_detail[dataset_name]['file_id']
    #     folder_id = datanode_detail[dataset_name]['meta']['folder_id']
    #     file_name = datanode_detail[dataset_name]['meta']['file_name']
        
    #     log_filename = f'{folder_id}_{file_name}_{type}.csv'
    #     log_path = os.path.join(validation_log_dir, log_filename)
    #     all_record_path = os.path.join(validation_log_dir, f'{folder_id}_{file_name}_{type}_all_record.json')
                
    #     log_folder_id = datanode.get_directory_id(parent_dir_id=PROJECT_FOLDER_ID, name="Logs")

    #     if os.path.exists(log_path):
    #         ddf_log = dd.read_csv(log_path)
            
            
    #         ddf_merge = ddf_log.merge(df_val_types, on='rule_name')
            
    #         df_type_count = ddf_merge.groupby(['rule_type'])['pk'].nunique().compute()
    #         count_format = df_type_count['format'] if 'format' in df_type_count else 0
    #         count_consistency = df_type_count['consistency'] if 'consistency' in df_type_count else 0
    #         count_completeness = df_type_count['completeness'] if 'completeness' in df_type_count  else 0
            
    #         with open(all_record_path) as data_file:
    #             all_record = json.load(data_file)['all_record']
                
    #         # write log file
    #         datanode.upload_file(
    #             directory_id=log_folder_id, 
    #             file_path=log_path, 
    #             description=f"log file of '{file_name}' ({file_id})", 
    #             lineage=[file_id]
    #         )
    #         time.sleep(1)
    #         log_file_id = datanode.get_file_id(name=log_filename, directory_id=log_folder_id)
    #         # datanode.write(df=ddf_merge, directory=log_folder_id, name=f'{folder_id}_{file_name}_{type}', replace=True, lineage=)
                
            

    #         # monad output
    #         return {
    #             'file_id': datanode_detail[dataset_name]['file_id'],
    #             'name': dataset_name,
    #             'type': type,
    #             'run_datetime': start_run_all,
    #             'n_error_format': count_format,
    #             'n_error_consistency': count_consistency,
    #             'n_error_completeness': count_completeness,
    #             'all_record': all_record,
    #             'logs_file_id': log_file_id,
    #         }
            
    #     else:
    #         return None        


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
        # start_run_all = datetime.now()
        # is_run_all_success = True
        
             
        pipeline_name = get_pipeline_name(pipeline=pipeline)            
        gen_log_start(pipeline_name=pipeline_name)

        func_log_list = {}
        datanode, token = get_dsm_datanode()
        log_folder_id = datanode.get_directory_id(parent_dir_id=PROJECT_FOLDER_ID, name="Logs")
        save_func_path = os.path.join(validation_log_dir, f'func_result_{pipeline_name}.json')

        # kedro default
        nodes = pipeline.nodes
        done_nodes = set()
        load_counts = Counter(chain.from_iterable(n.inputs for n in nodes))

        for exec_index, node in enumerate(nodes):
            start_time = datetime.now()
            
            is_success = True
            error_log = "-----"
            try:
                run_node(node, catalog, hook_manager, self._is_async, session_id)
                done_nodes.add(node)
            except Exception:
                is_success = False
                is_run_all_success = False
                self._suggest_resume_scenario(pipeline, done_nodes, catalog)
                error_log = traceback.format_exc()
                
            ##### function log
            # import pdb;pdb.set_trace()
            end_time = datetime.now()
            delta = end_time - start_time
            print('Difference is:', delta)


            log_filename = f'function_{node.name}.txt'
            log_path = os.path.join(validation_log_dir, log_filename)
            with open(log_path, 'w') as f:
                f.write(error_log)

            # import pdb; pdb.set_trace()
            # write log file
            datanode.upload_file(
                directory_id=log_folder_id, 
                file_path=log_path, 
                description=f"log file of '{node.name}'", 
                # lineage=[file_id]
            )
            time.sleep(1)
            log_file_id = datanode.get_file_id(name=log_filename, directory_id=log_folder_id)
            
            func_log_list[node.name] = {
                "func_id": node.name,
                "name": node.name,
                "type": 'Dask',
                "start": start_time,
                "end": end_time,
                "duration": str(delta),
                "is_success": is_success,
                "logs_file_id": log_file_id,
            }
            #####

            if not is_success:
                self._write_function_json(
                    path=save_func_path, 
                    json_data=func_log_list,
                    status="FAILED",
                )
                raise   
                      
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
        
        self._write_function_json(
            path=save_func_path, 
            json_data=func_log_list,
            status="SUCCESS",
        )
        
        gen_log_finish(pipeline_name=pipeline_name)
        print("------------------Finish run all node!!-----------------")

        print("Write logs")

        # ## data nodes detail        
        # dataset_name_list = list(pipeline.data_sets())        
        # datanode_detail = {}
        
        # for dataset_name in dataset_name_list:
        #     _, dataset_meta = catalog.load(dataset_name)
            
        #     datanode_detail[dataset_name] = {
        #         'id': dataset_name,
        #         'file_id': dataset_meta['file_id'],
        #         'meta': dataset_meta,
        #     }
            
            
        ## function detail, input_edges, output_edges, monad input and monad output
        # functions_detail = {}        
        # monad_log_list = {}
        # input_edges = []
        # output_edges = []
        
        # for node in pipeline.nodes:
            
        #     ## function detail
        #     func_obj = node.func
        #     func_source_code = inspect.getsource(func_obj)
            
        #     functions_detail[node.name] = {
        #         'func_id': node.name,
        #         'func_name': node.name,
        #         'python_code': func_source_code,
        #         'input_ids': node.inputs,
        #         'output_ids': node.outputs,
        #     }
            
        #     ## input_edges & monad input
        #     for dataset_name in list(node.inputs):
        #         edge_id = f'{dataset_name}_____{node.name}'
                
        #         # input_edges
        #         input_edges.append({
        #             'edge_id': edge_id,
        #             'source': dataset_name,
        #             'target': node.name,
        #         })

        #         monad_log_list[edge_id] = self._read_monad_logs(
        #             df_val_types=df_val_types,
        #             type='read',
        #             datanode_detail=datanode_detail,
        #             dataset_name=dataset_name,
        #             start_run_all=start_run_all,
        #             validation_log_dir=validation_log_dir,
        #             datanode=datanode,
        #         )
            
        #     ## output_edges & monad output
        #     for dataset_name in list(node.outputs):
        #         edge_id = f'{node.name}_____{dataset_name}'
                
        #         # output_edges
        #         output_edges.append({
        #             'edge_id': edge_id,
        #             'source': node.name,
        #             'target': dataset_name,
        #         })

        #         monad_log_list[edge_id] = self._read_monad_logs(
        #             df_val_types=df_val_types,
        #             type='write',
        #             datanode_detail=datanode_detail,
        #             dataset_name=dataset_name,
        #             start_run_all=start_run_all,
        #             validation_log_dir=validation_log_dir,
        #             datanode=datanode
        #         )
                
        
        # output_dict = {
        #     "pipeline": {
        #         "name": pipeline_name,
        #         "project": project_name,
        #         "git": {
        #             "commits": commits,
        #             "current_branch_name": current_branch_name,
        #             "last_commit_url": last_commit_url,
        #         }
        #     },
        #     "datanodes": datanode_detail,
        #     "functions": functions_detail,
        #     "input_edges": input_edges,
        #     "output_edges": output_edges,
        #     "monad_log": monad_log_list,
        #     "function_log": func_log_list,
        # }
        # # print(output_dict)
        
        
        # print('------------')
        # end_run_all = datetime.now()
        
        # result_status = "SUCCESS" if is_run_all_success else "FAILED"
        # run_all_result = {
        #     'uuid': str(uuid.uuid4()),
        #     'pipeline': pipeline_id,
        #     'start_time': start_run_all,
        #     'end_time': end_run_all,
        #     # 'duration': str(end_run_all - start_run_all),
        #     'status': result_status,
        #     'result': output_dict,
        #     'last_editor': last_editor,
        # }
        
        # json_str = json.dumps(run_all_result, indent=4, default=str)
        # json_data = json.loads(json_str)
        # print(json_str)
        # res = requests.post(url, json=json_data, headers=headers)
        # print(res)

class WriteLogRunner(DsmRunner):
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
               
        # kedro default
        nodes = pipeline.nodes
        done_nodes = set()
        load_counts = Counter(chain.from_iterable(n.inputs for n in nodes))
        
        func_log_list = {}

        
        for exec_index, node in enumerate(nodes):
            start_time = datetime.now()
            
            is_success = True
            error_log = "-----"
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
            # end_time = datetime.now()
            # delta = end_time - start_time
            # print('Difference is:', delta)


            # log_filename = f'function_{node.name}.txt'
            # log_path = os.path.join(validation_log_dir, log_filename)
            # with open(log_path, 'w') as f:
            #     f.write(error_log)


            # # write log file
            # datanode.upload_file(
            #     directory_id=log_folder_id, 
            #     file_path=log_path, 
            #     description=f"log file of '{node.name}'", 
            #     # lineage=[file_id]
            # )
            # time.sleep(1)
            # log_file_id = datanode.get_file_id(name=log_filename, directory_id=log_folder_id)
            
            # func_log_list[node.name] = {
            #     "func_id": node.name,
            #     "name": node.name,
            #     "type": 'Dask',
            #     "start": start_time,
            #     "end": end_time,
            #     "duration": str(delta),
            #     "is_success": is_success,
            #     "logs_file_id": log_file_id,
            # }
            # #####
            
            

            # # decrement load counts and release any data sets we've finished with
            # for data_set in node.inputs:
            #     load_counts[data_set] -= 1
            #     if load_counts[data_set] < 1 and data_set not in pipeline.inputs():
            #         catalog.release(data_set)
            # for data_set in node.outputs:
            #     if load_counts[data_set] < 1 and data_set not in pipeline.outputs():
            #         catalog.release(data_set)

            # self._logger.info(
            #     "Completed %d out of %d tasks", exec_index + 1, len(nodes)
            # )