"""``SequentialRunner`` is an ``AbstractRunner`` implementation. It can be
used to run the ``Pipeline`` in a sequential manner using a topological sort
of provided nodes.
"""
import sys
import os
sys.path.append(os.getcwd())

from collections import Counter
from itertools import chain
from pathlib import Path

from pluggy import PluginManager

from kedro.io import AbstractDataSet, DataCatalog, MemoryDataSet
from kedro.pipeline import Pipeline
from kedro.runner.runner import AbstractRunner, run_node
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

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

# from src.etl_pipeline.pipeline_registry import register_pipelines
# from src.dsm_kedro_plugin.generate_datanode.utils.utils import get_token
# from src.dsm_kedro_plugin.generate_datanode.generate_setting import PIPELINE_PROJECT_PATH, KEDRO_PROJECT_BASE 
from src.config.project_setting import PROJECT_FOLDER_ID, PROJECT_NAME, DATAPLATFORM_API_URI, OBJECT_STORAGE_URI
from .utils.logs_generator import parse_commit_log, gen_log_start, gen_log_finish, get_pipeline_name, get_dsm_datanode, validation_log_dir

class WriteFullLogRunner(AbstractRunner):
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

    def _run_all_node(self, pipeline, pipeline_name, catalog, hook_manager, session_id, send_logs=False):
        func_log_list = {}
        datanode, token = get_dsm_datanode()
        log_folder_id = datanode.get_directory_id(parent_dir_id=PROJECT_FOLDER_ID, name="Logs")
        save_func_path = os.path.join(validation_log_dir, f'func_result_{pipeline_name}.json')

        # kedro default
        nodes = pipeline.nodes
        done_nodes = set()
        load_counts = Counter(chain.from_iterable(n.inputs for n in nodes))
        error_exception = None
        for exec_index, node in enumerate(nodes):
            start_time = datetime.now()
            
            is_success = True
            error_log = "-----"
            try:
                run_node(node, catalog, hook_manager, self._is_async, session_id)
                done_nodes.add(node)
            except Exception as e:
                is_success = False
                is_run_all_success = False
                self._suggest_resume_scenario(pipeline, done_nodes, catalog)
                error_log = traceback.format_exc()
                error_exception = e
                
            ##### function log
            end_time = datetime.now()
            delta = end_time - start_time
            print('Difference is:', delta)


            log_filename = f'function_{node.name}.txt'
            log_path = os.path.join(validation_log_dir, log_filename)
            with open(log_path, 'w') as f:
                f.write(error_log)

            # write log file
            res = datanode.writeListFile(
                directory_id=log_folder_id, 
                file_path=log_path, 
                description=f"log file of '{node.name}'", 
                replace=True
            )
            listdatanode_file_id = res['file_id']
            log_file_id = datanode.get_file_version(file_id=listdatanode_file_id)[0]
            
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

            if not is_success:
                print('------------fail-------------')
                self._write_function_json(
                    path=save_func_path, 
                    json_data=func_log_list,
                    status="FAILED",
                )

                if send_logs:
                    gen_log_finish(pipeline_name=pipeline_name)

                raise error_exception
                      
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
        
             
        pipeline_name = get_pipeline_name(pipeline=pipeline)            
        gen_log_start(pipeline_name=pipeline_name)

        self._run_all_node(
            pipeline=pipeline, 
            pipeline_name=pipeline_name, 
            catalog=catalog, 
            hook_manager=hook_manager, 
            session_id=session_id,
            send_logs=True,
        )
        
        gen_log_finish(pipeline_name=pipeline_name)
        print("------------------Finish run all node!!-----------------")

        print("Write logs")

class WriteFunctionLogRunner(WriteFullLogRunner):
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
               
        pipeline_name = get_pipeline_name(pipeline=pipeline) 

        self._run_all_node(
            pipeline=pipeline, 
            pipeline_name=pipeline_name, 
            catalog=catalog, 
            hook_manager=hook_manager, 
            session_id=session_id,
            send_logs=True,
        )