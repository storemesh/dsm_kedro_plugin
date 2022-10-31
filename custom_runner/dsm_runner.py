"""``SequentialRunner`` is an ``AbstractRunner`` implementation. It can be
used to run the ``Pipeline`` in a sequential manner using a topological sort
of provided nodes.
"""

from collections import Counter
from itertools import chain

from pluggy import PluginManager

from kedro.io import AbstractDataSet, DataCatalog, MemoryDataSet
from kedro.pipeline import Pipeline
from kedro.runner.runner import AbstractRunner, run_node

import git
import inspect
import traceback
from datetime import datetime
import json

from etl_pipeline.pipeline_registry import register_pipelines
from config.config_source_table import PROJECT_NAME
dsm_kedro_plugin = __import__("dsm-kedro-plugin")

# from dsm-kedro-plugin.custom_dataset.validation.validation_rules import rules



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
        
        LOG_FOLDER = 293
        validation_rules = dsm_kedro_plugin.custom_dataset.validation.validation_rules.rules   
        import pdb; pdb.set_trace()
        get_token = dsm_kedro_plugin.generate_datanode.utils.utils.get_token
        token = get_token()
        validation_rules = { value['func'].name: value for key, value in validation_rules.items() }
        
        start_run_time = datetime.now()
        
        nodes = pipeline.nodes
        done_nodes = set()
        
        
        pipeline_detail = register_pipelines()
        
        # import pdb; pdb.set_trace()
        last_output_dataset_name = list(pipeline.all_outputs())[-1]
        # pipeline_detail['payment_integration'].all_outputs()
        # output_dataset_names
        
        pipeline_name = None
        for key, value in pipeline_detail.items():
            
            if last_output_dataset_name in list(value.all_outputs()):
                pipeline_name = key
                break
        
        
        
                
        ## pipeline logs
        repo = git.Repo(search_parent_directories=True)
        branch = repo.active_branch
        branch_name = branch.name
        hexsha = repo.head.object.hexsha
    


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
                
                url_path = f"{LOG_FOLDER}/{datanode_detail[dataset_name]['file_id']}.parquet"
                
                
                import pdb;pdb.set_trace()
                
                validation_rules
                
                # monad input
                monad_log_list[edge_id] = {
                    'file_id': datanode_detail[dataset_name]['file_id'],
                    'name': dataset_name,
                    'type': 'read',
                    'run_datetime': start_run_time,
                    'format_summary': None,
                    'consistency_summary': None,
                    'completeness_summary': None,
                    'log_url': url_path,
                }
            
            ## output_edges & monad output
            for dataset_name in list(node.outputs):
                edge_id = f'{node.name}_____{dataset_name}'
                
                # output_edges
                output_edges.append({
                    'edge_id': edge_id,
                    'source': node.name,
                    'target': dataset_name,
                })
                
                # monad output
                monad_log_list[edge_id] = {
                    'file_id': datanode_detail[dataset_name]['file_id'],
                    'name': dataset_name,
                    'type': 'write',
                    'run_datetime': start_run_time,
                    'format_summary': None,
                    'consistency_summary': None,
                    'completeness_summary': None,
                }
                
        
        output_dict = {
            "pipeline": {
                "name": pipeline_name,
                "project": PROJECT_NAME,
                "git": {
                    "hexsha": hexsha,
                    "branch_name": branch_name,
                }
            },
            "datanodes": datanode_detail,
            "functions": functions_detail,
            "input_edges": input_edges,
            "output_edges": output_edges,
            "monad_log": monad_log_list,
            "function_log": func_log_list,
        }
        print(output_dict)
        print(json.dumps(output_dict, indent=4, default=str))
            
        
        print('ddd')
