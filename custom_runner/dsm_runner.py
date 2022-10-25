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

from etl_pipeline.pipeline_registry import register_pipelines
from config.config_source_table import PROJECT_NAME

import git

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
        nodes = pipeline.nodes
        done_nodes = set()
        
        
        pipeline_detail = register_pipelines()
        
        output_dataset_names = pipeline.all_outputs()
        pipeline_detail['payment_integration'].all_outputs()
        
        pipeline_name = None
        for key, value in pipeline_detail.items():
            if output_dataset_names == value.all_outputs():
                pipeline_name = key
                break
                
        
        repo = git.Repo(search_parent_directories=True)
        branch = repo.active_branch
        branch_name = branch.name
        hexsha = repo.head.object.hexsha
        
        pipeline_logs = {
            "name": pipeline_name,
            "project": PROJECT_NAME,
            "git": {
                "hexsha": hexsha,
                "branch_name": branch_name,
            }
        }
        
        
        
            
            
        
        import pdb;pdb.set_trace()

        load_counts = Counter(chain.from_iterable(n.inputs for n in nodes))

        for exec_index, node in enumerate(nodes):
            try:
                run_node(node, catalog, hook_manager, self._is_async, session_id)
                done_nodes.add(node)
            except Exception:
                self._suggest_resume_scenario(pipeline, done_nodes, catalog)
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
            
        ## data nodes detail
        
        dataset_name_list = list(pipeline.data_sets())        
        data_nodes = {}
        import pdb;pdb.set_trace()
        for dataset_name in dataset_name_list:
            _, dataset_meta = catalog.load(dataset_name)
            
            data_nodes[dataset_name] = {
                'id': dataset_name,
                'file_id': dataset_meta['file_id'],
                'meta': dataset_meta,
            }
        
        print('ddd')
