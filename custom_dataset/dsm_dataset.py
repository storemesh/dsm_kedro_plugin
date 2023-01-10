from typing import Any, Dict, Tuple, List

import numpy as np
import sys
import os
sys.path.append(os.getcwd())

from kedro.io import AbstractDataSet
from kedro.extras.datasets.api.api_dataset import APIDataSet
import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import pandas as pd
from dsmlibrary.datanode import DataNode, DatabaseManagement
from sqlalchemy import sql
import json
from datetime import date
import datetime
import time

from src.dsm_kedro_plugin.custom_dataset.validation.validation_schema import validate_data, ValidationException
from src.config.project_setting import DATAPLATFORM_API_URI, OBJECT_STORAGE_URI, PROJECT_FOLDER_ID, OBJECT_STORAGE_SECUE

import time

import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger('kedro')
logger.setLevel(logging.DEBUG)

class DsmDataNode(AbstractDataSet[dd.DataFrame, dd.DataFrame]):
    """``DsmDataNode`` loads/saves data from/to Data Platform.

    Example adding a catalog entry with
    `YAML API
    <https://kedro.readthedocs.io/en/stable/data/\
        data_catalog.html#use-the-data-catalog-with-the-yaml-api>`_:

    .. code-block:: yaml
    
        # for landing dataset
        l.<your_desired_name>:
            type: dsm_kedro_plugin.custom_dataset.dsm_dataset.DsmDataNode
            project_folder_name: Landing
            file_name: <your_desired_name>
            credentials: dsmlibrary

        # for Staging dataset
        s.<database_name>_<table_name>:
            type: dsm_kedro_plugin.custom_dataset.dsm_dataset.DsmDataNode
            project_folder_name: Staging
            file_name: <database_name>_<table_name>
            credentials: dsmlibrary
            schema: {
                'columns': {
                    'TR_NO': { 'data_type': 'int', 'nullable': False, 'is_required': False, 'validation_rule': [4]},
                    'FILING_DATE': { 'data_type': 'datetime64[ns]', 'nullable': False, 'is_required': False},
                    'REG_NO': { 'data_type': 'string', 'nullable': False, 'is_required': False, 'validation_rule': [4]},
                },
                'validation_rule': [5, 6, 7, 8, 9],
                'pk_column': 'TR_NO',
            }
            
        # for Integration dataset
        i.<database_name>_<table_name>:
            type: dsm_kedro_plugin.custom_dataset.dsm_dataset.DsmDataNode
            project_folder_name: Integration
            file_name: <database_name>_<table_name>
            credentials: dsmlibrary

    """
    def __init__(
        self, 
        credentials: Dict[str, Any],        
        file_name: str,
        folder_id: int = None,
        project_folder_name: str = None,
        schema: Dict = None,      
        extra_param: Dict = {},
    ):
        """Initialize a ``DsmDataNode`` with parameter from data catalog.

        Args:
            credentials (Dict[str, Any]): Dictionary of credentials variable. It must be contain dsm token in key 'token'. You can define it in `local/credentials.yml` 
            file_name (str): File Name to save/load in Data Discovery (.parquet).
            folder_id (int): Folder Id to save/load datanode in Data Discovery. If it is set, it will ignore `project_folder_name`
            project_folder_name (str): Folder Name to save/load datanode at root of project in Data Discovery (root project is defined in `src/config/project_setting.py`). 
            schema (Dict): schema config of this Datanode. Use it for validation
            extra_param (Dict): extra parameter to send to dsmlibrary

        Raises:
            Exception: When parameters are incorrectly

        Returns:
            A new ``DsmDataNode`` object.
        """
    
        self._token = credentials['token']
        self._file_id = None
        self._folder_id = folder_id
        self._project_folder_name = project_folder_name
        self._file_name = file_name
        self._schema = schema
        self._extra_param = extra_param           
        
        self.meta = {
            "file_id": None,
            "folder_id": folder_id,
            "file_name": file_name,
            "schema": schema,
        }

        self._validate_input()

    def _validate_input(self):
        if (self._folder_id == None) and (self._project_folder_name == None):
            raise Exception("'folder_id' and 'project_folder_name' are None. One of this should be given")

    def _get_folder_id(self, datanode):
        if self._folder_id:
            folder_id = self._folder_id
        elif self._project_folder_name :            
            folder_id = datanode.get_directory_id(parent_dir_id=PROJECT_FOLDER_ID, name=self._project_folder_name)
        else:
            raise Exception("'folder_id' and 'project_folder_name' are None. One of this should be given")
        return folder_id

    def _get_data_node(self):
        data_node = DataNode(
            self._token, 
            dataplatform_api_uri=DATAPLATFORM_API_URI,
            object_storage_uri=OBJECT_STORAGE_URI,
            object_storage_secue=OBJECT_STORAGE_SECUE,
        )
        return data_node

    def _validate_data(self, ddf, type):
        folder_path = 'logs/validation_logs/'
        save_path = os.path.join(folder_path, f'{self._folder_id}_{self._file_name}_{type}.csv')
        all_record_path = os.path.join(folder_path, f'{self._folder_id}_{self._file_name}_{type}_all_record.json')
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, exist_ok=True)

        if os.path.exists(save_path):
            os.remove(save_path) # clear previous validation logs

        if os.path.exists(all_record_path):
            os.remove(all_record_path) # clear previous validation logs


        if self._schema:   
            ddf_critical_error, ddf_rule_error = validate_data(
                ddf, 
                config=self._schema, 
            )

            df_critical_error, df_rule_error, n_original_row = dask.compute(
                ddf_critical_error,
                ddf_rule_error,
                ddf.shape[0]
            )

            all_record = { 'all_record': n_original_row}
            with open(all_record_path, 'w') as f:
                json.dump(all_record, f)

            if df_critical_error.shape[0] > 0:
                columns = df_critical_error.columns
                df_critical_error = df_critical_error.reset_index().drop(columns=['index'])
                raise ValidationException(
                    f"'{self._file_name}' have critical errors and is not allowed to save data. For fixing, see the detail below\n"
                    f"df_critical_error : \n{df_critical_error}"
                )
                
            if df_rule_error.shape[0] > 0:
                df_rule_error.to_csv(save_path, index=False)
                
                # remove error columns
                pk_remove_list = df_rule_error[df_rule_error['is_required'] == True]['pk'].unique()
                ddf = ddf[~ddf[self._schema['pk_column']].isin(pk_remove_list)]

        return ddf

        
    def _load(self) -> Tuple[dd.DataFrame, int]:
        """Initialise a ``DsmDataNode`` with parameter from data catalog.

        Args:
            credentials : Dictionary of credentials variable. It must be contain dsm token in key 'token'. You can define it in `local/credentials.yml` 
            file_name : File Name to save/load in Data Discovery (.parquet).
            folder_id : Folder Id to save/load datanode in Data Discovery. If it is set, it will ignore `project_folder_name`
            project_folder_name : Folder Name to save/load datanode at root of project in Data Discovery (root project is defined in `src/config/project_setting.py`). 
            schema : schema config of this Datanode. Use it for validation
            extra_param : extra parameter to send to dsmlibrary

        Raises:
            ModularPipelineError: When inputs, outputs or parameters are incorrectly
                specified, or they do not exist on the original pipeline.
            ValueError: When underlying pipeline nodes inputs/outputs are not
                any of the expected types (str, dict, list, or None).

        Returns:
            A new ``DsmDataNode`` object.
        """
        
        data_node = self._get_data_node()
        folder_id = self._get_folder_id(data_node) 
        file_id = data_node.get_file_id(name=f"{self._file_name}.parquet", directory_id=folder_id)
        
        ddf = data_node.read_ddf(file_id=file_id)
        self.meta['file_id'] = file_id
        self.meta['folder_id'] = folder_id
        
        return (ddf, self.meta)

    def _save(self, data: Tuple[dd.DataFrame, List[int]]) -> None:
        start_time = datetime.datetime.now()
        ddf, meta_list = data
        lineage_list = [ item['file_id'] for item in meta_list]
        
        data_node = self._get_data_node()
        self._folder_id = self._get_folder_id(data_node)
        file_id = None

        logger.info('      Write Validation:     ')
        ddf = self._validate_data(ddf, type='write')
        
        logger.info('      Write File:     ')
        with ProgressBar():
            data_node.write(df=ddf, directory=self._folder_id, name=self._file_name, profiling=True, replace=True, lineage=lineage_list)
        
        ## read validation logs
        logger.info('      Read Validation:     ')
        file_id = data_node.get_file_id(name=f"{self._file_name}.parquet", directory_id=self._folder_id)
        ddf_read = data_node.read_ddf(file_id=file_id)
        ddf_read = self._validate_data(ddf_read, type='read')


    def _describe(self) -> Dict[str, Any]:
        pass
    
    def exists(self) -> Dict[str, Any]:
        data_node = self._get_data_node()
        folder_id = self._get_folder_id(data_node)
        file_id = data_node.get_file_id(name=f"{self._file_name}.parquet", directory_id=folder_id)
        self.meta['file_id'] = file_id
        self.meta['folder_id'] = folder_id
        
        return self.meta
    

    
class DsmListDataNode(DsmDataNode):
    def _load(self) -> Tuple[dd.DataFrame, int]:
        data_node = self._get_data_node()
        file_id = data_node.get_file_id(name=f"{self._file_name}.listDataNode", directory_id=self._folder_id)
        
        ddf = data_node.get_update_data(file_id=file_id)       
        self.meta['file_id'] = file_id
        
        return (ddf, self.meta)
    
    def _save(self, data: Tuple[dd.DataFrame, List[int]]) -> None:
        ddf, meta_list = data
        lineage_list = [ item['file_id'] for item in meta_list]

        data_node = self._get_data_node()
        file_id = None
        with ProgressBar():
            data_node.writeListDataNode(df=ddf, directory_id=self._folder_id, name=self._file_name, profiling=True, replace=True, lineage=lineage_list)
    
    


class DsmDataNodeAppend(DsmDataNode):
    def _save(self, data: Tuple[dd.DataFrame, List[int]]) -> None:
        ddf, lineage_list = data

        data_node = self._get_data_node()
        
        if self._data_type:
            self._check_data_type('Write Process', ddf)

        try:
            file_id = data_node.get_file_id(name=f"{self._file_name}.parquet", directory_id=self._folder_id)
            ddf_previous = data_node.read_ddf(file_id=file_id) 

            # if the first one is empty data frame (with _hash column)
            if '_hash' in ddf_previous.columns:
                overwrite_value = True
                append_value = False            
            else:
                overwrite_value = False
                append_value = True
        except Exception as e:
            raise Exception(f'file name "{self._file_name}.parquet" are not exist in directory {self._folder_id}, you need to run "python src/generate_datanode/03_generate_integration.py" to generate empty integration file first')

        today = date.today()
        ddf['_update_date'] = today.strftime("%Y-%m-%d")
        data_node.write(
            df=ddf, 
            directory=self._folder_id, 
            name=self._file_name, 
            profiling=True, 
            replace=True,
            lineage=lineage_list,

            # dask parameter
            overwrite=overwrite_value,
            append=append_value,            
            partition_on='_update_date',
            ignore_divisions=True
        )
    
class DsmSQLDataNode(DsmDataNode):
    def _load(self) -> Tuple[dd.DataFrame, int]:
        
        data_node = self._get_data_node()
        if self._file_id:
            file_id = self._file_id
        else:            
            file_id = data_node.get_file_id(name=f"{self._file_name}", directory_id=self._folder_id)
            
        ddf = data_node.read_ddf(file_id=file_id)        
            
        self.meta['file_id'] = file_id
        
        return (ddf, self.meta)
            
    def _save(self, ddf: dd.DataFrame) -> None:
        pass

    def _describe(self) -> Dict[str, Any]:
        pass
    
class DsmAPIDataSet(APIDataSet):
    def __init__(
        self, 
        credentials: Dict[str, Any] = None,
        *args, 
        **kw,
        
    ) -> None:
        super().__init__(*args, **kw)
        
        if 'headers' in credentials:
            self._request_args['headers'].update(credentials['headers'])
        
        self._request_args['auth'] = None


class DsmReadExcelNode(DsmDataNode):    

    def _load(self) -> Tuple[pd.DataFrame, int]:  
        data_node = self._get_data_node()
        folder_id = self._get_folder_id(data_node)
        file_id = data_node.get_file_id(name=f"{self._file_name}", directory_id=folder_id) 
        meta, file_obj = data_node.get_file(file_id=file_id)     
        df = pd.read_excel(file_obj, **self._extra_param)
        ddf = dd.from_pandas(df, npartitions=1)
        
        self.meta['file_id'] = file_id
        self.meta['folder_id'] = folder_id
        
        return (ddf, self.meta)
        # return (ddf, file_id)
    
    def _save(self, data: Tuple[dd.DataFrame, List[int]]) -> None:
        raise ValueError('Not support save Excel yet')