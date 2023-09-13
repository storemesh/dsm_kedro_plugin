from typing import Any, Dict, Tuple, List

import numpy as np
import sys
import os
sys.path.append(os.getcwd())

from kedro.io import AbstractDataSet
from kedro.extras.datasets.api.api_dataset import APIDataSet
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
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
import requests
import logging
from multiprocessing.pool import ThreadPool
import dask
import shutil
from pathlib import Path

from src.dsm_kedro_plugin.custom_dataset.utils import validate_etl_date, remove_file_or_folder
from src.dsm_kedro_plugin.custom_dataset.validation.validation_schema import validate_data, ValidationException
from src.config.project_setting import DATAPLATFORM_API_URI, OBJECT_STORAGE_URI, PROJECT_FOLDER_ID, OBJECT_STORAGE_SECUE, ETL_MODE

# log setting
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger('kedro')
logger.setLevel(logging.INFO)               

# load kedro context
project_path = Path.cwd()
bootstrap_project(project_path)

session = KedroSession.create(project_path)
kedro_context = session.load_context()


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
        config: Dict = {},      
        extra_param: Dict = {},
        read_extra_param: Dict = {},
        write_extra_param: Dict = {},
    ):
        """Initialize a ``DsmDataNode`` with parameter from data catalog.

        Args:
            credentials (Dict[str, Any]): Dictionary of credentials variable. It must be contain dsm token in key 'token'. You can define it in `local/credentials.yml` 
            file_name (str): File Name to save/load in Data Discovery (.parquet).
            folder_id (int): Folder Id to save/load datanode in Data Discovery. If it is set, it will ignore `project_folder_name`
            project_folder_name (str): Folder Name to save/load datanode at root of project in Data Discovery (root project is defined in ``src/config/project_setting.py``). 
            schema (Dict): schema config of this Datanode. Use it for validation. You need to define validation rules in ``src/config/validation_rules.py`` first.
            extra_param (Dict): extra parameter to send to dsmlibrary

        Raises:
            Exception: When parameters are incorrectly

        Returns:
            A new ``DsmDataNode`` object.
        """
    
        self._token = credentials.get('token', None)
        self._apikey = credentials.get('apikey', None)
        self._file_id = None
        self._folder_id = folder_id
        self._project_folder_name = project_folder_name
        self._file_name = file_name
        self._schema = schema
        self._extra_param = extra_param  
        self._read_extra_param = read_extra_param 
        self._write_extra_param = write_extra_param 
        self._config = config         
        
        self.meta = {
            "file_id": None,
            "folder_id": folder_id,
            "file_name": file_name,
            "schema": schema,
        }

        self._validate_input()
        
    def _get_dask_dataframe(self, file_extension=None):        
        data_node = self._get_data_node()
        folder_id = self._get_folder_id(data_node) 

        # load file by dask
        file_id = data_node.get_file_id(name=f"{self._file_name}.{file_extension}", directory_id=folder_id)
        ddf = data_node.read_ddf(file_id=file_id, extra_param=self._read_extra_param)
        
        # load metadata        
        f_meta = data_node.get_meta_file(file_id=file_id)        
        self.meta['file_id'] = file_id
        self.meta['folder_id'] = folder_id
        self.meta['meta_discovery'] = f_meta
        
        return (ddf, self.meta)     

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
            token=self._token,
            apikey=self._apikey,
            dataplatform_api_uri=DATAPLATFORM_API_URI,
            object_storage_uri=OBJECT_STORAGE_URI,
            object_storage_secue=OBJECT_STORAGE_SECUE,
        )
        return data_node

    def _validate_data(self, ddf, type):
        folder_path = 'logs/validation_logs/'
        save_path = os.path.join(folder_path, f'{self._folder_id}_{self._file_name}_{type}.parquet')
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, exist_ok=True)

        if os.path.exists(save_path):
            os.remove(save_path) # clear previous validation logs

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

            if df_critical_error.shape[0] > 0:
                columns = df_critical_error.columns
                df_critical_error = df_critical_error.reset_index().drop(columns=['index'])
                raise ValidationException(
                    f"'{self._file_name}' have critical errors and is not allowed to save data. For fixing, see the detail below\n"
                    f"df_critical_error : \n{df_critical_error}"
                )
                
            if df_rule_error.shape[0] > 0:
                df_rule_error.to_parquet(save_path, index=False)
                
                # remove error columns
                pk_remove_list = df_rule_error[df_rule_error['is_required'] == True]['pk'].unique()
                ddf = ddf[~ddf[self._schema['pk_column']].isin(pk_remove_list)]

        return ddf
    
    def calculate_statistic_log(self, ddf):
        logger.info('           Read File Stat:     ')
        all_record = ddf.shape[0].compute()
        all_null_value = ddf.isna().sum().sum().compute()
        all_column = ddf.columns.shape[0]
        
        data_statistic = { 
            'all_record': all_record, 
            'all_null_value': int(all_null_value),
            'all_column': all_column,
        }
        
        return data_statistic
    
    def _write_statistic_log(self, before_validate_stat, after_validate_stat, file_id):        
        base_url = os.path.join(DATAPLATFORM_API_URI, 'api')
        
        headers = {'Authorization': f'Bearer {self._token}'} if self._apikey == None else {'Authorization': f'Api-Key {self._apikey}'}
        _res = requests.get(f'{base_url}/v2/file/{file_id}/',  headers=headers)
        meta = _res.json()
        context_meta = meta.get('context', {}) #.get('statistics', {})
        context_meta.update({
            'statistics': {
                "before_validate_stat": before_validate_stat,
                "after_validate_stat": after_validate_stat,
            }
        })
        
        _res = requests.patch(
            f'{base_url}/v2/file/{file_id}/',  
            headers=headers,
            json={
                'context': context_meta
            }
        )
        
        return _res
    
        
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
        return self._get_dask_dataframe(file_extension='parquet')   

    def _save(self, data: Tuple[dd.DataFrame, List[int]]) -> None:
        start_time = datetime.datetime.now()
        ddf, meta_list = data
        lineage_list = [ item['file_id'] for item in meta_list]
        
        # check save mode (initial or append)
        if ETL_MODE:
            etl_config = ETL_MODE['DsmDataNode']['write']
            if etl_config['auto_partition'] and \
                (self._project_folder_name in etl_config['append_folder']) and \
                ('partition_on' not in self._write_extra_param):
                    # add new column for partitioning (only catalog that provide partition_on param in _write_extra_param)
                    etl_date = kedro_context.params.get('etl_date', None) #catalog.load('params:etl_date')
                    validate_etl_date(etl_date)
                    ddf['_retrieve_date'] = etl_date
                    self._write_extra_param['partition_on'] = '_retrieve_date'         
                    
            if (etl_config['mode'] == 'append') and \
                (self._project_folder_name in etl_config['append_folder']) and \
                ('append' not in self._write_extra_param):
                    self._write_extra_param['append'] = True 
        
        data_node = self._get_data_node()
        self._folder_id = self._get_folder_id(data_node)
        file_id = None

        with ProgressBar():
            ## save data local
            logger.info('----- Save data and information before validate------')
            logger.info('      1. Write Temp File:     ')
            save_file_name = f'data/03_primary/{self._folder_id}_{self._file_name}.parquet'
            if os.path.exists(save_file_name) and os.path.isdir(save_file_name): shutil.rmtree(save_file_name)
            # import pdb; pdb.set_trace()
            ddf.to_parquet(save_file_name)
            
            logger.info('      2. Read Temp File:     ')
            ddf_tmp = dd.read_parquet(save_file_name)
            before_validate_stat = self.calculate_statistic_log(ddf_tmp)
            
            if self._schema:
                logger.info('----- Do Validation ------')                
                logger.info('      1. Writing Validation:     ')
                ddf_validated = self._validate_data(ddf_tmp, type='write')                
                
                logger.info('      2. Write DataNode:     ')
                res_meta = data_node.write(
                    df=ddf_validated, 
                    directory=self._folder_id, 
                    name=self._file_name, 
                    profiling=True, 
                    replace=True, 
                    lineage=lineage_list, 
                    **self._write_extra_param
                )
                                
                time.sleep(2) # wait for file finish writing
                
                ## read validation logs
                logger.info('      3. Reading Validation:     ')
                ddf_read = data_node.read_ddf(file_id=res_meta['file_id'])                
                
                ddf_read = self._validate_data(ddf_read, type='read')
                after_validate_stat = self.calculate_statistic_log(ddf_read)
                ddf = ddf_read
                
            else:
                # no validate, save data directly to data platform
                res_meta = data_node.write(df=ddf_tmp, directory=self._folder_id, name=self._file_name, profiling=True, replace=True, lineage=lineage_list, **self._write_extra_param)
                after_validate_stat = before_validate_stat
                ddf = ddf_tmp
                
            self._write_statistic_log(
                before_validate_stat=before_validate_stat, 
                after_validate_stat=after_validate_stat, 
                file_id=res_meta['file_id']
            )  

            remove_file_or_folder(save_file_name)                      


    def _describe(self) -> Dict[str, Any]:
        pass
    
    def _get_file_extension(self):
        return 'parquet'
    
    def exists(self) -> Dict[str, Any]:
        data_node = self._get_data_node()
        folder_id = self._get_folder_id(data_node)
        file_extension = self._get_file_extension()
        file_id = data_node.get_file_id(name=f"{self._file_name}.{file_extension}", directory_id=folder_id)
        self.meta['file_id'] = file_id
        self.meta['folder_id'] = folder_id
        
        return self.meta    

    
class DsmListDataNode(DsmDataNode):        
    def _get_file_extension(self):
        return 'listDataNode'
    
    def _load(self) -> Tuple[dd.DataFrame, int]:
        data_node = self._get_data_node()
        file_extension = self._get_file_extension()
        folder_id = self._get_folder_id(data_node)
        file_id = data_node.get_file_id(name=f"{self._file_name}.{file_extension}", directory_id=folder_id)
        
        load_only_updated = self._config.get('load_only_updated', False)
        data_date = None
        
        if ETL_MODE and ETL_MODE['DsmListDataNode']:
            etl_config_read = ETL_MODE['DsmListDataNode']['read']
            if etl_config_read['mode'] == 'from_date':
                etl_date = kedro_context.params.get('etl_date', None)
                data_date = validate_etl_date(etl_date)
            
            if (self._project_folder_name in etl_config_read['list_datanode_folder']) and \
                etl_config_read['load_only_updated'] and \
                ('load_only_updated' not in self._config):
                load_only_updated = etl_config_read['load_only_updated']
        
        
        if load_only_updated:
            ddf = data_node.get_update_data(file_id=file_id, data_date=data_date)    
        else:
            if data_date:
                index, result_list = data_node.get_file_from_date(file_id=file_id, data_date=data_date)                                    
                write_file_id = result_list[index]['file_id']
            else:
                write_file_id = data_node.get_file_version(file_id=file_id)[-1]['file_id']
                            
            ddf = data_node.read_ddf(file_id=write_file_id) 
        
        # load metadata        
        f_meta = data_node.get_meta_file(file_id=file_id)     
        
        self.meta['file_id'] = file_id
        self.meta['meta_discovery'] = f_meta
        
        return (ddf, self.meta)
    
    def _save(self, data: Tuple[dd.DataFrame, List[int]]) -> None:
        ddf, meta_list = data
        lineage_list = [ item['file_id'] for item in meta_list]

        data_node = self._get_data_node()
        folder_id = self._get_folder_id(data_node)
        
        overwrite_same_date = False
        write_from_date = None
        data_date = None
        
        if ETL_MODE and ETL_MODE['DsmListDataNode']:
            etl_config_write = ETL_MODE['DsmListDataNode']['write']
            overwrite_same_date = etl_config_write['overwrite_same_date']
            
            if etl_config_write['mode'] == 'from_date':
                etl_date = kedro_context.params.get('etl_date', None)
                data_date = validate_etl_date(etl_date)                        

        with ProgressBar():
            print(f'number of partitions: {ddf.npartitions}')
            logger.info('      1. Write File:     ')
            
            # write file by using datetime now for writing time 
            res_meta = data_node.writeListDataNode(
                df=ddf, 
                directory_id=folder_id, 
                name=self._file_name, 
                profiling=True, 
                replace=True, 
                lineage=lineage_list,
                overwrite_same_date=overwrite_same_date, 
                data_date=data_date,
            )
                        
            logger.info('      2. Read File:     ')                        
            # read latest file every time (overwrite same date if same date exist)
            # write_file_id = data_node.get_file_version(file_id=res_meta['file_id'])[-1]['file_id']
            if data_date:
                index, result_list = data_node.get_file_from_date(file_id=res_meta['file_id'], data_date=data_date)                                    
                write_file_id = result_list[index]['file_id']
            else:
                write_file_id = data_node.get_file_version(file_id=res_meta['file_id'])[-1]['file_id']
                            
            ddf = data_node.read_ddf(file_id=write_file_id) 
                
            ddf_read = data_node.read_ddf(file_id=write_file_id)            
            time.sleep(2) # wait for file finish writing
            before_validate_stat = self.calculate_statistic_log(ddf_read)
            logger.info('      3. Write Logs:     ')
            self._write_statistic_log(
                before_validate_stat=before_validate_stat, 
                after_validate_stat=before_validate_stat, 
                file_id=write_file_id
            )    
    


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
        folder_id = self._get_folder_id(data_node)
        file_id = data_node.get_file_id(name=f"{self._file_name}", directory_id=folder_id)

        dask.config.set(pool=ThreadPool(1))
        ddf = data_node.read_ddf(file_id=file_id, extra_param=self._read_extra_param)       
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
        df = pd.read_excel(file_obj, **self._read_extra_param)
        ddf = dd.from_pandas(df, npartitions=1)
        
        self.meta['file_id'] = file_id
        self.meta['folder_id'] = folder_id
        
        return (ddf, self.meta)
    
    def _save(self, data: Tuple[dd.DataFrame, List[int]]) -> None:
        raise ValueError('Not support save Excel yet')


class DsmReadCSVNode(DsmDataNode):    
    def _load(self) -> Tuple[dd.DataFrame, int]:           
        return self._get_dask_dataframe(file_extension='csv')    
    
    def _save(self, data: Tuple[dd.DataFrame, List[int]]) -> None:
        ddf, meta_list = data
        lineage_list = [ item['file_id'] for item in meta_list]
        
        save_path = os.path.join('./data/01_raw/', f"{self._file_name}.csv")
        ddf.to_csv(save_path, index=False, single_file=True)
        data_node = self._get_data_node()
        _folder_id = self._get_folder_id(data_node)
        _replace = self._config.get('replace', False)
        data_node.upload_file(directory_id=_folder_id, file_path=save_path, description="", lineage=lineage_list, replace=_replace)

        remove_file_or_folder(save_path)   
        
    def _describe(self) -> Dict[str, Any]:
        pass


class DsmReadFileObject(DsmDataNode):    
    def _load(self) -> Tuple[dd.DataFrame, int]:
        data_node = self._get_data_node()
        folder_id = self._get_folder_id(data_node) 
        file_id = data_node.get_file_id(name=f"{self._file_name}", directory_id=folder_id) 
        meta, file_obj = data_node.get_file(file_id=file_id)        
        
        self.meta['file_id'] = file_id
        self.meta['folder_id'] = folder_id
        
        return (file_obj, self.meta)
    
    def _save(self, data: Tuple[dd.DataFrame, List[int]]) -> None:
#         start_time = datetime.datetime.now()
#         ddf, lineage_list = data
        
#         save_path = os.path.join('./data/01_raw/', self._file_name)
#         data_node = DataNode(self._token)   
#         data_node.upload_file(directory_id=self._folder_id, file_path=save_path, description=f"ds {self._file_name}", lineage=lineage_list)
        raise ValueError('Not support save FileObject yet')

    def _describe(self) -> Dict[str, Any]:
        pass
    
class DsmJSONFile(DsmDataNode):    
    def _load(self) -> Tuple[dd.DataFrame, int]:
        data_node = self._get_data_node()
        folder_id = self._get_folder_id(data_node)
        file_id = data_node.get_file_id(name=f"{self._file_name}", directory_id=folder_id) 
        meta, file_obj = data_node.get_file(file_id=file_id)            
        json_data = json.load(file_obj) 
        
        self.meta['file_id'] = file_id
        self.meta['folder_id'] = folder_id
        
        return (json_data, self.meta)
    
    def _save(self, data: Tuple[dict, List[int]]) -> None:
        json_data, meta_list = data
        lineage_list = [ item['file_id'] for item in meta_list]
         
        save_path = os.path.join('./data/01_raw/', self._file_name)  
        json_object = json.dumps(json_data, indent=4, default=str)
        with open(save_path, "w") as outfile:
            outfile.write(json_object)        
        
        data_node = self._get_data_node()   
        folder_id = self._get_folder_id(data_node)
        data_node.upload_file(directory_id=folder_id, file_path=save_path, description=f"data of {self._file_name}", lineage=lineage_list, replace=True)

        remove_file_or_folder(save_path)   
        
    def _describe(self) -> Dict[str, Any]:
        pass