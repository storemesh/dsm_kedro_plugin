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
from src.config.project_setting import DATAPLATFORM_API_URI, OBJECT_STORAGE_URI, PROJECT_FOLDER_ID
# from src.generate_datanode.utils.utils import write_dummy_file

from line_profiler import LineProfiler
import time

import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.WARNING,  # set 3rd party logs to warning (for hiding it)
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# profile = LineProfiler()


def write_dummy_file(file_name, directory_id, data_node):
    df = pd.DataFrame([], columns=['_hash'])
    ddf_mock = dd.from_pandas(df, chunksize=100)
    data_node.write(
        df=ddf_mock,
        directory=directory_id,
        name=file_name, 
        replace=True
    )
    time.sleep(2)
    file_id = data_node.get_file_id(name=f'{file_name}.parquet', directory_id=directory_id)
    return file_id

class DsmDataNode(AbstractDataSet[dd.DataFrame, dd.DataFrame]):
    def __init__(
             self, 
             credentials: Dict[str, Any],        
             file_name: str,
             folder_id: int = None,
             project_folder_name: str = None,
             file_id: int = None,
             config: Dict = None,      
             param: Dict = {},
        ):
        self._token = credentials['token']
        self._file_id = file_id
        self._folder_id = folder_id
        self._project_folder_name = project_folder_name
        self._file_name = file_name
        self._config = config
        self._param = param           
        
        self.meta = {
            "file_id": file_id,
            "folder_id": folder_id,
            "file_name": file_name,
            "config": config,
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
        )
        return data_node

    # @profile 
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


        if self._config:   
            ddf_critical_error, ddf_rule_error = validate_data(
                ddf, 
                config=self._config, 
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
                
                
                # df_rule_error.to_csv(path_save, index=False, mode='a', header=not os.path.exists(path_save))
                df_rule_error.to_csv(save_path, index=False)
                
                # data_node.write(df=df_rule_error, directory=293, name=f"{file_id}_write", description="", replace=True, profiling=True, lineage=[file_id])

                # remove error columns
                pk_remove_list = df_rule_error[df_rule_error['is_required'] == True]['pk'].unique()
                ddf = ddf[~ddf[self._config['pk_column']].isin(pk_remove_list)]

        return ddf

        
    def _load(self) -> Tuple[dd.DataFrame, int]:
        data_node = self._get_data_node()
        # if self._file_id:
        #     file_id = self._file_id
        # else:
        #     try:
        #         file_id = data_node.get_file_id(name=f"{self._file_name}.parquet", directory_id=self._folder_id)
        #     except Exception as e:
        #         print("    Exception:", e)
        #         return None        
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
        # try:
        #     file_id = data_node.get_file_id(name=f"{self._file_name}.parquet", directory_id=self._folder_id)
        # except Exception as e:
        #     print(e)
            # write_dummy_file(self._file_name, self._folder_id, data_node)

        logger.info('      Write Validation:     ')
        ddf = self._validate_data(ddf, type='write')

        # else:
            
        #     n_original_row = dask.compute(ddf.shape[0])[0]
        logger.info('      Write File:     ')
        with ProgressBar():
            data_node.write(df=ddf, directory=self._folder_id, name=self._file_name, profiling=True, replace=True, lineage=lineage_list)

        logger.info('      Read Validation:     ')
        # read validation logs
        file_id = data_node.get_file_id(name=f"{self._file_name}.parquet", directory_id=self._folder_id)
        ddf_read = data_node.read_ddf(file_id=file_id)
        ddf_read = self._validate_data(ddf_read, type='read')

        # end_time = datetime.datetime.now()
        # logs = {
        #     'file_id': file_id,
        #     'start_time': start_time,
        #     'end_time': end_time,    
        #     'total': n_original_row,     
        # }
        # df_task_logs = pd.DataFrame([logs])
        # path_save = f'logs/task_logs/task_logs.csv'
        # df_task_logs.to_csv(path_save, index=False, mode='a', header=not os.path.exists(path_save))

        # else:
        #     raise Exception('For saving, folder_id and file_name parameter need to be specificed')

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
        # try:
        #     file_id = data_node.get_file_id(name=f"{self._file_name}.listDataNode", directory_id=self._folder_id)
        # except Exception as e:
        #     print("    Exception:", e)
        file_id = data_node.get_file_id(name=f"{self._file_name}.listDataNode", directory_id=self._folder_id)
        
        ddf = data_node.get_update_data(file_id=file_id)       
        self.meta['file_id'] = file_id
        
        return (ddf, self.meta)
    
    def _save(self, data: Tuple[dd.DataFrame, List[int]]) -> None:
        ddf, meta_list = data
        lineage_list = [ item['file_id'] for item in meta_list]

        data_node = self._get_data_node()
        # import pdb; pdb.set_trace()
        file_id = None
        # import pdb; pdb.set_trace()
        # try:
        #     file_id = data_node.get_file_id(name=f"{self._file_name}.parquet", directory_id=self._folder_id)
        # except Exception as e:
        #     print(e)
            # write_dummy_file(self._file_name, self._folder_id, data_node)
        # import pdb; pdb.set_trace()
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
    
    # def _read_excel(inputs, **kwargs):
    #     return dd.from_map(pd.read_excel, inputs, **kwargs)

    def _load(self) -> Tuple[pd.DataFrame, int]:  
        data_node = self._get_data_node()
        folder_id = self._get_folder_id(data_node)
        file_id = data_node.get_file_id(name=f"{self._file_name}", directory_id=folder_id) 
        meta, file_obj = data_node.get_file(file_id=file_id)     
        df = pd.read_excel(file_obj, **self._param)
        ddf = dd.from_pandas(df, npartitions=1)
        
        self.meta['file_id'] = file_id
        self.meta['folder_id'] = folder_id
        
        return (ddf, self.meta)
        # return (ddf, file_id)
    
    def _save(self, data: Tuple[dd.DataFrame, List[int]]) -> None:
        raise ValueError('Not support save Excel yet')

# test = DsmReadExcelNode(credentials={'token':"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNjcwMDM4OTEyLCJpYXQiOjE2Njk5NTI1MTIsImp0aSI6ImJjOWYyZWY0MDI1YzQwOGNhMjE4NDRlNWJkZjc1MTNhIiwidXNlcl9pZCI6MTV9.7mNzO6VQsozu4DjmS1x7ZpayNdNB7U1eH3dpiw_o6QM"
# }, file_name='TM_Dataset.xlsx', project_folder_name="Landing")
# print(test.load())