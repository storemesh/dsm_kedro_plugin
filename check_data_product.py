import sys
import os
sys.path.append(os.getcwd())

import gspread
import pandas as pd
import yaml
from pathlib import Path
import dask.dataframe as dd
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
import logging
import argparse

from src.tests.utils import get_unique_diff

argParser = argparse.ArgumentParser()
argParser.add_argument("-gsheet_fname", help="file name of google sheet")

 #-gsheet_fname "DITP65_Integration Database Model"
args = argParser.parse_args()
gsheet_fname = args.gsheet_fname



def map_dtype(dtype):
    if dtype not in [None, '']:
        data_type = dtype.split('(')[0].strip().lower()
        # rootLogger.info(dtype)
        map_type = {
            'string': ['string'],
            'char': ['string'],
            'integer': ['Int32', 'Int64'],
            'float': ['float64'],
            'datetime': ['datetime64'],
            'boolean': ['bool'],
            None: None,
        }
        if data_type not in map_type:
            rootLogger.info(f'     {data_type} is not exist in map dict')
            return None

        return map_type[data_type]
    else:
        return None


# integration file
with open('conf/base/catalogs/manual/integration.yml') as f:
    integration_dict = yaml.safe_load(f)
    
    
# kedro load catalog 
project_path = Path.cwd()
bootstrap_project(project_path)

session = KedroSession.create(project_path)
catalog = session.load_context().catalog
# catalog = catalog_func()
 
    
# read google sheet
sa = gspread.service_account(filename='conf/local/service_account.json')
sh = sa.open(gsheet_fname)

wks_list = sh.worksheets()



# logs generator
log_path = "logs/check_data_product.log"              

# logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logFormatter = logging.Formatter("%(message)s")
rootLogger = logging.getLogger('kedro')

fileHandler = logging.FileHandler(log_path, mode='w')
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)                
rootLogger.setLevel(logging.DEBUG)

for sheet in wks_list:
    rootLogger.info(f'\n\n')
    sheet_name = sheet.title
    rootLogger.info(f'---- {sheet_name} ----')

    integration_name = f'i.{sheet_name}'
    if integration_name not in integration_dict:
        rootLogger.info(f"  '{sheet_name}' is in Data Product but it's not in Integration Catalog")
        continue


    data = sheet.get("A14:H100")
    df_dp = pd.DataFrame(data)
    new_header = df_dp.iloc[0]
    df_dp = df_dp[1:]
    df_dp.columns = new_header

    try:
        ddf, meta = catalog.load(integration_name)
        inte_columns = ddf.columns

        ## display non intersec columns
        non_intersec, only_in_dp, only_in_inte, alert_text_diff = get_unique_diff(df_dp['Column Name'], inte_columns, only_top=False, origin_name='data product', output_name='integration')
        if len(non_intersec) > 0:
            rootLogger.info("\n    There are some columns that doesn't match between data product and integration file")
            rootLogger.info(alert_text_diff)
            rootLogger.info('\n')
        rootLogger.info(f"    Data Product Columns Mismatch:")
        for index, row in df_dp.iterrows():
            dp_column = row['Column Name']
            dp_dtype = map_dtype(row['Data Type'])

            if dp_column not in inte_columns:
                rootLogger.info(f"       '{dp_column}': not exist in Integration File")
                continue

            if dp_dtype == None:
                rootLogger.info(f"       '{dp_column}': data type is not define in Data Product or it's not exist in map dict")
                continue

            inte_dtype = str(ddf[dp_column].dtype)
            if inte_dtype not in dp_dtype:
                rootLogger.info(f'''       '{dp_column}': Data type is not same.   Data Product: '{row['Data Type']}'({dp_dtype})     Integration File: '{inte_dtype}' ''')


    except Exception as e:
        rootLogger.info(f'         ****Exception*** : {e}')
    
    
        