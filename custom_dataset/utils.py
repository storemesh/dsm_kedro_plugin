import datetime
import os
import shutil

def validate_etl_date(etl_date):
    try:
        data_date = datetime.date.fromisoformat(etl_date) 
        return data_date  
    except ValueError:
        raise ValueError(f"etl_date: Incorrect date format, it should be YYYY-MM-DD, but got {etl_date}")
    
def remove_file_or_folder(path):
    # delete temp file
    if os.path.isdir(path): 
        shutil.rmtree(path)
    else:
        os.remove(path)