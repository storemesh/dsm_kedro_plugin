import dask.dataframe as dd
from typing import List, Tuple
import hashlib

def md5hash(x):
    return hashlib.md5(x.encode('utf-8')).hexdigest()

def pass_data(data_query: Tuple[dd.DataFrame, int]) -> Tuple[dd.DataFrame, int]:
    ddf_query, ddf_query_file_id = data_query
    
    lineage_id = [ ddf_query_file_id ]
    return (ddf_query, lineage_id)

def get_change_data(
        data_query: Tuple[dd.DataFrame, int], 
        data_landing_latest: Tuple[dd.DataFrame, int],
    ) -> List[dd.DataFrame]:

    ddf_query, ddf_query_file_id = data_query
    ddf_landing_latest, ddf_landing_latest_file_id = data_landing_latest

    ddf_query['_hash'] = ddf_query.astype(str).values.sum(axis=1)
    ddf_query['_hash'] = ddf_query['_hash'].apply(md5hash, meta=('_hash', 'object'))   

    if ddf_landing_latest is not None:
        # already have landing of this table
        landing_latest_hash_index = ddf_landing_latest['_hash'].unique().compute()        
        ddf_landing_change = ddf_query[~ddf_query['_hash'].isin(landing_latest_hash_index)]
    else:
        ddf_landing_change = ddf_query

    lineage_id = [ ddf_query_file_id, ddf_landing_latest_file_id ]
    return (ddf_query, lineage_id), (ddf_landing_change, lineage_id)