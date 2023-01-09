from pathlib import Path
import pathlib
import os

KEDRO_PROJECT_BASE = pathlib.Path(__file__).parent.resolve().parent.parent.parent
PIPELINE_PROJECT_PATH = os.path.join(KEDRO_PROJECT_BASE, 'src/etl_pipeline')

GENERATE_CATALOG_PATH = os.path.join(KEDRO_PROJECT_BASE, 'conf/base/catalogs/generated')
JINJA_PATH = os.path.join(KEDRO_PROJECT_BASE, 'src/dsm_kedro_plugin/generate_datanode/jinja_template')
                                     
QUERY_LANDING_PIPELINE_PATH = os.path.join(PIPELINE_PROJECT_PATH, 'pipelines/query_landing')

SQL_DATANODE_CATALOG_PATH = os.path.join(KEDRO_PROJECT_BASE, 'conf/base/catalogs/generated/catalog_01_sql_datanode.yml')
LANDING_CATALOG_PATH = os.path.join(KEDRO_PROJECT_BASE, 'conf/base/catalogs/generated/catalog_02_landing.yml')
INTEGRATION_CATALOG_PATH = os.path.join(KEDRO_PROJECT_BASE, 'conf/base/catalogs/generated/catalog_03_integration.yml')