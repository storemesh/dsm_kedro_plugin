from pathlib import Path
import os

KEDRO_PROJECT_BASE = Path.cwd().parent.parent.parent # go to root of kedro project

GENERATE_CATALOG_PATH = os.path.join(KEDRO_PROJECT_BASE, 'conf/base/catalogs/generated')
JINJA_PATH = os.path.join(KEDRO_PROJECT_BASE, 'src/dsm-kedro-plugin/generate_datanode/jinja_template')

SQL_DATANODE_CATALOG_PATH = os.path.join(KEDRO_PROJECT_BASE, 'conf/base/catalogs/generated/catalog_01_sql_datanode.yml')
LANDING_CATALOG_PATH = os.path.join(KEDRO_PROJECT_BASE, 'conf/base/catalogs/generated/catalog_02_landing.yml')
INTEGRATION_CATALOG_PATH = os.path.join(KEDRO_PROJECT_BASE, 'conf/base/catalogs/generated/catalog_03_integration.yml')