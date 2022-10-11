from kedro.pipeline import Pipeline, node, pipeline
from .nodes import get_change_data

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            {% for node in node_list -%}     
            node(
                func=get_change_data,
                inputs=["{{ node.sql_query_catalog_name }}", "{{ node.landing_latest_file_id }}___{{ node.landing_latest_catalog_name }}"],
                outputs=["{{ node.landing_temp_file_id }}___{{ node.landing_temp_catalog_name }}", "{{ node.landing_change_file_id }}___{{ node.landing_change_catalog_name }}"],
                name="{{ node.landing_change_catalog_name }}___node",
            ),
            {% endfor %}
        ]
    )
