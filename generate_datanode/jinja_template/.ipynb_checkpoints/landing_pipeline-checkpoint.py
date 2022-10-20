from kedro.pipeline import Pipeline, node, pipeline
from .nodes import pass_data

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            {% for node in node_list -%}     
            node(
                func=pass_data,
                inputs=["{{ node.sql_query_catalog_name }}"],
                outputs="{{ node.landing_catalog_name }}",
                name="{{ node.landing_catalog_name }}___node",
            ),
            {% endfor %}
        ]
    )
