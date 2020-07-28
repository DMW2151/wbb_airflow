from airflow.plugins_manager import AirflowPlugin
from .operators.fs_to_mongo_operator import JSONtoMongoDBOperator


class mongoPlugin(AirflowPlugin):
    name = "mongo_plugin"
    operators = [JSONtoMongoDBOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
