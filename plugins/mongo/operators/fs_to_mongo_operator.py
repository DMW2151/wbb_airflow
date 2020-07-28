import json
import os
import glob
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from contextlib import closing


class JSONtoMongoDBOperator(BaseOperator):
    """
    Takes local JSON files and put to MongoDB
    args:
        conn_id: str:
        db: str: mongoDB to use to upload target; currently no suppport
            for differentiating between auth and data dbs
        collection: str: collection name, target of data upload
        collection_key: str: non-null key of each document on insert & update
        target_path: str: Either path to a *.json file or a directory
            containing *.json to upload to Mongo. Cannot be passed with
            `target_xcom`
        target_xcom: str: an xcom key that pulls either a path to a *json
            or a directory containing *.json to upload to Mongo. Cannot be
            passed with `target_path`
    """
    @apply_defaults
    def __init__(
        self,
        conn_id,
        db,
        collection,
        collection_key,
        target_path=None,
        target_xcom=None,
        *args,
        **kwargs
    ):
        super(JSONtoMongoDBOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.db = db
        self.collection = collection
        self.target_path = target_path
        self.target_xcom = target_xcom
        self.collection_key = collection_key

    def _update_db(self, filepath, collection):
        """
        Parses local JSON; writes to MongoDB
        args:
            filepath: str: path to *.json to upload...
            collection: str: name of collection to update
        """
        with open(filepath) as fi:
            try:
                doc = json.load(fi)
                key = {
                    self.collection_key: doc.get(self.collection_key)
                }

                collection.update(key, doc, upsert=True)
            except json.decoder.JSONDecodeError:
                return False
        fi.close()
        return True

    def execute(self, context):
        """Executed by task_instance at runtime"""

        with closing(MongoHook(self.conn_id).get_conn()) as client:
            db = client[self.db]
            collection = db[self.collection]

            # NOTE: Pass only one of target_xcom and target_path
            assert bool(self.target_path) ^ bool(self.target_xcom)

            if self.target_path:  # Use path given in task definition
                target = self.target_path

            else: # Access from XCOM
                target = context['task_instance'].xcom_pull(
                    **self.target_xcom
                )

            if os.path.isdir(target):
                for filepath in glob.glob(f'/{target}/*.json'):
                    self._update_db(filepath, collection)

            elif os.path.isfile(target):
                self._update_db(target, collection)
            
            else: # Should Never Exit Here...
                return False
        
        
        return True
