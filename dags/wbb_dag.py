import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

import requests

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import (BranchPythonOperator,
                                               PythonOperator)

from airflow.operators.mongo_plugin import JSONtoMongoDBOperator
from airflow.operators.dummy_operator import DummyOperator

# Default starting params for our DAG
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 25),
    'email': ['dmw2151@columbia.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'schedule': '@daily',
    'retry_delay': timedelta(minutes=5)
}


def has_new_filings(**context):
    """Pulls XCOM from task instance; continue pipeline if xcom is not null"""
    target_links = context['ti'].xcom_pull(
        key='filing_index_pgs',
        task_ids='download_recents_links'
    )

    if target_links:
        return ['reformat_filings_links']
    else:
        return ['close_task']


def extract_entry_href(
    root,
    namespace='atom',
    namespaces={'atom': 'http://www.w3.org/2005/Atom'}
):
    """
    Parse XML to extract valid paths

    NOTE: function uses */Atom/2005 namespace by default. Namespace mapping
    allows for easaier referecne to specific tags/attribs in XML. Without
    mappings `link` tags must be accessed as
    "{http://www.w3.org/2005/Atom])link", with mappigs, as "atom/linnk"
    args:
        root: ElementTree.Root:
        namespace: str: namespace of parsed document
        namespaces: dict: mapping of document namespace to the associated text
    returns:
        list: list of all valid `href`
    """
    return [
        e.find(f'{namespace}:link/[@href]', namespaces).get('href') for
        e in root.findall(f'{namespace}:entry', namespaces)
    ]


def get_recent_filings_directory(payload, **context):
    '''
    Download target data from SEC recent filings directory and push
    to an Airflow xcom.
    args:
        payload: dict: Expects parameters to format
        the SEC EDGAR query string. e.g. a valid query string might be:
            - https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&\
                owner=only&start=0&count=100&datea=2020-07-23&output=atom

        Expects params from among the following keys??
            - action (??)
            - CIK (company/individual unique Central Index Key;
                lookup https://www.sec.gov/edgar/searchedgar/cik.htm)
            - forms (SEC Form Type, e.g. 10K, 10Q, 4; )
            - company (company search, e.g. `Shell`)
            - datea (starting date)
            - dateb (ending date)
            - owner (one of `include`, `exclude`, `only`)
            - start (starting index of results e.g. 0)
            - count (only accepts 40, 80, 100??)
            - output (must be `atom` to recieve valid XML response??)
    '''
    r = requests.get(
        url='https://www.sec.gov/cgi-bin/browse-edgar?',
        params=payload
    )
    r.raise_for_status()

    # Parse XML to elementTree and extract links
    root = ET.fromstring(r.content)
    target_links = extract_entry_href(root)

    # NOTE: There is no data pipelining between tasks in Airflow! We can
    # Share data using temporrary files on the fs or using Airflow context.

    # Airflow tasks have access to the DAG context. With PythonOperator, when
    # we add `provide_context=True`, information about our dag becoes available
    # to our python function.
    #
    # We can then push/pull xcoms using `context['ti'].xcom_push(key, value)`
    # and `context['ti'].xcom_pull(task, key)`
    context['ti'].xcom_push(
        key='filing_index_pgs',
        value=target_links
    )


def reformat_filings_links(**context):
    """
    Replace the file extenstion on a given link

    the function `extract_entry_href()` returns files ending in .html,
    replacing `-index.htm` with `.txt` makes these links more useful.
    We can easily parse the .txt file to json

    For example:
     - https://www.sec.gov/Archives/edgar/data/2110/\
            000119312520197566/0001193125-20-197566-index.htm
     - https://www.sec.gov/Archives/edgar/data/2110/\
            000119312520197566/0001193125-20-197566.txt
    """
    target_links = context['ti'].xcom_pull(
        key='filing_index_pgs',
        task_ids='download_recents_links'
    )

    target_text_links = list(
        map(
            lambda s: re.sub('-index.htm$', '.txt', s),
            target_links
        )
    )

    context['ti'].xcom_push(
        key='filing_download_urls',
        value=target_text_links
    )


def create_temporary_file(**context):
    """
    Create a temporary file that contains two space delim fields:
        1. Url to full text of the filling
        2. document_uid - unique identifier of a document; parsed from url
    """

    # Use XCOMs to pull in the urls to filling summaries:
    target_text_links = context['ti'].xcom_pull(
        key='filing_download_urls',
        task_ids='reformat_filings_links'
    )

    # Create a Temporary file and write url & doc_uid...
    f = NamedTemporaryFile(delete=False)
    for doc_link in target_text_links:

        # Validate link has a uid w. regex patten
        doc_uid = re.search(
            '(?<=(https://www.sec.gov/Archives/edgar/data/))([\d|/|-]+)',
            doc_link
        )
        if doc_uid:
            doc_uid_no = doc_uid.group().replace('/', '-')
            f.write(bytes(f'{doc_link} {doc_uid_no}\n', 'utf-8'))

    # Push the name of the temp file so other tasks can use this information 
    context['ti'].xcom_push(
        key='filings_tmp_filepath',
        value=f.name
    )


def delete_temporary_file(**context):
    """ Remove temporary file created eearlrier in pipeline"""
    os.unlink(
        context['ti'].xcom_pull(
            task_ids='create_tmp_file', key='filings_tmp_filepath'
        )
    )

# Define DAG...
with DAG(dag_id='_wbb_process_f4_forms', default_args=args) as dag:

    # Download a list of recent filings
    download_recents_links = PythonOperator(
            task_id='download_recents_links',
            python_callable=get_recent_filings_directory,
            op_kwargs={
                'payload': {
                    'action': 'getcurrent',
                    'owner': 'only',
                    'type': '4',
                    'start': 0,
                    'count': 100,
                    'datea': "{{ yesterday_ds }}",
                    'dateb': "{{ ds }}",
                    'output': 'atom'
                }
            },
            provide_context=True,
            dag=dag
    )

    # Logic for deciding which part of the pipeline to run
    new_filings_exist = BranchPythonOperator(
            task_id='new_filings_exist',
            python_callable=has_new_filings,
            provide_context=True,
            dag=dag
    )

    # Reformat the recent filings' '*.htm' links to '*.txt' links
    reformat_filings_links = PythonOperator(
            task_id='reformat_filings_links',
            python_callable=reformat_filings_links,
            provide_context=True,
            dag=dag
    )

    # Create a temp file which contains all links from `reformat_filings_links`
    create_tmp_file = PythonOperator(
            task_id='create_tmp_file',
            python_callable=create_temporary_file,
            provide_context=True,
            dag=dag
    )

    # NOTE: Following step uses a few bash commands to process our data and
    # revolves around piping a url to `processF4File`.

    # processF4File is a bash function written in ./utils/processF4File.sh
    # that does the following:
    #    - downloads a url
    #    - extracts the content of an XML tag
    #    - converts the result to json

    # In total; the bash command we're calling does the following;
    #   - make the processF4File function available
    #   - apply processF4File on each line of our temp file
    #   - Insert the resulting data into mongoDB

    download_f4_forms_raw = BashOperator(
        task_id='download_recents',
        bash_command="""
        export filings_dir=$(mktemp -d) &&\
        . $airflow_home/utils/processF4File.sh &&\
        cat $target_file |\
            xargs -n 2 -P 8 bash -c 'processF4File $0 $1' &&\
        echo $filings_dir
        """,
        env={
            'target_file': """{{ 
                task_instance.xcom_pull(
                    key='filings_tmp_filepath',
                    task_ids='create_tmp_file'
                ) }}""",
            'airflow_home': os.environ.get('AIRFLOW_HOME')
        },
        xcom_push=True,
        dag=dag
    )

    upload_local_to_mongo = JSONtoMongoDBOperator(
        task_id='upload_local_to_mongo',
        conn_id='mongo_default',
        db='filings',
        collection='ownership',
        collection_key='doc_no',
        target_xcom={
            'task_ids': 'download_recents'
        },
        dag=dag
    )

    # Clean Up Task - remove file created in `create_tmp_file`
    rm_tmp_file = PythonOperator(
            task_id='rm_tmp_file',
            python_callable=delete_temporary_file,
            provide_context=True,
            dag=dag
    )

    # Clean Up Task - remove directory created in `download_recents`
    rm_tmp_directory = BashOperator(
        task_id='rm_tmp_directory',
        bash_command="""rm -rf $filings_tmp_dir """,
        env={
            'filings_tmp_dir': """{{
                task_instance.xcom_pull(task_ids='download_recents')
            }}""",
        },
        dag=dag
    )

    # Exit Task; just used as a graph node, no other purpose...
    close_task = DummyOperator(
        task_id='close_task'
    )

    # Set Order of execution between dags; there are many ways to set
    # deps between DAGs X >> Y, X << Y, X.set_upstream(Y), Y.set_downstream(X)
    # are all the same way of indicating Y depends on X...

    download_recents_links >> new_filings_exist

    # New Files To Download
    new_filings_exist >> reformat_filings_links >> create_tmp_file >> \
        download_f4_forms_raw >> upload_local_to_mongo

    download_f4_forms_raw >> rm_tmp_file >> close_task
    upload_local_to_mongo >> rm_tmp_directory >> close_task

    # No New Files to downloadd
    new_filings_exist >> close_task

