"""
Pipeline for pulling Red Light Camera data from Open Data API in json format via
https://secure.toronto.ca/opendata/cart/red_light_cameras.json (see
https://secure.toronto.ca/opendata/cart/red_light_cameras/details.html). This
json file will be stored in the existing table 'vz_safety_programs_staging.rlc'
in the bigdata RDS (table will be truncated each time this script is called).

To be run on the EC2.
"""
from datetime import datetime
import os
import sys
import psycopg2
import requests
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# Credentials
from airflow.hooks.postgres_hook import PostgresHook
vz_cred = PostgresHook("vzbigdata") # name of Conn Id defined in UI
vz_pg_uri = vz_cred.get_uri() # connection to RDS for psql via BashOperator
conn = vz_cred.get_conn() # connection to RDS for python via PythonOperator

# ------------------------------------------------------------------------------
# Slack notification
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: EC2 Task Failed / Tâche échouée au Ec2.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        proxy='http://137.15.73.132:8080'
        )
    return failed_alert.execute(context=context)

# ------------------------------------------------------------------------------
AIRFLOW_DAGS = os.path.dirname(os.path.realpath(__file__))
AIRFLOW_ROOT = os.path.dirname(AIRFLOW_DAGS)
AIRFLOW_TASKS = os.path.join(AIRFLOW_ROOT, 'assets/rlc/airflow/tasks')

DEFAULT_ARGS = {
    'email': ['Cathy.Nangini@toronto.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'owner': 'airflow',
    'start_date': datetime(2019, 9, 16), # YYYY, MM, DD
    'task_concurrency': 1,
    'on_failure_callback': task_fail_slack_alert
}

# ------------------------------------------------------------------------------
def pull_rlc_manual(conn):
  '''
  Connect to bigdata RDS, pull Red Light Camera json file from Open Data API,
  and overwrite existing rlc table in the vz_safety_programs_staging schema.
  '''

  local_table='vz_safety_programs_staging.rlc_manual'
    url="https://secure.toronto.ca/opendata/cart/red_light_cameras.json"
    data = requests.get(url).json()
    
    cur = conn.cursor()

    fields = [
        'additionalInfo',
        'side2',
        'midBlock',
        'tcs',
        'rlc',
        'policeDivision1',
        'privateAccess',
        'x',
        'activationDate',
        'ward2',
        'ward3',
        'ward1',
        'ward4',
        'district',
        'longitude',
        'side1',
        'latitude',
        'policeDivision2',
        'policeDivision3',
        'name',
        'y',
        'main'
    ]

    for item in data:
        rlc_data = {field: item[field] for field in fields}
        cur.execute("INSERT INTO vz_safety_programs_staging.rlc_manual VALUES (%s)", (json.dumps(rlc_data),))

# ------------------------------------------------------------------------------
# Set up the dag and task
RLC_DAG = DAG(
    'rlc_dag',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    template_searchpath=[os.path.join(AIRFLOW_ROOT, 'assets/rlc/airflow/tasks')],
    schedule_interval='0 4 * * 1-5')
    # minutes past each hour | Hours (0-23) | Days of the month (1-31) | Months (1-12) | Days of the week (0-7, Sunday represented as either/both 0 and 7)

TRUNCATE_TABLE = BashOperator(
    task_id='truncate_rlc',
    bash_command="/truncate_rlc_manual.sh",
    env={'vz_pg_uri':vz_pg_uri},
    retries=0,
    dag=RLC_DAG
)

PULL_RLC = PythonOperator(
    task_id='pull_rlc_manual',
    python_callable=pull_rlc_manual,
    dag=RLC_DAG,
    op_args=[conn]
)

# https://airflow.apache.org/concepts.html?highlight=what%20dag#bitshift-composition
TRUNCATE_TABLE >> PULL_RLC
