from datetime import datetime
import psycopg2
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.http_hook import HttpHook

# if task instance raises this, task transition to the Skipped status (for other exceptions, task are retry..)
from airflow.exceptions import AirflowSkipException
import requests
import json


TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


def utc_timestamp(time_str):
    # get rid of 'Z' timezone-spec
    t = time_str[:time_str.index('Z')]
    return datetime.strptime(t, TIME_FORMAT)


def parse_utc_timetamp(trx_dic):
    assert len(trx_dic['type']['operations']
               ) == 1, 'Expected only one operation'
    t_s = trx_dic['type']['operations'][0]['timestamp']
    return utc_timestamp(t_s)


default_args = {
    'owner': 'airflow',
    'start_date': datetime.utcnow(),  # datetime(2019, 4, 23, 20, 00, 00),
    'concurrency': 1,
    'retries': 0
    # 'retry_delay': timedelta(seconds=5)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1)
}

# initial load after sold-out of early adopter!!
INITIAL_LOAD_UTC = "2019-04-22T00:00:00Z"  # "2018-18-12T00:00:00Z"


def last_timestamp_loaded(ops_type):
    pg_hook = PostgresHook(postgres_conn_id='ctd')
    ret = pg_hook.get_first(
        'select max(ops_timestamp) from ods.tz_data where ops_type = %s', parameters=(ops_type,))
    if not ret[0]:
        return utc_timestamp(INITIAL_LOAD_UTC)
    else:
        return ret[0]


PAGE_SIZE = 500


def http_get_tzscan_data(ops_type, page_offset, page_size=PAGE_SIZE):
    """Return list of ops from TZscan
    """
    assert ops_type in ('Transaction', 'Origination',
                        'Delegation', 'Activation', '??')
    api_hook = HttpHook(http_conn_id='http_tzscan', method='GET')
    resp = api_hook.run(endpoint='v3/operations', data=dict(type=ops_type,
                                                            status='Processed', p=page_offset, number=page_size))
    return resp.json()
    # do not need json ... return json.loads(resp.content)


def fetch_latest_trx(ops_type, last_timestamp_inDB):
    """Return list of 'Transactions' created after last_timestamp_inDB
    """
    p_offset = 0
    ops_data = []
    found = False
    while not found:
        n_in_page = 0
        ops_in_page = http_get_tzscan_data(ops_type, page_offset=p_offset)
        for i, t in enumerate(ops_in_page):
            t_timestamp = parse_utc_timetamp(t)
            if i + 1 < len(ops_in_page):
                assert t_timestamp >= parse_utc_timetamp(
                    ops_in_page[i + 1]), 'Expected time-ordered ops'
            if t_timestamp > last_timestamp_inDB:
                n_in_page += 1
                ops_data.append(t)
            else:
                found = True
                break
        p_offset += 1
        print("Scan page-{} (n.Trx={}, oldest.ScanTrx={}, last.InDB={})".format(
            p_offset, n_in_page, t_timestamp, last_timestamp_inDB))
    return trx_data


def load_ops_in_ods(**kwargs):
    ops_type = kwargs['ops_type']
    last_timestamp_inDB = last_timestamp_loaded(ops_type)
    trx_data = fetch_latest_trx(ops_type, last_timestamp_inDB)

    # insert into DB
    ins_sql = """insert into ods.tz_data(ops_hash, ops_type, ops_timestamp, response_json)
                 values (%s, %s, %s, %s);"""
    pg_hook = PostgresHook(postgres_conn_id='ctd')

    for i, t in enumerate(trx_data):
        t_timestamp = utc_timestamp(t['type']['operations'][0]['timestamp'])
        try:
            pg_hook.run(ins_sql, parameters=(t['hash'],
                                             ops_type,
                                             t_timestamp,
                                             json.dumps(t)))
        except psycopg2.Error as e:
            if e.pgcode == '23505':  # unique violation
                print("{}-th trx (hash={}, timestamp={} (last.inDB={}".format(i,
                                                                              t['hash'], t_timestamp, last_timestamp_inDB))
            else:
                raise e


with DAG('load_trx_data',
         catchup=False,
         default_args=default_args,
         # None (manual trigger) or every 2min: '*/2 * * * *',
         schedule_interval='*/2 * * * *') as dag:

    task_load_trx_ods = PythonOperator(task_id='task_load_trx_ods',
                                       python_callable=load_ops_in_ods,
                                       op_kwargs=dict(ops_type='Transaction'))

    task_load_trx_itg = DummyOperator(task_id='task_load_trx_itg')

task_load_trx_ods >> task_load_trx_itg



