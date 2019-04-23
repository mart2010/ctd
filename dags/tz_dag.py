from datetime import datetime
import psycopg2 
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.http_hook import HttpHook

# if task instance raises this, task transition to the Skipped status (for other exceptions, task are retry..)
from airflow.exceptions import AirflowSkipException
import requests
import json


TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

# for now, just get rid of 'Z' timezone-spec
def get_utc_timestamp(time_str):
    t = time_str[:time_str.index('Z')]
    return datetime.strptime(t, TIME_FORMAT)


def parse_utc_timetamp(trx_dic):
    assert len(trx_dic['type']['operations']) == 1, 'Expected only one operation'
    t_s = trx_dic['type']['operations'][0]['timestamp']
    return get_utc_timestamp(t_s)


INITIAL_LOAD_UTC = "2019-04-23T20:20:00Z"

def latest_loaded(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='ctd')
    ret = pg_hook.get_first('select max(ops_timestamp) from ods.tz_data;')
    if not ret[0]:
        return get_utc_timestamp(INITIAL_LOAD_UTC)
    else:
        return ret[0]

# will return a list, like Check out why ops is a lit, could there be more than one for an ops hash?
t = [{'block_hash': 'BMUYC8xHkXh27uVohFJpW88itRzqoCQhqbmRuyb1VPLYzFScqnw',
             'hash': 'opWg4UdvHhLWjBWtiWnHo7TnK9U8ScceBMACp1emnqJ8howEBbR',
             'network_hash': 'NetXdQprcVkpaWU',
             'type': {'kind': 'manager',
                      'operations': [{'amount': 1250000000,
                                      'burn': 0,
                                      'counter': 965461,
                                      'destination': {'tz': 'tz1KtGwriE7VuLwT3LwuvU9Nv4wAxP7XZ57d'},
                                      'failed': False,
                                      'fee': 1420,
                                      'gas_limit': '10300',
                                      'internal': False,
                                      'kind': 'transaction',
                                      'op_level': 398182,
                                      'src': {'tz': 'tz1RnP77bMxzvcX6xARWuFF2hvbh4TiPkgft'},
                                      'storage_limit': '277',
                                      'timestamp': '2019-04-16T14:38:24Z'}],
                      'source': {'tz': 'tz1RnP77bMxzvcX6xARWuFF2hvbh4TiPkgft'}}}]



PAGE_SIZE = 20

def get_tzapi_data(ops_type, page_offset, page_size=PAGE_SIZE):
    """Return list of ops from TZscan
    """
    assert ops_type in ('Transaction', 'Origination', 'Delegation', 'Activation', '??')
    api_hook = HttpHook(http_conn_id='http_tzscan', method='GET')
    resp = api_hook.run(endpoint='v3/operations', data=dict(type=ops_type, status='Processed', p=page_offset, number=page_size))
    return resp.json()
    # do not need json ... return json.loads(resp.content)


def fetch_latest_data(**kwargs):
    ti = kwargs['ti']
    oldest_inDB = ti.xcom_pull(key=None, task_ids='latest_loaded')

    p_offset = 0
    trx_data = []
    found = False
    while not found:
        trx_in_page = get_tzapi_data(ops_type="Transaction", page_offset=p_offset)
        for i, t in enumerate(trx_in_page):
            t_timestamp = parse_utc_timetamp(t)
            if i+1 < len(trx_in_page):
                assert t_timestamp >= parse_utc_timetamp(trx_in_page[i+1]), 'Expected time-ordered trx'
            if t_timestamp >= oldest_inDB:
                trx_data.append(t)
            else:
                found = True
                break
        p_offset += 1
        print("Scan page-{} (n.Trx={}, oldestScanTrx={}, oldestinDB={})".format(p_offset, len(trx_data), t_timestamp, oldest_inDB))
    return trx_data


def load_trx_data(**kwargs):
    ti = kwargs['ti']
    trx_data = ti.xcom_pull(key=None, task_ids='fetch_latest_data')

    # insert into DB
    ins_sql = """insert into ods.tz_data(ops_hash, ops_kind, op_level, ops_timestamp, response_json)
                  values (%s, %s, %s, %s, %s);"""
    pg_hook = PostgresHook(postgres_conn_id='ctd')

    for i, t in enumerate(trx_data):
        t_timestamp = get_utc_timestamp(t['type']['operations'][0]['timestamp'])
        try:
            pg_hook.run(ins_sql,parameters=(t['hash'], 
                                            t['type']['operations'][0]['kind'],
                                            t['type']['operations'][0]['op_level'],
                                            t_timestamp,
                                            json.dumps(t)))
        except psycopg2.Error as e:
            if e.pgcode == '23505':    #unique violation
                latest_inDB = ti.xcom_pull(key=None, task_ids='latest_loaded')
                print("{}-th trx (hash={}, timestamp={} already in DB (latest-inDB={}".format(i, t['hash'], t_timestamp, latest_inDB))
            else:
                raise e


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 4, 23, 20, 00, 00),
    'concurrency': 1,
    'retries': 0
    # 'retry_delay': timedelta(seconds=5)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1)
}


with DAG('load_tz_data', catchup=False,
         default_args=default_args,
         schedule_interval='*/2 * * * *',) as dag:

    task_latest_loaded = PythonOperator(task_id='latest_loaded',
                                        python_callable=latest_loaded)

    task_fetch_latest_data = PythonOperator(task_id='fetch_latest_data',
                                            python_callable=fetch_latest_data,
                                            provide_context=True)

    task_load_trx_data = PythonOperator(task_id='load_trx_data',
                                        python_callable=load_trx_data,
                                        provide_context=True)


task_latest_loaded >> task_fetch_latest_data >> task_load_trx_data
