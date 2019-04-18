from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.postgres_operator import PostgresOperator
# if task instance raises this, task transition to the Skipped status (for other exceptions, task are retry..)
from airflow.exceptions import AirflowSkipException
import requests
import elt


def http_get_data(url, as_json=True, **kwargs):
	""" Generic HTTP GET call with params kwargs converted to well formed query string
	Return data in json or text
	"""   
	try:
		r = requests.get(url, params=kwargs)
		if as_json:
			return r.json()
		else:
			return r.text
	except requests.exceptions.RequestException as e:
		print(e)
		raise e


INITIAL_BLOCK_LEVEL = 398179

def last_blockloaded(**kwargs):
	return INITIAL_BLOCK_LEVEL

	db_conn = elt.get_ro_connection()
	ret = db_conn.fetch_one('select max(op_level) from ods.load_tz_data')
	if not ret:
		return INITIAL_BLOCK_LEVEL
	else:
		return ret[0]


# will return a list, like Check out why ops is a lit, could there be more than one for an ops hash?

RET_PAGE_SIZE = 20

def fetch_ops_data(ops_type, page_no):
	"""Return list of dict element   
	"""
	return [ {'block_hash': 'BMUYC8xHkXh27uVohFJpW88itRzqoCQhqbmRuyb1VPLYzFScqnw',
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
								'op_level': 398182-page_no,
								'src': {'tz': 'tz1RnP77bMxzvcX6xARWuFF2hvbh4TiPkgft'},
								'storage_limit': '277',
								'timestamp': '2019-04-16T14:38:24Z'}],
				'source': {'tz': 'tz1RnP77bMxzvcX6xARWuFF2hvbh4TiPkgft'}}}]

	assert ops_type in ('Transaction',)
	page_no = page_no-1
	# fetch from TZscan
	return http_get_data('https://api6.tzscan.io/v3/operations', type=ops_type, p=page_no, number=RET_PAGE_SIZE)


def check_and_get_minblock(trx_list):
	min_op_level = trx_list[0]['type']['operations'][0]['op_level']
	for t in trx_list:
		assert len(t['type']['operations']) == 1
		if t['type']['operations'][0]['op_level'] < min_op_level:
			min_op_level = t['type']['operations'][0]['op_level']
	return min_op_level


def fetch_latest_data(**kwargs):
	ti = kwargs['ti']
	last_loaded_inDB = ti.xcom_pull(key=None, task_id='last_blockloaded')

	already_loaded = False
	no = 1
	trx_data = []
	while not already_loaded:
		t_l = fetch_ops_data(ops_type="Transaction", page_no=no)
		trx_data += t_l
		no += 1
		min_block_inPage = check_and_get_minblock(t_l)
		if last_loaded_inDB > min_block_inPage:
			already_loaded = True
		else:
			print("Must request page %s, increase 'RET_PAGE_SIZE to limit number of HTTP requests" \
				  "(min block in page=%s, last block in DB=%s)".format(no, min_block_inPage, last_loaded_inDB ))
	return trx_data


def load_trx_data(**kwargs):
	sql = """"insert into ods.tz_data(ops_hash, ops_kind, op_level, ops_timestamp)
			  values (%(hash)s, %(kind)s, %(level)s, %(tstmp)s)"""

	ti = kwargs['ti']
	trx_data = ti.xcom_pull(key=None, task_id='fetch_latest_data')

	db_conn = elt.get_connection()
	# insert into ps DB, and ignore duplicate hash error (expecting a few, maybe report them) 
	for t in trx_data:
		try:
			db_conn.execute(sql, params={'hash': t['hash'], 
										 'kind': t['operations'][0]['kind'],
										 'level': t['operations'][0]['op_level'],
										 'tstmp': t['operations'][0]['timestamp']})
		except "UniqueKeyViolation" as e:
			print("ignore trx loaded previously")
	db_conn.commit()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 4, 15, 13, 00, 00),
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

	task_last_blockloaded = PythonOperator(task_id='last_blockloaded', 
										   python_callable=last_blockloaded)

	task_fetch_latest_data = PythonOperator(task_id='fetch_latest_data',
											python_callable=fetch_latest_data,
											provide_context=True)
	task_load_trx_data = PythonOperator(task_id='load_trx_data',
										python_callable=load_trx_data,
										provide_context=True)


task_last_blockloaded >> task_fetch_latest_data >> task_load_trx_data



