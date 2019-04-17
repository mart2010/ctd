from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator



def greet():
    print('Writing in file')
    with open('/home/d7loz9/dev/other/ctd/greet.txt', mode='a') as f:
        now = datetime.now()
        t = now.strftime("%Y-%m-%d %H:%M")
        f.write(t + '\n')
    return 'Greeted'

def respond():
    return 'Greet Responded Again'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 4, 15, 13, 00, 00),
    'concurrency': 1,
    'retries': 0
}


# with DAG('my_simple_dag', default_args=default_args, schedule_interval='*/10 * * * *',) as dag:
#     # opr_hello = BashOperator(task_id='say_Hi', bash_command='echo "Hi!!"')
#     opr_greet = PythonOperator(task_id='greet', python_callable=greet)
#     opr_sleep = BashOperator(task_id='sleep_me', bash_command='sleep 5')
#     opr_respond = PythonOperator(task_id='respond', python_callable=respond)

# opr_greet >> opr_sleep >> opr_respond



# def print_hello():
#     return 'Hello world!'

# dag = DAG('hello_world', description='Simple tutorial DAG',
#           schedule_interval='*/2 * * * *',
#           start_date=datetime(2019, 4, 15), catchup=False)

# dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
# hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

# dummy_operator >> hello_operator


