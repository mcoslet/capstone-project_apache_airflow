import datetime
from typing import List

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from random import uniform
from hello_operator import HelloOperator

default_args = {
    'owner': 'Miko',
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=1)
}


def _training_model(ti):
    accuracy = uniform(0.1, 10.0)
    print(f'Accuracy: {accuracy}')
    ti.xcom_push(key="accuracy", value=accuracy)


def _choose_model(ti):
    accuracy: List[float] = ti.xcom_pull(key='accuracy',
                                         task_ids=['training_model_A', 'training_model_B', 'training_model_C'])
    max_accuracy: float = max(accuracy)
    print(f'Best Accuracy from {accuracy} is {max_accuracy}')


with DAG(
        dag_id="Test_XComs",
        default_args=default_args,
        start_date=datetime.datetime(2018, 11, 11),
        schedule="@once"
) as dag:
    download_data = BashOperator(task_id="download_data", bash_command="echo Download Data ...")

    train_model = [PythonOperator(task_id=f'training_model_{model}', python_callable=_training_model)
                   for model in ['A', 'B', 'C']]

    choose_model = PythonOperator(task_id="choose_model", python_callable=_choose_model)

    hello_task = HelloOperator(task_id="sample-task", conn_id='postgres_default')

    download_data >> train_model >> choose_model >> hello_task
