import datetime
from typing import List

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from random import uniform

default_args = {
    'owner': 'mcoslet',
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=1)
}


def _training_model(ti):
    """
    Mock train model using random to get accuracy
    :param ti: task instance
    :return: float number from 0.1 to 10
    """
    accuracy = uniform(0.1, 10.0)
    print(f'Accuracy: {accuracy}')
    ti.xcom_push(key="accuracy", value=accuracy)


def _choose_model(ti):
    """
    Choose best model based on accuracy list
    :param ti: task instance
    :return: Best model from accuracy list
    """
    accuracy: List[float] = ti.xcom_pull(key='accuracy',
                                         task_ids=['training_model_A', 'training_model_B', 'training_model_C'])
    max_accuracy: float = max(accuracy)
    print(f'Best Accuracy from {accuracy} is {max_accuracy}')


with DAG(
        dag_id="test_xcom",
        default_args=default_args,
        start_date=datetime.datetime(2018, 11, 11),
        schedule="@once",
        doc_md= "Train 3 models and choose best accuracy"
) as dag:
    download_data = BashOperator(task_id="download_data", bash_command="echo Download Data ...")

    train_model = [PythonOperator(task_id=f'training_model_{model}', python_callable=_training_model)
                   for model in ['A', 'B', 'C']]

    choose_model = PythonOperator(task_id="choose_model", python_callable=_choose_model)

    download_data >> train_model >> choose_model
