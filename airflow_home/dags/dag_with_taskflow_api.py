from typing import List

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from random import uniform

default_args = {
    'owner': 'mcoslet',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


@dag(dag_id="taskflow_api",
     default_args=default_args,
     start_date=datetime(2018, 6, 18),
     schedule_interval="@daily",
     tags=['taskflow'])
def dag_taskflow_api():
    """
    This dag is a example of taskflow api in airflow, it's train 3 models and choose best accuracy
    """
    @task(task_id='trainig_model')
    def _training_model():
        accuracy: float = uniform(0.1, 10)
        print(f'Accuracy: {accuracy}')
        return accuracy

    @task(task_id='choose_model')
    def _choose_model(accuracies: List[float]):
        max_accuracy: float = max(accuracies)
        print(f"Best accuracy from {accuracies} is {max_accuracy}")

    accuracies: List[float] = [_training_model() for _ in ['A', 'B', 'C']]
    _choose_model(accuracies)


start_dag = dag_taskflow_api()
