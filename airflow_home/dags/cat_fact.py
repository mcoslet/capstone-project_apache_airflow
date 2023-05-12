from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'mcoslet',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

url = 'https://catfact.ninja/fact'


@dag(dag_id='cat_fact', start_date=datetime(2023, 5, 10), schedule='@daily', default_args=default_args, tags=['taskflow'])
def cat_fact():
    """
    This dag get a cat fact and print this fact on daily basic
    """
    @task
    def get_a_cat_fact():
        """
        Gets a cat fact from the CatFacts API
        """
        return requests.get(url).json()['fact']

    @task
    def print_the_cat_fact(fact: str):
        """
        Print a cat fact from the CatFacts API
        """
        print(f'Cat fact for today: {fact}')

    print_the_cat_fact(get_a_cat_fact())


cat_fact()
