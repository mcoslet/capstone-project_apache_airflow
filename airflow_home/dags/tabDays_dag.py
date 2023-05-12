from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mcoslet',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
        dag_id='Weekday',
        default_args=default_args,
        schedule_interval="@once", tags=['BranchPythonOperator'],
        doc_md="Check what today weekday is and trigger today task") as dag:
    # used to factorize the code and avoid repetition
    tabDays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

    # returns the week day (monday, tuesday, etc.)
    def get_day(**kwargs):
        """
        Use datetime.now.weekday to get today weekday 
        :param kwargs: airflow context to push today weekday
        :return: Today weekday
        """
        kwargs['ti'].xcom_push(key='day', value=datetime.now().weekday())


    # PythonOperator will retrieve and store into "weekday" variable the week day
    get_weekday = PythonOperator(
        task_id='weekday',
        python_callable=get_day,
        provide_context=True
    )

    # returns the name id of the task to launch (task_for_monday, task_for_tuesday, etc.)
    def branch(**kwargs):
        return 'task_for_' + tabDays[kwargs['ti'].xcom_pull(task_ids='weekday', key='day')]


    # BranchPythonOperator will use "weekday" variable, and decide which task to launch next
    fork = BranchPythonOperator(
        task_id='branching',
        python_callable=branch,
        provide_context=True
    )

    # task 1, get the week day, and then use branch task
    get_weekday.set_downstream(fork)

    # One dummy operator for each week day, all branched to the fork
    for day in range(0, 6):
        fork.set_downstream(EmptyOperator(task_id="task_for_" + tabDays[day]))
