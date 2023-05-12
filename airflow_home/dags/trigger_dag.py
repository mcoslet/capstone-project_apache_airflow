from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.utils.db import provide_session
from airflow.models.dag import get_last_dagrun
from astronomer.providers.core.sensors.filesystem import FileSensorAsync

FILE_PATH = Variable.get("FILE_PATH", default_var='/tmp')

with DAG(
        dag_id='trigger_dag',
        start_date=datetime(2023, 5, 1),
        schedule_interval='@hourly'
) as dag:
    waiting_for_file = FileSensorAsync(task_id='waiting_for_file',
                                       filepath=f'{FILE_PATH}/test.txt',
                                       poke_interval=30,
                                       timeout=60 * 2,
                                       mode="poke"
                                       )

    trigger_dag = [TriggerDagRunOperator(task_id=f'trigger_{dag_id}',
                                         trigger_dag_id=f'{dag_id}_grid')
                   for dag_id in ['dag_id_1', 'dag_id_2', 'dag_id_3']]

    # SubDAGs are a legacy Airflow feature that allowed the creation of reusable task patterns in DAGs.
    # SubDAGs caused performance and functional issues, and they were deprecated Airflow 2.0.
    with TaskGroup(group_id='task_group') as task_group:
        def _get_execution_date_of(dag_id):
            @provide_session
            def _get_last_execution_date(exec_date, session=None, **kwargs):
                dag_a_last_run = get_last_dagrun(dag_id, session)
                return dag_a_last_run.execution_date

            return _get_last_execution_date


        wait_for_parent_dag = ExternalTaskSensor(
            task_id="wait_for_dag",
            external_dag_id='dag_id_1_grid',
            external_task_id=None,
            execution_date_fn=_get_execution_date_of('dag_id_1_grid'),
            timeout=60 * 2,
            mode="poke",
            check_existence=True
        )


        def _print_result(**context):
            ti = context['ti']
            log = ti.xcom_pull(key='log_on_end')
            print(log)
            for key in context:
                print(f'{key}: {context[key]}')


        print_result = PythonOperator(task_id='print_result',
                                      provide_context=True,
                                      python_callable=_print_result)

        remove_file = BashOperator(task_id='remove_file', bash_command=f'rm -rf {FILE_PATH}/test.txt')

        create_timestamp = BashOperator(task_id='create_timestamp',
                                        bash_command=f'touch {FILE_PATH}/finished_$ts_nodash_env',
                                        env={'ts_nodash_env': '{{ts_nodash}}'})

        wait_for_parent_dag >> print_result >> remove_file >> create_timestamp

    waiting_for_file >> trigger_dag >> task_group
