import random
from datetime import datetime

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from postgres_sql_count_rows import PostgresSQLCountRows

database = "PostgresSQL"
dags_config = {
    'dag_id_1': {'schedule_interval': "@hourly", "start_date": datetime(2023, 5, 4)},
    'dag_id_2': {'schedule_interval': "@hourly", "start_date": datetime(2023, 5, 4)},
    'dag_id_3': {'schedule_interval': "@hourly", "start_date": datetime(2023, 5, 4)}}

for key in dags_config:
    dag_config = dags_config[key]
    with DAG(
            dag_id=key + "_grid",
            start_date=dag_config['start_date'],
            schedule_interval=dag_config['schedule_interval']
    ) as dag:
        @task(task_id='print_process_start')
        def log_on_start(dag_id: str):
            print(f"{dag_id} start processing tables in database: {database}")


        @task(task_id='log_on_end')
        def log_on_end(**kwargs):
            context = kwargs
            ti = context['ti']
            run_id = context['dag_run'].run_id
            ti.xcom_push(key='log_on_end', value=f"{run_id} ended")


        def _check_table_exist(sql_to_get_schema, sql_to_check_table_exist,
                               table_name):
            """ callable function to get schema name and after that check if table exist """
            hook = PostgresHook()
            # get schema name
            query = hook.get_records(sql=sql_to_get_schema)
            for result in query:
                if 'airflow' in result:
                    schema = result[0]
                    print(schema)
                    break

            # check table exist
            query = hook.get_first(sql=sql_to_check_table_exist.format(schema, table_name))
            print(query)
            if query:
                return 'dummy_task'
            return 'create_table'


        log_process = log_on_start(key)
        get_current_user = BashOperator(task_id='get_current_user', bash_command='echo "$USER"', do_xcom_push=True)
        insert_new_row = PostgresOperator(task_id="insert_new_row", trigger_rule='none_failed', sql=f'''
        INSERT INTO table_name VALUES
        ({random.randint(0, 101)}, 'miko', '{datetime.now()}');
        ''')
        dummy_task = EmptyOperator(task_id="dummy_task")
        create_table = PostgresOperator(task_id="create_table", sql='''
        CREATE TABLE table_name(custom_id integer NOT NULL,
        user_name VARCHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL);
        ''')
        
        query_the_table = PostgresSQLCountRows(task_id='query_the_table', conn_id='postgres_default')

        log_on_end = log_on_end()

        # BranchPythonOperator will decide which task to launch next
        table_name = "table_name"
        branch_table_exist = BranchPythonOperator(
            task_id='check_table_exist',
            python_callable=_check_table_exist,
            op_args=["SELECT * FROM pg_tables;",
                     "SELECT * FROM information_schema.tables "
                     "WHERE table_schema = '{}'"
                     "AND table_name = '{}';", table_name]
        )

        log_process >> get_current_user >> branch_table_exist
        branch_table_exist >> create_table >> insert_new_row
        branch_table_exist >> dummy_task >> insert_new_row
        insert_new_row >> query_the_table >> log_on_end
