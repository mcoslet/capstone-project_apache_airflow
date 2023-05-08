from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class HelloOperator(BaseOperator):
    def __init__(self, conn_id: str = 'postgres_default',
                 database: str = 'airflow', table_name: str = 'table_name', **kwargs):
        super().__init__(**kwargs)
        self.__conn_id = conn_id
        self.__database = database
        self.__table_name = table_name

    def execute(self, context):
        hook = PostgresHook(schema='airflow', postgres_conn_id='postgres_default')
        sql = "select count(*) from table_name"
        result = hook.get_first(sql)
        print(f'result: {result}')
        return result
