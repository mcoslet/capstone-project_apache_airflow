from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults


class PostgreSQLCountRows(BaseOperator):
    """
    Simple example operator that logs one parameter and returns a string saying hi.
    :param my_parameter: (required) parameter taking any input.
    """
    @apply_defaults
    def __init__(self, conn_id: str = 'postgres_default',
                 database: str = 'airflow', table_name: str = 'table_name', **kwargs):
        super().__init__(**kwargs)
        self.__conn_id = conn_id
        self.__database = database
        self.__table_name = table_name

    def execute(self, context):
        hook = PostgresHook(schema=f'{self.__database}', postgres_conn_id=f'{self.__conn_id}')
        sql = f"select count(*) from {self.__table_name}"
        result = hook.get_first(sql)
        print(f'result: {result}')
        return result
