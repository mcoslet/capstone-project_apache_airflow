from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import List, Any, Sequence
from datetime import datetime


def _append_data(rows: List[List[Any]]) -> List[List[Any]]:
    """
    Method what append datetime.now() to row
    :param rows: weather info from openweather
    :return: rows with datetime appended
    """
    for row in rows:
        row.append(datetime.now())
    return rows


class PostgresInsertMultiple(BaseOperator):
    template_fields: Sequence[str] = ("rows",)

    def __init__(self, task_id: str, rows: List[List[Any]], conn_id: str = 'postgres_default',
                 database: str = 'airflow', table_name: str = 'weather', **kwargs):
        super().__init__(**kwargs)
        self.__conn_id = conn_id
        self.__database = database
        self.__table_name = table_name
        self.__rows = _append_data(rows)
        self.__target_fields = ['city', 'humidity', 'pressure', 'wind', 'description', 'temp', 'timestamp']

    def execute(self, context):
        hook = PostgresHook(schema=self.__database, postgres_conn_id=self.__conn_id)
        hook.insert_rows(table=self.__table_name, rows=self.__rows, target_fileds=self.__target_fields)
