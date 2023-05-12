import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime

FILE_PATH = '/opt/airflow/resources/Accidents.csv'


@dag(dag_id='Simple_ETL', start_date=datetime(2023, 5, 10), schedule_interval='@once', tags=['pandas', '.csv'])
def simple_etl():
    """
    Simple ETL using Accidents.csv and pandas
    """
    @task(task_id='extract')
    def _extract(file_path: str):
        """
        Extract data from .csv and convert to DataFrame using pandas
        :param file_path: .csv file location
        :return: DataFrame with .csv data
        """
        if not file_path:
            raise ValueError('file_path cannot be empty')
        df = pd.read_csv(file_path)
        df['Year'] = pd.DatetimeIndex(df['datetime']).year # Adding 'Year' column using datetime column
        return df

    @task(task_id='transform')
    def _transform(df):
        """
        Count all accidents per year
        :param df: DataFrame where to count all accidents per year
        :return: DataFrame with two columns, Year and accidents count
        """
        return df[['Year', 'case_number']].sort_values(by=['Year']).groupby('Year').count()

    @task(task_id='load')
    def _load(df):
        """
        Transform df to str
        :param df: Pandas DataFrame
        :return: df to string
        """
        return df.to_string()

    df = _extract(file_path=FILE_PATH)
    df_grouped = _transform(df)
    df_printed = _load(df_grouped)

    df >> df_grouped >> df_printed


simple_etl()
