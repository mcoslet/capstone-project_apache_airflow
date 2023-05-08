import glob
import json
import os
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from geopy import Nominatim
from pandas import json_normalize

cities = ["Chisinau", "Orhei"]

# Get Current date, subtract 5 days and convert to timestamp
todayLessFiveDays = datetime.today() - timedelta(days=5)
todayLessFiveDaysTimestamp = time.mktime(todayLessFiveDays.timetuple())
geolocator = Nominatim(user_agent="MyApp")

days = []
for i in range(1, 6):
    todayLessFiveDays = datetime.today() - timedelta(days=i)
    todayLessFiveDaysTimestamp = time.mktime(todayLessFiveDays.timetuple())
    days.append(todayLessFiveDaysTimestamp)

# Get API from airflow var
API_KEY = Variable.get(key='openweathermapApi')
tmp_data_dir = Variable.get(key='tmp_data_dir', default_var='/tmp/out')
weather_data_spark_code = Variable.get("weather_data_spark_code")


# Tmp Data Check
def _tmp_data():
    # Checking if directories exist
    if not os.path.exists(tmp_data_dir):
        os.mkdir(f'{tmp_data_dir}/')
    if not os.path.exists(f'{tmp_data_dir}/weather/'):
        os.mkdir(f'{tmp_data_dir}/weather/')
    if not os.path.exists(f'{tmp_data_dir}/processed/'):
        os.mkdir(f'{tmp_data_dir}/processed/')
    if not os.path.exists(f'{tmp_data_dir}/processed/current_weather/'):
        os.mkdir(f'{tmp_data_dir}/processed/current_weather/')
    if not os.path.exists(f'{tmp_data_dir}/processed/hourly_weather/'):
        os.mkdir(f'{tmp_data_dir}/processed/hourly_weather/')


def _get_weather(**kwords):
    for city in cities:
        location = geolocator.geocode(city)
        url = 'https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}' \
            .format(location[1][0], location[1][1], API_KEY)
        res = requests.get(url)
        data = res.json()
        if data['cod'] == 200:
            time = datetime.today().strftime('%Y%m%d%H%M%S%f')
            with open(f'{tmp_data_dir}/weather/weather_output_{time}.json', 'w') as output_file:
                json.dump(data, output_file)
        else:
            raise AirflowFailException(f'Could not get info from openweather api, code {data["cod"]}')


# Store Location Iterative
def _process_location_csv_iterative():
    for city in cities:
        lat, long = geolocator.geocode(city)[1]
        _store_location_csv(lat, long)


def _store_location_csv(lat, long):
    location = geolocator.reverse(str(lat) + ',' + str(long), addressdetails=True)
    print(location.raw)
    print(type(location))
    address = location.raw['address']

    # Process location data
    location_df = json_normalize({
        'latitude': lat,
        'longitude': long,
        'city': address['city'],
        'postcode': address['postcode'],
        'country': address['country']
    })

    # Store Location
    location_df.to_csv(f'{tmp_data_dir}/location.csv', mode='a', sep=',', index=None, header=False)


# Processed file
def get_current_weather_file():
    for i in glob.glob(f'{tmp_data_dir}processed/current_weather/part-*.csv'):
        return i


def get_hourly_weather_file():
    for i in glob.glob(f'{tmp_data_dir}processed/hourly_weather/part-*.csv'):
        return i


# Dag Skeleton
with DAG(dag_id='weather_data', schedule_interval='@daily', start_date=datetime.today() - timedelta(days=1),
         catchup=False):
    start = EmptyOperator(task_id='start')
    tmp_data = PythonOperator(task_id='tmp_data', python_callable=_tmp_data)
    extracting_weather = PythonOperator(task_id='extracting_weather', python_callable=_get_weather)

    with TaskGroup('create_postgres_tables') as create_postgres_tables:
        # Create table Location
        creating_location_table = PostgresOperator(
            task_id='creating_location_table',
            sql='''
                            CREATE TABLE IF NOT EXISTS location_tmp (
                                latitude VARCHAR(255) NOT NULL,
                                longitude VARCHAR(255) NOT NULL,
                                city VARCHAR(255) NULL,
                                state VARCHAR(255) NULL,
                                postcode VARCHAR(255) NULL,
                                country VARCHAR(255) NULL,
                                PRIMARY KEY (latitude,longitude)
                            );

                            CREATE TABLE IF NOT EXISTS location (
                                latitude VARCHAR(255) NOT NULL,
                                longitude VARCHAR(255) NOT NULL,
                                city VARCHAR(255) NULL,
                                state VARCHAR(255) NULL,
                                postcode VARCHAR(255) NULL,
                                country VARCHAR(255) NULL,
                                PRIMARY KEY (latitude,longitude)
                            );

                            '''
        )
        # Create Table Requested Weather
        creating_table_requested_weather = PostgresOperator(
            task_id='creating_table_requested_weather',
            postgres_conn_id='postgres_default',
            sql='''
                        CREATE TABLE IF NOT EXISTS current_weather_tmp (
                            latitude VARCHAR(255) NOT NULL,
                            longitude VARCHAR(255) NOT NULL,
                            timezone VARCHAR(255) NOT NULL,
                            requested_datetime VARCHAR(255) NULL,
                            sunrise VARCHAR(255) NULL,
                            sunset VARCHAR(255) NULL,
                            temp VARCHAR(255) NULL,
                            feels_like VARCHAR(255) NULL,
                            pressure VARCHAR(255) NULL,
                            humidity VARCHAR(255) NULL,
                            dew_point VARCHAR(255) NULL,
                            uvi VARCHAR(255) NULL,
                            clouds VARCHAR(255) NULL,
                            visibility VARCHAR(255) NULL,
                            wind_speed VARCHAR(255) NULL,
                            wind_deg VARCHAR(255) NULL,
                            weather_id VARCHAR(255) NULL,
                            weather_main VARCHAR(255) NULL,
                            weather_description VARCHAR(255) NULL,
                            weather_icon VARCHAR(255) NULL,
                            PRIMARY KEY (latitude,longitude,requested_datetime)
                        );

                        CREATE TABLE IF NOT EXISTS current_weather (
                            latitude VARCHAR(255) NOT NULL,
                            longitude VARCHAR(255) NOT NULL,
                            timezone VARCHAR(255) NOT NULL,
                            requested_datetime VARCHAR(255) NULL,
                            sunrise VARCHAR(255) NULL, 
                            sunset VARCHAR(255) NULL,
                            temp VARCHAR(255) NULL,
                            feels_like VARCHAR(255) NULL,
                            pressure VARCHAR(255) NULL,
                            humidity VARCHAR(255) NULL,
                            dew_point VARCHAR(255) NULL,
                            uvi VARCHAR(255) NULL,
                            clouds VARCHAR(255) NULL,
                            visibility VARCHAR(255) NULL,
                            wind_speed VARCHAR(255) NULL,
                            wind_deg VARCHAR(255) NULL,
                            weather_id VARCHAR(255) NULL,
                            weather_main VARCHAR(255) NULL,
                            weather_description VARCHAR(255) NULL,
                            weather_icon VARCHAR(255) NULL,
                            PRIMARY KEY (latitude,longitude,requested_datetime)
                );
                '''
        )

        # Create Table Hourly Weather
        creating_table_hourly_weather = PostgresOperator(
            task_id='creating_table_hourly_weather',
            postgres_conn_id='postgres_default',
            sql='''
                        CREATE TABLE IF NOT EXISTS hourly_weather_tmp (
                            latitude VARCHAR(255) NOT NULL,
                            longitude VARCHAR(255) NOT NULL,
                            timezone VARCHAR(255) NOT NULL,
                            datetime VARCHAR(255) NULL,
                            temp VARCHAR(255) NULL,
                            feels_like VARCHAR(255) NULL,
                            pressure VARCHAR(255) NULL,
                            humidity VARCHAR(255) NULL,
                            dew_point VARCHAR(255) NULL,
                            uvi VARCHAR(255) NULL,
                            clouds VARCHAR(255) NULL,
                            visibility VARCHAR(255) NULL,
                            wind_speed VARCHAR(255) NULL,
                            wind_deg VARCHAR(255) NULL,
                            wind_gust VARCHAR(255) NULL,
                            weather_id VARCHAR(255) NULL,
                            weather_main VARCHAR(255) NULL,
                            weather_description VARCHAR(255) NULL,
                            weather_icon VARCHAR(255) NULL,
                            PRIMARY KEY (latitude,longitude,datetime)
                        );

                        CREATE TABLE IF NOT EXISTS hourly_weather (
                            latitude VARCHAR(255) NOT NULL,
                            longitude VARCHAR(255) NOT NULL,
                            timezone VARCHAR(255) NOT NULL,
                            datetime VARCHAR(255) NULL,
                            temp VARCHAR(255) NULL,
                            feels_like VARCHAR(255) NULL,
                            pressure VARCHAR(255) NULL,
                            humidity VARCHAR(255) NULL,
                            dew_point VARCHAR(255) NULL,
                            uvi VARCHAR(255) NULL,
                            clouds VARCHAR(255) NULL,
                            visibility VARCHAR(255) NULL,
                            wind_speed VARCHAR(255) NULL,
                            wind_deg VARCHAR(255) NULL,
                            wind_gust VARCHAR(255) NULL,
                            weather_id VARCHAR(255) NULL,
                            weather_main VARCHAR(255) NULL,
                            weather_description VARCHAR(255) NULL,
                            weather_icon VARCHAR(255) NULL,
                            PRIMARY KEY (latitude,longitude,datetime)
                        );

                        '''
        )

    with TaskGroup('truncate_temp_table_postgres') as truncate_temp_table_postgres:
        # Truncate location_temp Postgres
        truncate_location_temp_postgres = PostgresOperator(
            task_id='truncate_location_temp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                            TRUNCATE TABLE location_tmp;
                        '''
        )

        # Truncate current_weather_temp Postgres
        truncate_current_weather_temp_postgres = PostgresOperator(
            task_id='truncate_current_weather_temp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                            TRUNCATE TABLE current_weather_tmp;
                        '''
        )

        # Truncate hourly_weather_temp Postgres
        truncate_hourly_weather_temp_postgres = PostgresOperator(
            task_id='truncate_hourly_weather_temp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                            TRUNCATE TABLE hourly_weather_tmp;
                        '''
        )
    # Process Location Data
    process_location_csv = PythonOperator(task_id='process_location_csv',
                                          python_callable=_process_location_csv_iterative)

    # Spark Submit
    spark_process_weather = SparkSubmitOperator(
        application=f'{weather_data_spark_code}', task_id="spark_process_weather"
    )

    # TaskGroup for Storing processed data into postgres temp tables
    with TaskGroup('store_processed_temp_data_in_postgres') as store_processed_temp_data_in_postgres:
        store_location_tmp_postgres = PostgresOperator(
            task_id='store_location_tmp_postgres',
            postgres_conn_id='postgres_default',
            sql=f'''
                       COPY location_tmp
                       FROM '{tmp_data_dir}location.csv' 
                       DELIMITER ','
                       ;
                   '''
        )

        store_current_weather_tmp_postgres = PostgresOperator(
            task_id='store_current_weather_tmp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                       COPY current_weather_tmp
                       FROM '%s' 
                       DELIMITER ','
                       ;
                   ''' % get_current_weather_file()
        )

        store_hourly_weather_tmp_postgres = PostgresOperator(
            task_id='store_hourly_weather_tmp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                       COPY hourly_weather_tmp
                       FROM '%s' 
                       DELIMITER ','
                       ;
                   ''' % get_hourly_weather_file()
        )

    # TaskGroup for Storing from temp table to original tables
    with TaskGroup(group_id='copy_from_tmp_table_to_original_table') as copy_from_tmp_table_to_original_table:
        copy_location_tmp_to_location = PostgresOperator(
            task_id='copy_location_tmp_to_location',
            postgres_conn_id='postgres_default',
            sql='''
                            INSERT INTO location 
                            SELECT * 
                            FROM location_tmp
                            EXCEPT
                            SELECT * 
                            FROM location
                            ON CONFLICT (latitude,longitude) DO NOTHING;
                        '''
        )

        copy_current_weather_tmp_to_current_weather = PostgresOperator(
            task_id='copy_current_weather_tmp_to_current_weather',
            postgres_conn_id='postgres_default',
            sql='''
                            INSERT INTO current_weather 
                            SELECT * 
                            FROM current_weather_tmp
                            EXCEPT
                            SELECT * 
                            FROM current_weather
                            ON CONFLICT (latitude,longitude,requested_datetime) DO NOTHING;
                        '''
        )
        copy_hourly_weather_tmp_to_current_weather = PostgresOperator(
            task_id='copy_hourly_weather_tmp_to_current_weather',
            postgres_conn_id='postgres_default',
            sql='''
                            INSERT INTO hourly_weather 
                            SELECT * 
                            FROM hourly_weather_tmp
                            EXCEPT
                            SELECT * 
                            FROM hourly_weather
                            ON CONFLICT (latitude,longitude,datetime) DO NOTHING;
                        '''
        )

    # TaskGroup for Creating Postgres Views
    with TaskGroup('create_materialized_views') as create_materialized_views:
        # Create View for DataSet 1
        create_view_dataset_1 = PostgresOperator(
            task_id='create_view_dataset_1',
            postgres_conn_id='postgres_default',
            sql='''
                   CREATE OR REPLACE VIEW VW_DATASET_1
                   AS
                   SELECT 
                   loc.country AS Country,
                   loc.state AS State,
                   loc.city AS City,
                   CAST(hw.datetime AS DATE) AS Date,
                   EXTRACT(MONTH FROM CAST(hw.datetime AS DATE)) AS Month,
                   MAX(CAST(hw.temp AS DECIMAL)) AS Max_Temperature
                   FROM location loc, hourly_weather hw
                   WHERE ROUND(CAST(loc.latitude AS DECIMAL),4) = ROUND(CAST(hw.latitude AS DECIMAL),4)
                   AND ROUND(CAST(loc.longitude AS DECIMAL),4) = ROUND(CAST(hw.longitude AS DECIMAL),4)
                   GROUP BY City,State,Country,Date,Month
                   ORDER BY Date DESC;
                   '''
        )

        # Create View for DataSet 2
        create_view_dataset_2 = PostgresOperator(
            task_id='create_view_dataset_2',
            postgres_conn_id='postgres_default',
            sql='''
                        CREATE OR REPLACE VIEW  VW_DATASET_2
                        AS
                        SELECT 
                        loc.country AS Country,
                        loc.state AS State,
                        loc.city AS City,
                        CAST(hw.datetime AS DATE) AS Date,
                        MAX(CAST(hw.temp AS DECIMAL)) AS Max_Temperature,
                        MIN(CAST(hw.temp AS DECIMAL)) AS Min_Temperature,
                        ROUND(AVG(CAST(hw.temp AS DECIMAL)),2) AS Average_Temperature
                        FROM location loc, hourly_weather hw
                        WHERE ROUND(CAST(loc.latitude AS DECIMAL),4) = ROUND(CAST(hw.latitude AS DECIMAL),4)
                        AND ROUND(CAST(loc.longitude AS DECIMAL),4) = ROUND(CAST(hw.longitude AS DECIMAL),4)
                        GROUP BY City,State,Country,Date
                        ORDER BY Date DESC;
                        '''
        )

    # Pre Cleanup task
    pre_cleanup = BashOperator(task_id='pre_cleanup', bash_command=f'rm -rf {tmp_data_dir}/')
    # Post Cleanup task
    post_cleanup = BashOperator(task_id='post_cleanup', bash_command=f'rm -rf {tmp_data_dir}/')

    # DAG Dependencies
    start >> pre_cleanup >> tmp_data >> extracting_weather
    extracting_weather >> create_postgres_tables >> truncate_temp_table_postgres >> process_location_csv >> spark_process_weather
    spark_process_weather >> store_processed_temp_data_in_postgres >> copy_from_tmp_table_to_original_table >> create_materialized_views >> post_cleanup
