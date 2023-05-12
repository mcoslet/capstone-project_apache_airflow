import ssl

import certifi
from datetime import datetime, timedelta
from airflow.models import DAG

from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from geopy import Nominatim
import requests
from slack import WebClient
from slack.errors import SlackApiError

# API key to connect to OpenWeatherApi
API_KEY = Variable.get('openweathermapApi')
default_args = {
    'owner': 'mcoslet',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

# Url to connect to OpenWeatherApi
url = 'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api}&units=metric'

# For what city to get weather info
city = 'Chisinau'


def get_lat_lon(city: str, **kwargs):
    """
    Method what exctract latitude and longitude from a city.
    Need to pass lat and lon to OpenWeatherApi url
    :param city: City to get lat and lon
    :param kwargs: airflow context to transfer data between tasks
    :return: push to xcom a tuple with lat and lon value
    """
    ti = kwargs['ti']
    geolocator = Nominatim(user_agent="get_weather_dag")
    location = geolocator.geocode(city)
    ti.xcom_push(key='lat_lon', value=(location.latitude, location.longitude))


def compile_api_url(api_key, **kwargs):
    """
    Create an url what will extract weather info for a city
    :param api_key: API_KEY from OpenWeatherApi, need to get info from OpenWeatherApi
    :param kwargs: airflow context to transfer data between tasks
    :return: push to xcom url what will extract weather info for a city
    """
    ti = kwargs['ti']
    lat, lon = ti.xcom_pull(key='lat_lon')
    ti.xcom_push(key='compiled_url', value=url.format(lat=lat, lon=lon, api=api_key))


def request_compiled_api_url(**kwargs):
    """
    Make a request to compiled_url to extract weather info
    :param kwargs: airflow context to transfer data between tasks
    :return: dict with weather info for a city
    """
    ti = kwargs['ti']
    requests_url = ti.xcom_pull(key='compiled_url')
    ti.xcom_push(key='data', value=requests.get(requests_url).json())


def check_request(**kwargs):
    """
    Check if request url return HTTP code 200
    :param kwargs: airflow context to transfer data between tasks
    :return: Next step what need to be scheduled
    """
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data')
    if data['cod'] == 200:
        return 'get_weather_info'
    return 'failed_callback'


def get_weather_info(**kwargs):
    """
    Extract a part of information from OpenWeatherApi
    :param kwargs: airflow context to transfer data between tasks
    :return: dict[str, str] with keys: (description, temp, pressure, humidity)
    """
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data')
    ti.xcom_push(key='data_get_weather_info', value=({
        'description': data['weather'][0]['description'],
        'temp': data['main']['temp'],
        'pressure': data['main']['pressure'],
        'humidity': data['main']['humidity']
    }))


def send_msg_slack(**kwargs):
    """
    Send a message to slack chanel TestAirflowWithSlack in #general
    :param kwargs: airflow context to transfer data between tasks
    """
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data_get_weather_info')
    text_template = 'Today was {description} with a temperature of {temp} degrees,' \
                    ' the pressure was {pressure} hPa and the humidity {humidity} %'
    slack_token = Variable.get('slack_token')  # Get From Vault
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    client = WebClient(token=slack_token, ssl=ssl_context)
    try:
        client.chat_postMessage(
            channel="C056Q5EHG9H",
            text=text_template.format(description=data['description'],
                                      temp=data['temp'],
                                      pressure=data['pressure'],
                                      humidity=data['humidity']))
    except SlackApiError as e:
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found')


with DAG(dag_id='get_weather',
         start_date=datetime(2023, 5, 12),
         schedule='@daily',
         default_args=default_args,
         tags=['OpenWeatherApi', 'Slack'],
         doc_md=f'Get weather from OpenWeatherApi and print in slack info about weather in {city}') as dag:
    lat_lon = PythonOperator(task_id=f'get_lat_lon_for_{city}', python_callable=get_lat_lon, op_args=[city])
    compiled_url = PythonOperator(task_id='compile_url', python_callable=compile_api_url, op_args=[API_KEY])
    data_from_api = PythonOperator(task_id='data_from_api', python_callable=request_compiled_api_url)
    check_request_branch = BranchPythonOperator(task_id="check_request_branch", python_callable=check_request)
    failed_callback = BashOperator(task_id='failed_callback', bash_command="echo Unexpected response received, "
                                                                           "More info: {{ti.xcom_pull(key='data')}}",
                                   trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    get_weather_info = PythonOperator(task_id='get_weather_info', python_callable=get_weather_info,
                                      trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    send_msg_slack = PythonOperator(task_id='send_msg_slack', python_callable=send_msg_slack)

    lat_lon >> compiled_url >> data_from_api >> check_request_branch
    check_request_branch >> get_weather_info >> send_msg_slack
    check_request_branch >> failed_callback
