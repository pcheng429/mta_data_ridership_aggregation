import configparser 
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.commons.data_io import download_csv_data_from_api
from etl.process_mta_hourly_ridership import process_mta_hourly_ridership
from etl.process_mta_wif_location import process_mta_wifi_location
from etl.aggregate_to_daily_ridership_with_provider_historical_info import aggregate_data

default_args = {
    'owner': 'Peng Cheng',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
 
# Load external config 
config = configparser.ConfigParser()
config.read("dags/config/config.ini")
default_config = config["default"]

with DAG(
    default_args=default_args,
    dag_id='aggregate_ridership_and_wifi_location_v01',
    description='This is the dag to ingest two open data sources(hourly ridership and wifi location) from MTA and aggregate them',
) as dag:
    ingest_ridership = PythonOperator(
        task_id='ingest_mta_hourly_ridership_data',
        python_callable=download_csv_data_from_api,
        op_kwargs={"api_url":default_config["MTA_HOURLY_RIDERSHIP_SOURCE_API"],
                   "data_name":default_config["MTA_HOURLY_RIDERSHIP_RAW_DATA_NAME"],
                   "dir_path":default_config["RAW_DATA_DIR"]
                   }
    )

    ingest_wifi_location = PythonOperator(
        task_id='ingest_mta_wifi_location_data',
        python_callable=download_csv_data_from_api,
        op_kwargs={
            "api_url":default_config["MTA_WIFI_LOCATION_SOURCE_API"],
            "data_name":default_config["MTA_WIFI_LOCATION_RAW_DATA_NAME"],
            "dir_path":default_config["RAW_DATA_DIR"]
            }
    )

    process_ridership = PythonOperator(
        task_id='process_mta_ridership_data',
        python_callable=process_mta_hourly_ridership,
        op_kwargs={
            "dir_path":default_config["PROCESSED_DATA_DIR"],
            "new_data_name":default_config["MTA_RIDERSHIP_PROCESSED_DATA_NAME"]
        }
    )

    process_wifi_location = PythonOperator(
        task_id='process_mta_wifi_location_data',
        python_callable=process_mta_wifi_location,
        op_kwargs={
            "dir_path":default_config["PROCESSED_DATA_DIR"],
            "new_data_name":default_config["MTA_PROVIDER_HISTORICAL_PROCESSED_DATA_NAME"]
        }
    )

    aggregate_mta_data = PythonOperator(
        task_id='aggregate_mta_ridership_provider_historical_info',
        python_callable=aggregate_data,
        op_kwargs={
            "dir_path":default_config["AGGREAGATED_DATA_DIR"],
            "new_data_name":default_config["AGGREAGATED_DATA_NAME"]
        }
    )

    # step 1: ingest two datasets 
    # step 2: pre-process both of them in paralellism 
    ingest_ridership >> process_ridership
    ingest_wifi_location >> process_wifi_location

    # Step3: aggregate two datasets into daily ridership data 
    [process_ridership, process_wifi_location] >> aggregate_mta_data
