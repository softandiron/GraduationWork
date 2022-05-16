"""
Developed by Artem Minin
Moscow 2022
"""


from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable

from airflow.operators.python import PythonOperator
import os
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import zipfile
import csv
import logging

import pandahouse as ph
from clickhouse_driver import Client

import config

logging.basicConfig(level=logging.INFO)
client = Client('localhost')

# Clickhouse connection
DB_NAME = config.DB_NAME
HOST = config.HOST
USER = config.HOST
PASSWORD = config.PASSWORD

# AWS S3 buckets
FROM_BUCKET = config.FROM_BUCKET
TO_BUCKET = config.TO_BUCKET


def read_keys():
    """
    Reading the AWS S3 credentials from .csv file
    :return: access and secret keys
    """

    with open('airflow_accessKeys.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        next(reader, None)
        for row in reader:
            access_key = row[0]
            secret_key = row[1]
        logging.info('AWS S3 credentials has been received')
        return access_key, secret_key


def get_files_list_in_bucket(s3):
    """
    Get the current list of the files in the S3 bucket
    :param s3: our AWS S3 client connection object
    :return: the list of the current files
    """
    current_files = []
    logging.info('The list of files in the bucket today: ')
    for key in s3.list_objects(Bucket=FROM_BUCKET)['Contents']:
        current_files.append(key['Key'])
        logging.info(key['Key'])
    return current_files


def search_for_new_files(current_files, yesterday_in_bucket):
    """
    Compare the current list of files in the bucket with our old list to find all new files
    :param current_files: The current list of the files in the S3 bucket
    :param yesterday_in_bucket: The list of the old files stored in Airflow Variable
    :return: the list of the new files
    """
    new_files = []
    logging.info('The list of the new files: ')
    for file in current_files:
        if file not in yesterday_in_bucket:
            new_files.append(file)
            logging.info(file)
    return new_files


def download_new_files_as_csv(s3, new_files):
    """
    Download .zip archives of the new files in the bucket and unzip the .csv with data
    :param s3: our AWS S3 client connection object
    :param new_files: the list of the new files in the bucket
    :return: the list of the new .csv files names
    """

    new_files_csv = []
    for new_file in new_files:
        s3.download_file(Bucket='netobucket',
                         Key=new_file,
                         Filename='archive.zip')
        with zipfile.ZipFile('archive.zip', 'r') as archive:
            archive.extractall('./datafiles')
            list_of_files = archive.namelist()
            if len(list_of_files) > 1:
                logging.warning('There are more than one file in the archive!')
            if len(list_of_files) < 1:
                logging.warning('There are no any files in the archive!')
            csv_file_name = list_of_files[0]
            log_message = 'New file has been downloaded and unzipped: ' + csv_file_name
            logging.info(log_message)
            new_files_csv.append(csv_file_name)
            archive.close()
        os.remove('archive.zip')
    return new_files_csv


def create_clickhouse_table(file_name):
    """
    The function creates new table with daly reports for the current month
    :param file_name: the name of .csv file
    :return: the name of the table
    """
    table_name = 'trips' + file_name[:6]
    request = 'CREATE TABLE tripDB.' + table_name +' (\
    date Date, tripduration Int32, gender Int16)\
    ENGINE = MergeTree ORDER BY date SETTINGS index_granularity = 8192 ;'
    client.execute(request)
    log_message = 'table created with name: ' + table_name
    logging.info(log_message)
    return table_name


def insert_data_to_clickhouse(csv_file, table):
    """
    Upload data to  Clickhouse via pandas Data Frame. Only necessary columns.
    :param csv_file: the name of the initial .csv file
    :param table: the name of the empty Clickhouse table
    :return: nothing
    """
    # create Data Frame from the original .csv
    file_link = './datafiles/' + csv_file
    df = pd.read_csv(file_link)
    logging.info('initial data frame created')

    # create a table for export to Clickhouse
    df['date'] = pd.to_datetime(df['starttime']).dt.floor('d')
    df_for_cklickhouse = df[['date', 'tripduration', 'gender']].copy()
    logging.info('data frame created')

    # insert data to Clickhouse
    connection = dict(database=DB_NAME,
                      host=HOST,
                      user=USER,
                      password=PASSWORD)
    ph.to_clickhouse(df_for_cklickhouse, table, index=False, chunksize=100000, connection=connection)
    message = 'Data Frame loaded to Clickhouse table: ' + table
    logging.info(message)


def SQL_requests(table):
    """
    Getting reports from Clickhouse via SQL request. Gathering output data into pandas DF
    :param table: Clickhouse table name
    :return: pandas DF
    """

    # getting number of trips per days
    trips_count_list = client.execute('SELECT COUNT(date), date \
    FROM tripDB.' + table + ' \
    GROUP BY date;')
    trips_count_df = pd.DataFrame.from_records(trips_count_list, columns=['trips', 'date'])
    logging.info('number of trips found')

    # getting duration of the trips per days
    trips_duration_list = client.execute('SELECT ROUND(AVG(tripduration), 3), date \
    FROM tripDB.' + table + ' \
    GROUP BY date;')
    trips_duration_df = pd.DataFrame.from_records(trips_duration_list, columns=['duration', 'data'])
    logging.info('duration found')

    # getting distribution by genders
    gender_0_list = client.execute('SELECT COUNT(gender), date \
    FROM tripDB.' + table + ' \
    WHERE gender=0 \
    GROUP BY date ;')
    gender_1_list = client.execute('SELECT COUNT(gender), date \
    FROM tripDB.' + table + ' \
    WHERE gender=1 \
    GROUP BY date ;')
    gender_2_list = client.execute('SELECT COUNT(gender), date \
    FROM tripDB.' + table + ' \
    WHERE gender=2 \
    GROUP BY date ;')
    gender_0_df = pd.DataFrame.from_records(gender_0_list, columns=['gender_0', 'data'])
    gender_1_df = pd.DataFrame.from_records(gender_1_list, columns=['gender_1', 'data'])
    gender_2_df = pd.DataFrame.from_records(gender_2_list, columns=['gender_2', 'data'])
    logging.info('gender distribution found')

    # performing calculations of %
    report_df = trips_count_df[['date', 'trips']].copy()
    report_df['duration'] = trips_duration_df['duration'].copy()
    report_df['gender_0_%'] = round(gender_0_df['gender_0'] / (gender_0_df['gender_0'] +
                                                               gender_1_df['gender_1'] +
                                                               gender_2_df['gender_2']) * 100, 2)
    report_df['gender_1_%'] = round(gender_1_df['gender_1'] / (gender_0_df['gender_0'] +
                                                               gender_1_df['gender_1'] +
                                                               gender_2_df['gender_2']) * 100, 2)
    report_df['gender_2_%'] = round(gender_2_df['gender_2'] / (gender_0_df['gender_0'] +
                                                               gender_1_df['gender_1'] +
                                                               gender_2_df['gender_2']) * 100, 2)
    logging.info('Data Frame with report is ready')

    return report_df


def upload_report_to_S3_bucket(s3, report_df, table):
    file_name = './datafiles/report_' + table + '.csv'
    report_df.to_csv(file_name)
    message = 'Report saved locally as ' + file_name
    logging.info(message)

    key = 'report_' + table + '.csv'
    try:
        response = s3.upload_file(file_name, 'netobucketreports', key)
        logging.info('File uploaded to the AWS S3 bucket')
    except ClientError as e:
        logging.warning(e)
    except FileNotFoundError as e:
        logging.warning(e)


def drop_clickhouse_table(table):
    """
    I am not sure if it is needed, but I would prefer to drop the table after it was used
    :param table: the table name
    :return:
    """
    request = 'DROP TABLE tripDB.' + table
    client.execute(request)


def ETL():
    """
    The main function, which calls all necessary functions to perform the ETL process
    :return: nothing
    """
    logging.info('Starting the task')

    # getting keys for our AWS S3 buckets
    access_key, secret_key = read_keys()

    # checking our Airflow Variable to get names of the files we have already aggregated
    yesterday_in_bucket = Variable.get("files_in_bucket")

    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    logging.info('AWS S3 connection established')

    current_files = get_files_list_in_bucket(s3)
    new_files = search_for_new_files(current_files, yesterday_in_bucket)
    new_files_csv = download_new_files_as_csv(s3, new_files)

    # if we have more than one new file do the ETL for each one
    for new_file in new_files_csv:
        table = create_clickhouse_table(new_file)
        insert_data_to_clickhouse(new_file, table)
        report_df = SQL_requests(table)
        upload_report_to_S3_bucket(s3, report_df, table)
        drop_clickhouse_table(table)

    # update the Airflow Variable
    Variable.set('files_in_bucket', value=current_files)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['minin.kp11@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),

}
with DAG(
    dag_id='S3_ETL_v3',
    default_args=default_args,
    description='The DAG for ETL AWS S3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=2,
) as dag:

    etl = PythonOperator(
        task_id='check_bucket_n_download_df',
        python_callable=ETL,
        provide_context=True,
        op_kwargs={"dag_run_id": "{{ run_id }}"},
        dag=dag,
    )