"""
Developed by Artem Minin
Moscow 2022
"""
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable

from airflow.operators.python import PythonOperator
import os
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import zipfile
import logging

import pandahouse as ph
from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO)
client = Client('localhost')


def read_config():
    """
    Reading credentials from the config.py
    :return:
    """
    try:
        import config

        # Clickhouse connection
        db_name = config.DB_NAME
        host = config.HOST
        user = config.USER
        password = config.PASSWORD

        # AWS S3 buckets
        from_bucket = config.FROM_BUCKET
        to_bucket = config.TO_BUCKET

        # AWS keys
        access_key = config.ACCESS_KEY
        secret_key = config.SECRET_KEY

        return db_name, host, user, password, from_bucket, to_bucket, access_key, secret_key
    except ImportError:
        logging.warning("config.py import error")


def read_old_files_names_csv():
    """
    Getting the list of files, which where already aggregated, to avoid repetitions.
    :return: the list with files names.
    """
    with open('old_files.csv', 'r') as file_to_read:
        rows = csv.reader(file_to_read)
        old_files_raw = list(rows)  # Can contain 0 or more items
        if len(old_files_raw) > 0:
            old_files = old_files_raw[0]
        else:
            old_files = []
        return old_files


def write_old_files_names_csv(string_to_write):
    """
    Add new names into the csv file with list of names
    :param string_to_write:
    :return: Nothing
    """
    with open('old_files.csv', 'a') as file_to_write:
        final_string_to_write = str(string_to_write) + ','
        file_to_write.write(final_string_to_write)
        logging.info(f'File name was add to old_files.csv as {final_string_to_write}')


def get_files_list_in_bucket(s3, from_bucket):
    """
    Get the current list of files in the S3 bucket.
    :param s3: our AWS S3 client connection object;
    :param from_bucket: the name of the bucket, we download data from ;
    :return: the list of the current files.
    """
    current_files = []
    logging.info('The list of files in the bucket today: ')
    for key in s3.list_objects(Bucket=from_bucket)['Contents']:
        current_files.append(key['Key'])
        logging.info(key['Key'])
    return current_files


def search_for_new_files(current_files):
    """
    Compare the current list of files in the bucket with our old list to find all new files.
    :param current_files: The current list of the files in the S3 bucket.
    :return: the list of the names of the new files.
    """
    old_files = read_old_files_names_csv()
    new_files = []
    logging.info('The list of the new files: ')
    for file in current_files:
        if file not in old_files:
            new_files.append(file)
            logging.info(file)
    return new_files


def download_new_files_as_csv(s3, new_files, from_bucket):
    """
    Download .zip archives of the new files in the bucket and unzip the .csv with data;
    :param s3: our AWS S3 client connection object;
    :param new_files: the list of the new files in the bucket;
    :param from_bucket: the name of bucket we download files from;
    :return: the list of the new .csv files names.
    """

    new_files_csv = []
    for new_file in new_files:
        s3.download_file(Bucket=from_bucket,
                         Key=new_file,
                         Filename='archive.zip')

        # Add the file name to old_files.csv
        write_old_files_names_csv(new_file)

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


def create_clickhouse_table():
    """
    The function creates a new table for reports
    :return: the name of the table.
    """
    table_name = 'trips'  # Change it, if you want
    request = 'CREATE TABLE IF NOT EXISTS tripDB.' + table_name +' (\
               date Date, tripduration Int32, gender Int16)\
               ENGINE = MergeTree ORDER BY date SETTINGS index_granularity = 8192 ;'
    client.execute(request)
    logging.info(f'Clickhouse table created with name: {table_name}')
    return table_name


def get_and_load_clickhouse_reports(db_name, table, host, user, password, s3, to_bucket):
    """
    Search for .csv files in the directory ./datafiles,
    make a dataframe for each file,
    load necessary columns to Clickhouse table,
    get reports by SQL requests,
    upload reports to the target bucket;
    :param db_name: the name of the Clickhouse database;
    :param table: the name of the empty Clickhouse table;
    :param host: BD address, for ex: localhost: 'http://localhost:8123';
    :param user: Clickhouse client user, for ex: 'default';
    :param password: Clickhouse client password, by default: ''  ;
    :param s3: boto3 client for AWS S3 connection;
    :param to_bucket: the name of the target bucket, where the final reports will store;
    :return: Success message.
    """
    for filename in os.listdir('./datafiles'):
        if filename.endswith('.csv'):
            file_link = os.path.join('./datafiles', filename)
            report_month = filename[:7]
            logging.info(f'Found a file for month: {report_month}')
            # create Data Frame from the original .csv
            df = pd.read_csv(file_link)
            logging.info('initial data frame created')

            # create a table for export to Clickhouse
            df['date'] = pd.to_datetime(df['starttime']).dt.floor('d')
            df_for_cklickhouse = df[['date', 'tripduration', 'gender']].copy()
            logging.info('data frame created')

            # insert data to Clickhouse
            connection = dict(database=db_name,
                              host=host,
                              user=user,
                              password=password)
            ph.to_clickhouse(df_for_cklickhouse, table, index=False, chunksize=100000, connection=connection)
            message = 'Data Frame loaded to Clickhouse table: ' + table
            logging.info(message)

            report_df = SQL_requests(table)
            upload_report_to_S3_bucket(s3, report_df, report_month, to_bucket)

            # Remove the aggregated file
            os.remove('./datafiles/' + filename)

    return 'Success!!'


def SQL_requests(table):
    """
    Getting reports from Clickhouse via SQL request. Gathering output data into pandas DF.
    After receiving reports, the table clears using TRUNCATE.
    :param table: Clickhouse table name;
    :return: pandas DF.
    """
    # getting number of trips per days
    trips_count_list = client.execute(
        'SELECT COUNT(date), date \
        FROM tripDB.' + table + ' \
        GROUP BY date;')
    trips_count_df = pd.DataFrame.from_records(trips_count_list, columns=['trips', 'date'])
    logging.info('number of trips found')

    # getting duration of the trips per days
    trips_duration_list = client.execute(
        'SELECT ROUND(AVG(tripduration), 3), date \
        FROM tripDB.' + table + ' \
        GROUP BY date;')
    trips_duration_df = pd.DataFrame.from_records(trips_duration_list, columns=['duration', 'data'])
    logging.info('duration found')

    # getting distribution by genders
    gender_0_list = client.execute(
        'SELECT COUNT(gender), date \
        FROM tripDB.' + table + ' \
        WHERE gender=0 \
        GROUP BY date ;')
    gender_1_list = client.execute(
        'SELECT COUNT(gender), date \
        FROM tripDB.' + table + ' \
        WHERE gender=1 \
        GROUP BY date ;')
    gender_2_list = client.execute(
        'SELECT COUNT(gender), date \
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

    # clear the table
    client.execute(
        'TRUNCATE TABLE IF EXISTS tripDB.' + table + ';'
    )

    return report_df


def upload_report_to_S3_bucket(s3, report_df, report_month, to_bucket):
    """
    Saves the Pandas DataFrame as .CSV file and upload the file into the target AWS S3 bucket.
    :param s3: boto3 client for AWS S3 connection;
    :param report_df: the DataFrame, we want to save into .CSV;
    :param report_month: the part of the file's name;
    :param to_bucket: the name of the target bucket, where the final reports will store;
    :return: Nothing
    """
    file_name = './reports/report_' + report_month + '.csv'
    report_df.to_csv(file_name)
    message = 'Report saved locally as ' + file_name
    logging.info(message)

    key = 'report_' + report_month + '.csv'
    try:
        response = s3.upload_file(file_name, to_bucket, key)
        logging.info(f'File uploaded to the AWS S3 bucket with response: {response}')
    except ClientError as e:
        logging.warning(e)
    except FileNotFoundError as e:
        logging.warning(e)


def collect_new_files():
    """
    The first of two main functions. Aimed to perform the first part of the ETL process:
    to download and unzip all the new files from the first bucket.
    :return: the list of .CSV file. Actually not needed.
    """
    logging.info('Starting the task')

    # getting keys for our AWS S3 buckets
    db_name, host, user, password, from_bucket, to_bucket, access_key, secret_key = read_config()

    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    logging.info('AWS S3 connection established')

    current_files = get_files_list_in_bucket(s3, from_bucket)
    logging.info(f'Current files in bucket: {current_files}')
    new_files = search_for_new_files(current_files)
    new_files_csv = download_new_files_as_csv(s3, new_files, from_bucket)

    return new_files_csv


def aggregate_and_upload_csv():
    """
    The second main part of the ELT process. Aggregate each downloaded .CSV file, upload them into Clickhouse table,
    prepare reports, and load reports as .CSV files into the second AWS S3 bucket.
    :return: Nothing
    """
    # getting keys for our AWS S3 buckets
    db_name, host, user, password, from_bucket, to_bucket, access_key, secret_key = read_config()
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    logging.info('AWS S3 connection established')

    table_name = create_clickhouse_table()
    get_and_load_clickhouse_reports(db_name=db_name,
                                    table=table_name,
                                    host=host,
                                    user=user,
                                    password=password,
                                    s3=s3,
                                    to_bucket=to_bucket)


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

    download_files = PythonOperator(
        task_id='download_files',
        python_callable=collect_new_files,
        # provide_context=True,
        op_kwargs={"dag_run_id": "{{ run_id }}"},
        dag=dag,
    )

    get_and_load_reports = PythonOperator(
        task_id='get_and_load_reports',
        # python_callable=aggregate_and_upload_csv,
        python_callable=aggregate_and_upload_csv,
        # provide_context=True,
        op_kwargs={"dag_run_id": "{{ run_id }}"},
        dag=dag,
    )

    download_files >> get_and_load_reports

