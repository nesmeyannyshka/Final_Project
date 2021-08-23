import psycopg2
import logging
import os
import requests
import json

from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook

from datetime import datetime

import pyspark
from pyspark.sql import SparkSession

#Postgres functions

def load_to_bronze_spark(table):

    logging.info("Creating connection parameters")
    pg_conn = BaseHook.get_connection('oltp_dshop')
    pg_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/dshop_bu"
    pg_properties = {"user": pg_conn.login, "password": pg_conn.password}
    now = datetime.now().date().strftime("%Y-%m-%d")

    logging.info("Building Spark Session")
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.2.23.jar') \
        .master('local') \
        .appName('load_to_bronze') \
        .getOrCreate()

    logging.info(f"Writing table {table} from {pg_conn.host} to Bronze")
    table_df = spark.read.jdbc(pg_url, table=table, properties=pg_properties)
    table_df.write.parquet(
        os.path.join('/', 'new_datalake', 'bronze', now, table),
        mode="overwrite")

    logging.info("Successfully loaded")

def load_to_silver_spark(table):

    logging.info("Building Spark Session")
    now = datetime.now().date().strftime("%Y-%m-%d")
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.2.23.jar') \
        .master('local') \
        .appName('load_to_silver') \
        .getOrCreate()

    logging.info(f"Reading table {table} from Bronze")
    table_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'bronze', now, table))

    logging.info(f"Clean and drop duplicates on table {table}")
    table_df=table_df.dropDuplicates()

    logging.info(f"Writing table {table} to Silver")
    table_df.write.parquet(os.path.join('/', 'new_datalake','silver',now, table), mode='overwrite')

    logging.info("Successfully loaded")

#API functions

def api_to_bronze():
    logging.info("Creating connection parameters")
    hdfs_conn = BaseHook.get_connection('datalake_hdfs')
    now = datetime.now().date().strftime("%Y-%m-%d")
    client = InsecureClient(f"http://{hdfs_conn.host}:{hdfs_conn.port}/", user=hdfs_conn.login)
    directory = os.path.join('/', f'new_datalake', f'bronze', f'{now}')

    logging.info("Login on portal")
    api_conn = BaseHook.get_connection('http_api')
    post_params = {"username": api_conn.login, "password": api_conn.password}
    response_post = requests.post(api_conn.host+'/auth', json=post_params,timeout=5)

    logging.info("Use authorization key")
    key = response_post.json()
    params = {"date": now}
    headers = {'Authorization': 'JWT ' + key['access_token']}

    logging.info(f"Writing API on {now} to Bronze")
    response_get = requests.get(api_conn.host+'/out_of_stock', params=params, headers=headers, timeout=5)
    client.write(os.path.join(directory, f'out_of_stock_{now}.json'), data=json.dumps(response_get.json()),
                 encoding='utf-8', overwrite=True)

    logging.info("Successfully loaded")

def api_to_silver():
    logging.info("Building Spark Session")
    now = datetime.now().date().strftime("%Y-%m-%d")
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.2.23.jar') \
        .master('local') \
        .appName('load_to_silver') \
        .getOrCreate()

    logging.info(f"Reading API data from Bronze")
    oos = spark.read.format('json')\
        .load(os.path.join('/', f'new_datalake', f'bronze', f'{now}', f'out_of_stock_{now}.json'))

    logging.info(f"Clean and drop duplicates")
    oos=oos.dropDuplicates()

    logging.info(f"Writing API data to Silver")
    oos.write.parquet(os.path.join('/', 'new_datalake','silver',now, f'out_of_stock_{now}'), mode='overwrite')

    logging.info("Successfully loaded")

# Load to DWH
def dwh():
    logging.info("Building Spark Session")
    now = datetime.now().date().strftime("%Y-%m-%d")
    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.2.23.jar') \
        .master('local') \
        .appName('load_to_dwh') \
        .getOrCreate()

    logging.info(f"Reading data from Silver")
    oos_df = spark.read.parquet(os.path.join('/', 'new_datalake','silver',now, f'out_of_stock_{now}'))
    store_types_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'store_types'))
    '''
    orders_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'orders'))
    products_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'products'))
    departments_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'departments'))
    aisles_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'aisles'))
    clients_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'clients'))
    stores_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'stores'))
    location_areas_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'location_areas'))
    '''

    logging.info("Creating connection parameters")
    gp_conn = BaseHook.get_connection('gp_conn')
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/postgres"
    gp_properties = {"user": gp_conn.login, "password": gp_conn.password}

    oos_df.write.jdbc(gp_url, table='fact_oos', properties=gp_properties, mode='overwrite')
    store_types_df.write.jdbc(gp_url, table='dim_stores', properties=gp_properties, mode='overwrite')
    '''
    logging.info(f"Clean and drop duplicates")
    oos = oos.dropDuplicates()

    logging.info(f"Writing API data to Silver")
    oos.write.parquet(os.path.join('/', 'new_datalake', 'silver', now, f'out_of_stock_{now}'), mode='overwrite')

    logging.info("Successfully loaded") '''