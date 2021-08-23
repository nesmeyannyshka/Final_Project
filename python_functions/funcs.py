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
from pyspark.sql.types import StructType
import pyspark.sql.functions as F

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

    logging.info(f"Reading table {table} from {pg_conn.host}")
    table_df = spark.read.jdbc(pg_url, table=table, properties=pg_properties)

    logging.info(f"Writing table {table} to Bronze")
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
    logging.info("Creating connection parameters")
    gp_conn = BaseHook.get_connection('gp_conn')
    gp_url = f"jdbc:postgresql://{gp_conn.host}:{gp_conn.port}/postgres"
    gp_properties = {"user": gp_conn.login, "password": gp_conn.password}

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
    orders_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'orders'))
    products_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'products'))
    departments_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'departments'))
    aisles_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'aisles'))
    clients_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'clients'))
    stores_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'stores'))
    location_areas_df = spark.read.parquet(os.path.join('/', 'new_datalake', 'silver', now, 'location_areas'))
    
    logging.info("Creating table fact_order")
    fact_order=orders_df\
        .withColumn('order_id', orders_df.order_id.cast('int'))\
        .withColumn('product_id', orders_df.product_id.cast('int'))\
        .withColumn('client_id', orders_df.client_id.cast('int'))\
        .withColumn('store_id', orders_df.store_id.cast('int'))\
        .withColumn('quantity', orders_df.quantity.cast('int'))\
        .withColumn('order_date', orders_df.order_date.cast('date'))\
        .withColumnRenamed('order_date','fc_date')

    logging.info("Creating table fact_oos")
    fact_oos=oos_df\
        .withColumn('product_id', oos_df.product_id.cast('int'))\
        .withColumn('date', oos_df.date.cast('date'))\
        .withColumnRenamed('date','fc_date')

    logging.info("Processing tables products_df, aisles_df, department_df")
    products_df=products_df\
        .withColumn('product_id', products_df.product_id.cast('int'))\
        .withColumn('aisle_id', products_df.aisle_id.cast('int'))\
        .withColumn('department_id', products_df.department_id.cast('int'))

    aisles_df=aisles_df\
        .withColumn('aisle_id', aisles_df.aisle_id.cast('int'))

    departments_df=departments_df\
        .withColumn('department_id', departments_df.department_id.cast('int'))

    logging.info("Creating dimension table dim_products")
    dim_products=products_df\
        .join(aisles_df, on='aisle_id', how='left')\
        .join(departments_df, on='department_id', how='left')\
        .select('product_id', 'product_name', 'aisle','department')

    logging.info("Processing tables stores_df, store_types_df, location_areas_df")
    stores_df=stores_df\
        .withColumn('store_id', stores_df.store_id.cast('int'))\
        .withColumn('store_type_id', stores_df.store_type_id.cast('int'))\
        .withColumn('location_area_id', stores_df.location_area_id.cast('int'))

    store_types_df=store_types_df\
        .withColumn('store_type_id', store_types_df.store_type_id.cast('int'))

    location_areas_df=location_areas_df\
        .withColumn('area_id', location_areas_df.area_id.cast('int'))

    logging.info("Creating dimension table dim_stores")
    dim_stores=stores_df\
        .join(store_types_df, on='store_type_id')\
        .join(location_areas_df, stores_df.location_area_id==location_areas_df.area_id)\
        .select('store_id', 'type', 'area')

    logging.info("Processing table clients_df")
    clients_df=clients_df\
        .withColumn('id', clients_df.id.cast('int'))\
        .withColumn('location_area_id', clients_df.location_area_id.cast('int'))\
        .withColumnRenamed('id','client_id')

    logging.info("Creating dimension table dim_clients")
    dim_clients=clients_df\
        .join(location_areas_df, clients_df.location_area_id==location_areas_df.area_id)\
        .select('client_id', 'fullname', 'area')

    logging.info("Creating dimension table dim_dates")
    dim_dates=fact_oos\
        .select('fc_date')\
        .distinct()\
        .select('fc_date',
                F.date_format('fc_date','d').alias('n_day'),
                F.date_format('fc_date','E').alias('n_week'),
                F.date_format('fc_date','M').alias('n_month'),
                F.date_format('fc_date','yyyy').alias('n_year'))

    logging.info("Download tables in database")
    fact_order.write.option("truncate", "true").jdbc(gp_url, table='fact_order', properties=gp_properties, mode='overwrite')
    fact_oos.write.option("truncate", "true").jdbc(gp_url, table='fact_oos', properties=gp_properties, mode='overwrite')
    dim_products.write.option("truncate", "true").jdbc(gp_url, table='dim_products', properties=gp_properties, mode='overwrite')
    dim_stores.write.option("truncate", "true").jdbc(gp_url, table='dim_stores', properties=gp_properties, mode='overwrite')
    dim_clients.write.option("truncate", "true").jdbc(gp_url, table='dim_clients', properties=gp_properties, mode='overwrite')
    dim_dates.write.option("truncate", "true").jdbc(gp_url, table='dim_dates', properties=gp_properties, mode='overwrite')

    logging.info("Successfully loaded")