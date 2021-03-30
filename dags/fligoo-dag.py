from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import DAG
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import codecs
import os
import logging
import psycopg2
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import datetime as dt
import pickle
import urllib3
from urllib3 import request
import certifi
import json
import pandas as pd
import sys
from pandas.io.json import json_normalize

def load_json():
        #get json data and load in a pandas DataFrame
        url='http://api.aviationstack.com/v1/flights?access_key=57e92769d579fc9d6e9201cd7836d732&limit=100&flight_status=active'
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
        r = http.request('GET', url)
        data = json.loads(r.data.decode('utf-8'))
        raw_flights = pd.json_normalize(data, 'data')
        #Save data in the data lake in the bronze layer
        raw_flights.to_csv('./lake/bronze/raw_flights.csv', index = False)  

def transform_data():
        #Read raw Data
        silver_flights = pd.read_csv('./lake/bronze/raw_flights.csv')
        #get only needed columns
        silver_flights=silver_flights[['flight_date','flight_status','departure.airport','departure.timezone','arrival.airport','arrival.timezone','arrival.terminal','airline.name','flight.number']]
        #Replace the "/" of arrival.timezone to ' - ' 
        silver_flights['arrival.timezone'] = silver_flights['arrival.timezone'].str.replace('/',' - ')
        #Replace the "/" of arrival.terminal to ' - '
        silver_flights['arrival.terminal'] = silver_flights['arrival.terminal'].str.replace('/',' - ')
        silver_flights.head(10)
        #Save data in the data lake in the silver layer
        silver_flights.to_csv('./lake/silver/silver_flights.csv', index = False)  

def get_connection():
        c= BaseHook.get_connection('postgres') 

        return c


def execute_insert():
        #Get Connection parameters
        c = get_connection()     
        conn_string = "host="+ c.host +" port="+ str(c.port) +" dbname="+ c.schema +" user=" + c.login  +" password="+ c.password
        #conn_string = "host=192.168.1.236 port=5434 dbname=testfligoo user=testfligoo  password=testfligoo"
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        #Insert flights data      
        sql = "INSERT INTO testdata VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"      
        gold_flights =  pd.read_csv('./lake/silver/silver_flights.csv')
        data = [tuple(x) for x in gold_flights.values]
        cursor.executemany(sql, data)

        #Commit
        conn.commit()

        #Close connection
        conn.close()
        return data      


default_dag_args = {
        'start_date': datetime(2020, 8, 1,8),
        'email': ['gastonfortuny@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': True,
        'project_id' : 'fligoo-challenge',
        'retries': 0,
        'retry_delay': timedelta(minutes=1),
}

with DAG('fligoo-dag',
        schedule_interval = '*/1 * * * *', #excecute at every 1 minute,
        catchup = False,
        default_args=default_dag_args) as dag:

        t_start = DummyOperator(task_id='start')   
        t_load_json = PythonOperator(task_id='load_json', python_callable=load_json, dag=dag)
        t_transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)   
        t_execute_insert = PythonOperator(task_id='execute_insert', python_callable=execute_insert, dag=dag)   
        t_end = DummyOperator(task_id='end')
        t_start >> t_load_json >> t_transform_data >> t_execute_insert  >> t_end    
