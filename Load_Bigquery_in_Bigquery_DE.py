from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.connection import Connection
from airflow.operators.postgres_operator import PostgresOperator
import psycopg2
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import duckdb
import gspread
import pygsheets
from datetime import datetime
from datetime import timedelta
import os
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


args={'owner': 'Bob Sebastian'}

# args = {
#     'owner': 'Bob',
#     #'depends_on_past': False,
#     #'start_date': days_ago(2),
#     'email': ['bob@voila.id','bobsebastian1997@gmail.com'],
#     'email_on_failure': True
#     #'email_on_retry': False,
#     #'retries': 1,
#     #'retry_delay': timedelta(minutes=5)
#     # 'queue': 'bash_queue',
#     # 'pool': 'backfill',
#     # 'priority_weight': 10,
#     # 'end_date': datetime(2016, 1, 1),
#     # 'wait_for_downstream': False,
#     # 'dag': dag,
#     # 'sla': timedelta(hours=2),
#     # 'execution_timeout': timedelta(seconds=300),
#     # 'on_failure_callback': some_function,
#     # 'on_success_callback': some_other_function,
#     # 'on_retry_callback': another_function,
#     # 'sla_miss_callback': yet_another_function,
#     # 'trigger_rule': 'all_success'
#         }

with DAG(dag_id="Load_BigqueryTable_Business", start_date=datetime(2022,11,22), 
     schedule_interval="0 2 * * *", catchup=False, default_args = args) as dag: 
     #schedule_interval pake UTC default postgreSQL bukan UTC+7 Jakarta,
     #makanya kalau ngasih schedule_interval harus -7 (contoh "*/10 2-12 * * *" berarti akan jalan di jam 9-20 setiap 10 menit)

    DailyRevenue = BigQueryExecuteQueryOperator(
    task_id="DailyRevenue",
    sql="""
    select date_trunc(cast(InvoiceDate as date), Day) as InvoiceDate,round(Sum(TotalPrice),2) as TotalRevenue
    from `Annual_Sales.artist_revenue`
    group by InvoiceDate 
    order by InvoiceDate
    """,
    destination_dataset_table=f"Annual_Sales.TotalDailyRevenue_1",
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="bigquery_connection",
    use_legacy_sql=False)

    ArtistProductive = BigQueryExecuteQueryOperator(
    task_id="ArtistProductive",
    sql="""
    select ArtistName,GenreName,Count(GenreName) as CountedSongs
    from `Annual_Sales.song`
    group by ArtistName,GenreName 
    order by GenreName asc, Count(GenreName) desc
    """,
    destination_dataset_table=f"Annual_Sales.ArtistProductive_2",
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="bigquery_connection",
    use_legacy_sql=False)

    ArtistRevenue = BigQueryExecuteQueryOperator(
    task_id="ArtistRevenue",
    sql="""
    select ArtistName, round(Sum(TotalPrice),2) as TotalRevenue
    from `Annual_Sales.artist_revenue`
    group by ArtistName
    order by Sum(TotalPrice) desc
    """,
    destination_dataset_table=f"Annual_Sales.TotalArtistRevenue_3",
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="bigquery_connection",
    use_legacy_sql=False)

    FrequencyCity = BigQueryExecuteQueryOperator(
    task_id="FrequencyCity",
    sql="""
    select  Country,count(distinct(InvoiceDate)) as FrequencyLevel
    from `Annual_Sales.transactions`
    where Country is not null
    group by Country
    order by FrequencyLevel desc, Country
    """,
    destination_dataset_table=f"Annual_Sales.FrequencyCity_4",
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="bigquery_connection",
    use_legacy_sql=False)

    DailyRevenue >> ArtistProductive >> ArtistRevenue >> FrequencyCity
