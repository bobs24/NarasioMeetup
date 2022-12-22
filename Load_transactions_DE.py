import psycopg2
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from datetime import datetime
from datetime import timedelta
import os
from google.cloud import bigquery

args={'owner': 'Bob Sebastian'}

def Load_transactions():
    conn = psycopg2.connect(
    host        = "db",
    database    = "postgres",
    user        = "postgres",
    password    = "postgres",
    port        = '5432')
    cursor = conn.cursor()
    query = '''
        select  cus."CustomerId",tra."TrackId",invl."InvoiceId",
		cus."FirstName"|| ' ' ||replace(cus."LastName",'�','o') as "FullName",
		replace(cus."Address",'�','d') as "Address" ,cus."City" ,
		cus."State" ,cus."Country" ,cus."PostalCode",cus."Email" ,
		cast(inv."Total" as float) as "Total",inv."InvoiceDate" 
        from 
		        "Track" tra 	left join "Album" alb 			on alb."AlbumId" = tra."AlbumId"
						        left join "InvoiceLine" invl 	on tra."TrackId" = invl."TrackId" 
						        left join "Invoice" inv			on invl."InvoiceId" = inv."InvoiceId"
						        left join "Customer" cus		on inv."CustomerId" = cus."CustomerId" 
        where invl."InvoiceId" is not null
        order by cus."CustomerId"
    '''
    cursor.execute(query)
    conn.commit()
    df_query = cursor.fetchall()
    df_query = pd.DataFrame(df_query, columns = ['CustomerId','TrackId','InvoiceId','FullName','Address','City','State',
                                                'Country','PostalCode','Email','Total','InvoiceDate'])
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/voilas-data-aa5f825577f9.json'
    bq_client = bigquery.Client()
    table_id = 'Annual_Sales.transactions'
    job_config = bigquery.LoadJobConfig(schema=[
    bigquery.SchemaField("CustomerId", "INTEGER"),
    bigquery.SchemaField("TrackId", "INTEGER"),
    bigquery.SchemaField("InvoiceId", "INTEGER"),
    bigquery.SchemaField("FullName", "STRING"),
    bigquery.SchemaField("Address", "STRING"),
    bigquery.SchemaField("City", "STRING"),
    bigquery.SchemaField("State", "STRING"),
    bigquery.SchemaField("Country", "STRING"),
    bigquery.SchemaField("PostalCode", "STRING"),
    bigquery.SchemaField("Email", "STRING"),
    bigquery.SchemaField("Total", "FLOAT"),
    bigquery.SchemaField("InvoiceDate", "DATETIME")
    ])
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE #untuk mereplace table, ada 3 sebener e coba cek documentation
    job = bq_client.load_table_from_dataframe(
    df_query, table_id, job_config=job_config
    )
    job.result()
    cursor.close()

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

with DAG(dag_id="Load_Bigquery_transactions", start_date=datetime(2022,12,16), 
     schedule_interval="0 */3 * * *", catchup=False, default_args = args) as dag: 
     #schedule_interval pake UTC default postgreSQL bukan UTC+7 Jakarta,
     #makanya kalau ngasih schedule_interval harus -7 (contoh "*/10 2-12 * * *" berarti akan jalan di jam 9-20 setiap 10 menit)

    Load_bigquery = PythonOperator(
        task_id="Load_table_3",
        python_callable=Load_transactions)    

    Load_bigquery