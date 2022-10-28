from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import urllib.request as rq
import pandas as pd
import json
from subprocess import Popen
from google.cloud import bigquery

#libreoffice_executable = 'C:/Program Files/LibreOffice/program/soffice.exe'
libreoffice_executable = 'libreoffice'
svc_bigquery = json.loads(Variable.get('svc_bigquery'))

def download_file():
        url = 'https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls'
        response = rq.urlretrieve(url, 'vendas-combustiveis-m3.xls')        

def converter_xls():
    p = Popen([libreoffice_executable, '--headless', '--convert-to', 'xls', '--outdir','convert', 'vendas-combustiveis-m3.xls'])
    p.communicate()


def transform_data(sheet_name,table_name):
    df1 = pd.read_excel('./convert/vendas-combustiveis-m3.xls', sheet_name=sheet_name)
    df1.drop(columns=['TOTAL','REGIÃO'],inplace=True)
    df1.columns = ['product','ano','uf','1','2','3','4','5','6','7','8','9','10','11','12']
    new_df = df1.melt(id_vars=['product','ano','uf'],var_name='month',value_name="volume")
    new_df['year_month'] = new_df['ano'].map(str) + '-' + new_df['month'].map(str)
    new_df['year_month'] = pd.to_datetime(new_df['year_month'],format='%Y-%m')
    new_df.drop(columns=['ano','month'],inplace=True)
    new_df['volume'] = new_df['volume'].astype(float)
    new_df.fillna(0,inplace=True)
    new_df['unit'] = 'm3'
    new_df['created_at'] = datetime.now()   
    new_df.to_csv(f'{table_name}.csv',index=False)

def load_dataframe(table_name):
    table_id = f'anp.{table_name}'
    dataframe = pd.read_csv(f"{table_name}.csv")
    client = bigquery.Client.from_service_account_info(svc_bigquery)
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("product", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField("uf", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField("volume", field_type="FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("year_month", field_type="DATE", mode="NULLABLE"),
        bigquery.SchemaField("unit", field_type="STRING", mode="NULLABLE"),
        bigquery.SchemaField("created_at", field_type="TIMESTAMP", mode="NULLABLE"),
    ],
    autodetect=False,
    source_format=bigquery.SourceFormat.CSV,
    write_disposition="WRITE_TRUNCATE",)
    print("start_load_dataframe")
    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


with DAG(
    "ETL_ANP",
    schedule_interval=None,
    start_date=datetime(2022,10,28),
    max_active_runs=1,
    catchup=False
    ) as dag:
    
    download = PythonOperator(
        task_id = 'download_file',
        python_callable=download_file
    )

    convert = PythonOperator(
        task_id = 'converter_xls',
        python_callable=converter_xls
    )

    transform_fuel = PythonOperator(
        task_id = 'transform_fuel',
        python_callable=transform_data,
        op_kwargs={'sheet_name': 1, 'table_name': 'fuels'}
    )

    transform_diesel = PythonOperator(
        task_id = 'transform_diesel',
        python_callable=transform_data,
        op_kwargs={'sheet_name': 2, 'table_name': 'diesel'}
    )


    load_fuel = PythonOperator(
        task_id = 'load_fuel',
        python_callable=load_dataframe,
        op_kwargs={'table_name': 'fuels'}
    )

    load_diesel = PythonOperator(
        task_id = 'load_diesel',
        python_callable=load_dataframe,
        op_kwargs={'table_name': 'diesel'}
    )

download >> convert >> [transform_fuel,transform_diesel]
transform_fuel >> load_fuel
transform_diesel >> load_diesel