import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='umbrella',
    description='Umbrella example with DummyOperator',
    start_date=airflow.utils.dates.days_ago(5),
    schedule_interval='@daily',
    tags=['chapter01'],  # По тегам удобно искать нужные даги
)

fetch_weather_forecast = DummyOperator(task_id='fetch_weather_forecast', dag=dag)
fetch_sales_data = DummyOperator(task_id='fetch_sales_data', dag=dag)
clean_forecast_data = DummyOperator(task_id='clean_forecast_data', dag=dag)
clean_sales_data = DummyOperator(task_id='clean_sales_data', dag=dag)
join_datasets = DummyOperator(task_id='join_data', dag=dag)
train_ml_model = DummyOperator(task_id='train_ml_model', dag=dag)
deploy_ml_model = DummyOperator(task_id='deploy_ml_model', dag=dag)


fetch_weather_forecast >> clean_forecast_data
fetch_sales_data >> clean_sales_data
[clean_forecast_data, clean_sales_data] >> join_datasets
join_datasets >> train_ml_model >> deploy_ml_model
