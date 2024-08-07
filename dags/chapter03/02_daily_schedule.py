import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

"""!!! Внимание сюда !!!"""
dag = DAG(
    dag_id="02_daily_schedule",
    schedule_interval="@daily",  # Ежедневный запуск в полночь
    start_date=dt.datetime(2019, 1, 1),  # дата и время запуска задачи
    tags=['chapter03'],
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data && "
        "curl -o /data/events.json "
        "https://localhost:5000/events"
    ),
    dag=dag,
)


def _calculate_stats(input_path, output_path):
    """Рассчитывает статистику по событиям"""
    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path": "/data/events.json",
        "output_path": "/data/stats.csv",
    },
    dag=dag,
)

fetch_events >> calculate_stats