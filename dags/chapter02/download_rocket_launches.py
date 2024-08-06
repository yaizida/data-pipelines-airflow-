import json
import os

import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

from custom_errors import NoDataError


# get the airflow.task logger
task_logger = logging.getLogger('airflow.task')

dag = DAG(
    dag_id="download_rocket_launches",
    description="Download rocket pictures od recently launched rockets",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily"
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'", # noqa
    dag=dag
)


def _get_pictures():
    os.makedirs("/tmp/launches", exist_ok=True)

    with open("/tmp/launches.json", "r") as f:
        launches = json.load(f)

        if len(launches["results"]) > 0:
            task_logger.info("Data load")
        else:
            raise NoDataError("No data found")

        image_urls = [launch.get("image") for launch in launches["results"]]

        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"

                with open(target_file, "wb") as f:
                    f.write(response.content)
                task_logger.info(f"Downloaded {image_url} to {target_file}")

            except requests_exceptions.MissingSchema:
                task_logger.error(f"{image_url} appears to be an invalid URL.")

            except requests_exceptions.ConnectionError:
                task_logger.error(f"Could not connect to {image_url}.")


get_picture = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag
)

notify = BashOperator(
    task_id="notify",
    bash_command="echo 'There are now $(ls /tmp/images/ | wc -l) images.'",
    dag=dag
)

download_launches >> get_picture >> notify
