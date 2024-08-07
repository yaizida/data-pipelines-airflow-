from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

# import the logging module
import logging

# get the airflow.task logger
task_logger = logging.getLogger('airflow.task')


@task.python
def extract():

    # with default airflow logging settings, DEBUG logs are ignored
    task_logger.debug('This log is at the level of DEBUG')

    # each of these lines produces a log statement
    print('This log is created via a print statement')
    task_logger.info('This log is informational')
    task_logger.warning('This log is a warning')
    task_logger.error('This log shows an error!')
    task_logger.critical('This log shows a critical error!')

    data = {'a': 19, 'b': 23, 'c': 42}

    # Using the Task flow API to push to XCom by returning a value
    return data


# logs outside of tasks will not be processed
task_logger.warning('This log will not show up!')


with DAG(dag_id='more_logs_dag',
         start_date=datetime(2022, 6, 5),
         schedule_interval='@daily',
         dagrun_timeout=timedelta(minutes=10),
         catchup=False) as dag:

    # command to create a file and write the data from the extract task into it
    # these commands use Jinja templating within {{}}
    commands = """
        touch /usr/local/airflow/{{ds}}.txt
        echo {{ti.xcom_pull(task_ids='extract')}} > /usr/local/airflow/{{ds}}.txt
        """ # noqa

    write_to_file = BashOperator(task_id='write_to_file', bash_command=commands) # noqa

    # logs outside of tasks will not be processed
    task_logger.warning('This log will not show up!')

    extract() >> write_to_file
