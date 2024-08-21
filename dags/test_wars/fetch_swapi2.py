import logging
from http import HTTPStatus

import pendulum
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from airflow.decorators import dag, task
from airflow.models import Variable

from config.custom_errors import InsertError
from test_wars.models import StarWars


POSTGRES_CONN_ID = 'PostgresBi'

Session = sessionmaker(bind=create_engine(Variable.get("postgres_con")))
session = Session()

# get the airflow.task logger
task_logger = logging.getLogger('airflow.task')


@dag(
    dag_id='FetchSwapi',
    schedule='@once',
    catchup=False,
    start_date=pendulum.yesterday("Europe/Moscow"),
    max_active_runs=1,
    tags=['StarWars'],
)
def fetchswapi():
    @task
    def _fetch_swapi():
        try:
            # Получение данных из API
            response = requests.get('https://swapi.dev/api/people/')
            if response.status_code != HTTPStatus.OK:
                raise requests.exceptions.RequestException(
                    response.status_code, response.text
                )
            data = response.json()

            # Получение списка персонажей
            people = data["results"]

            # Создание списка записей для вставки
            star_wars_records = []
            for person in people:
                star_wars_records.append(
                    StarWars(
                        name=person["name"],
                        height=person["height"],
                        mass=person["mass"],
                        hair_color=person["hair_color"],
                        skin_color=person["skin_color"],
                        eye_color=person["eye_color"],
                        birth_year=person["birth_year"],
                        gender=person["gender"],
                    )
                )

            # Вставка всех записей в базу данных
            with Session() as session:
                session.add_all(star_wars_records)
                try:
                    session.commit()
                except IntegrityError as e:
                    session.rollback()
                    task_logger.info(f"Ошибка вставки: {e}")
        except Exception as error:
            raise InsertError(error)

        task_logger.info(f'Task {__name__} finished')

    _fetch_swapi()


fetchswapi()
