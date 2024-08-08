import logging
from http import HTTPStatus

import pendulum
import requests
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from airflow.decorators import dag, task
from airflow.models import Variable

from custom_errors import InsertError


POSTGRES_CONN_ID = 'PostgresBi'
TABLE_NAME = 'star_wars'

Session = sessionmaker(bind=create_engine(Variable.get("postgres_con")))
session = Session()

# get the airflow.task logger
task_logger = logging.getLogger('airflow.task')

Base = declarative_base()


class StarWars(Base):

    __tablename__ = TABLE_NAME

    __table_args__ = {'schema': 'test'}

    name = Column(String, primary_key=True)
    height = Column(Integer)
    mass = Column(Integer)
    hair_color = Column(String)
    skin_color = Column(String)
    eye_color = Column(String)
    birth_year = Column(String, primary_key=True)
    gender = Column(String, nullable=False)


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

        # Получение данных из API (например, SWAPI)
        response = requests.get('https://swapi.dev/api/people/5/')

        if response.status_code != HTTPStatus.OK:
            raise requests.exceptions.RequestException(response.status_code,
                                                       response.text)

        data = response.json()

        # Создание экземпляра модели StarWars
        star_wars_record = StarWars(
            name=data['name'],
            height=data['height'],
            mass=data['mass'],
            hair_color=data['hair_color'],
            skin_color=data['skin_color'],
            eye_color=data['eye_color'],
            birth_year=data['birth_year'],
            gender=data['gender']
        )
        try:
            # Добавление записи в сессию
            session.add(star_wars_record)

            # Сохранение изменений в базе данных
            session.commit()

            # Закрытие сессии
            session.close()
        except Exception as error:
            raise InsertError(error)

        task_logger.info(f'Task {__name__} finished')

    _fetch_swapi()


fetchswapi()
