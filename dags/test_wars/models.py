from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

TABLE_NAME = 'star_wars'


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
