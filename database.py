from sqlalchemy import create_engine
from  sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from os import environ
from dotenv import load_dotenv

load_dotenv()

# from .main import db_config

db_config = {
    "host": environ.get('DB_HOST'),
    "user":environ.get('DB_USERNAME'),
    "password": environ.get('DB_PASSWORD'),
    "database": environ.get('DB_DATABASE'),
}


SQLALCHEMY_DATABASE_URL = f'postgresql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}/{db_config["database"]}'

engine = create_engine(SQLALCHEMY_DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

