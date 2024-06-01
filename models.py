import json
from sqlalchemy import ForeignKey, Boolean, Column, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
import database
# from .database import Base

Base = database.Base

class UserModel(Base):
    __tablename__ = "user"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True)
    hashed_password = Column(String)
    company_id = Column(String,unique=True)
    is_active = Column(Boolean, default=True)
    


class CompanyModel(Base):

    __tablename__ = "companies"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)
    configuration =  Column(JSONB)
    ulid = Column(String, unique=True)

    
    
    

