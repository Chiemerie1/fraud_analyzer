from pydantic import parse_obj_as
# from . import models, schemas
import models
import schemas
from typing import List
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi import status, Body, HTTPException
import mongo
from  bson import ObjectId
import bson
from pymongo.collection import Collection
from pymongo.database import Database


database = mongo.mongo_db



def find_company(
    name: str, db: Database = database
) -> schemas.Company | None:
    result = db["company"].find_one({"name": name})  
    
    if not (result):
        return None
    return result



def create_company_mongo(company: schemas.Company, db: Database = database): 
    company_collection: Collection = db["company"]
    found_company = find_company(company.name, company_collection)
    if found_company:
        return found_company
    
    company = jsonable_encoder(company)
    new_company = company_collection.insert_one(company)
    created_company = company_collection.find_one({"_id": new_company.inserted_id})
    created_company["_id"] = str(created_company["_id"])
    return JSONResponse(status_code=status.HTTP_201_CREATED, content=created_company)


def get_user_by_username_and_company(username: str, company_id: str, db=database):
    username = (
        db["user"].find_one({"username": username})
    )
    company_id = (
        db["user"].find_one({"company_id": company_id})
    )
    return username, company_id


def get_user_by_username(username: str, db=database):
    username = (
        db["user"].find_one({"username": username})
    )
    return username



def find_company_by_id(id: str, db=database) -> schemas.Company | None:
    try:
        company_dict = database.company.find_one(
            {
                "_id": ObjectId(id)
            }
        )
        if company_dict:
            company_dict["_id"] = str(company_dict["_id"])
            return schemas.Company(**company_dict)
        else:
            return None
    except bson.errors.InvalidId:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Company not found",
        )



def update_company(id: str, updated_company_data: schemas.Company, db=database) -> schemas.Company:
    existing_company = find_company_by_id(id)
    
    if existing_company:
        updated_company_data_dict = updated_company_data.dict()
        db.company.update_one({"_id": ObjectId(id)}, {"$set": updated_company_data_dict})
        
        return updated_company_data
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Company not found")






