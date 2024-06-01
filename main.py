from fastapi import FastAPI, HTTPException
import pandas as pd
from warnings import filterwarnings
filterwarnings("ignore")
from typing import List

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient

# from . import models, schemas, crud
import models
import schemas
import crud
import database
import tags
import rules

import mongo
from bson import ObjectId
import pymongo

description = "Fraud Analyzer and report generation."

##### Initializing the app #

analyzer = FastAPI(
        title="Fraud Analyzer",
        description=description,
        summary="Fraud Analyzer",
        version="0.1.1",
        openapi_tags=tags.tags_metadata
    )
        

# Database Dependency pymongo

conn_url = 'mongodb://localhost:27017/'

def get_mongo_client() -> pymongo.MongoClient:
    client = MongoClient(conn_url)
    try:
        client.admin.command("ping")
        return client
    except Exception:
        client.close()
    
##### defining auth checks #


database = mongo.mongo_db




##### Getting from real database

@analyzer.post("/companies/create-company", response_model=schemas.Company)
def create_company(company: schemas.Company):
    company = crud.create_company_mongo(company)
    return company



@analyzer.get("/companies", response_model=List[schemas.Company])
async def get_all_companies(limit: int):
    companies = list(database["company"].find().limit(limit))
    return companies



@analyzer.get("/companies/{id}", response_model=schemas.Company)
def get_company(id: str):
    company = crud.find_company_by_id(id)
    return company



@analyzer.put("/companies/{id}", response_model=schemas.Company)
def update_company_route(id: str, updated_company: schemas.Company):
    updated_company = crud.update_company(id, updated_company)
    return updated_company



@analyzer.post("/analyze/{authorization}", response_description="Add new transaction")
async def analyze_transaction(
    payload: schemas.CustomerTransaction,
    authorization: str,
    ):
    
    ##### Read the company data from the mongo DB
    company = crud.find_company_by_id(authorization)
    # print(company)   ############
    if not company:
        raise create_http_exception("invalid company ID")
    
    customer_tier = company.configuration.get_tier(payload.customer_tier)
    print(f'Customer tier: {customer_tier}')
  
    if not customer_tier:
        raise create_http_exception("Invalid customer tier")

    trans_type_config = customer_tier.get_trans_type_config(payload.type.upper())

    if not(trans_type_config):
        raise create_http_exception("Invalid transaction type")

    input_df = pd.DataFrame({
        "hash": [payload.hash],
        "type": [payload.type.upper()],
        "amount": [payload.amount],
        "customer_tier": [payload.customer_tier],
        "customer_unique_id": [payload.customer_unique_id],
        "transaction_time": [payload.transaction_time],
        'company_id': [payload.company_id]
    })
    
    
    transactions = mongo.mongo_db.transactions

    new_df = rules.attach_day_and_hour(input_df, "transaction_time")
    new_df = new_df.to_dict(orient="records")
    created_company = transactions.insert_many(new_df)
    
    
    getMappedResults = lambda x: {"rule": x, "score": rules.apply_rule(x, payload, input_df, company)}
    results = list( map(getMappedResults, trans_type_config.rules))
    print(results)
    getScores = lambda x : x["score"]
    return {"risk_score": sum([getScores(x) for x in results])/len(trans_type_config.rules), "results": results}



def create_http_exception(msg: str, status_code: int = 400):
    return  HTTPException(
            status_code=status_code,
            detail= { "message": msg},
            headers={"content-type": "application/json"}
        )
    
    
