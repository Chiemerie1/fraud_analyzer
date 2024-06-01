from pymongo import MongoClient


# username = "mongo_fraud"
# password = "nansernanser"
# host = "localhost"
# port = 27017
database = "fraud_analyzer"

conn_url = f'mongodb://localhost:27017/'

client = MongoClient(conn_url)

mongo_db = client[database]