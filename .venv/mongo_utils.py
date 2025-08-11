from pymongo import MongoClient
from config import MONGO_URI

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["unigap"]
collection = db["project1"]
