from pymongo import MongoClient
import csv
import os

MONGO_URL = os.environ.get('MONGOHQ_URL')

if MONGO_URL:
    # Get a connection
    conn = pymongo.Connection(MONGO_URL)
    
    # Get the database
    db = conn[urlparse(MONGO_URL).path[1:]]

print "Test sucessful"