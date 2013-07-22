from flask import Flask,jsonify
import flask
import simplejson as json
import os
import sys
import pymongo
from urlparse import urlparse
from bson import Binary, Code
from bson.json_util import dumps

app = Flask(__name__)



@app.route("/state/<state>")
def get_state(state):
	return "Not Yet Implemented"

@app.route("/year/<year>")
def get_year(year):
	return "Not Yet Implemented"

@app.route("/get-all/<year>/<state>")
def get_year_and_state(year,state):
	return "Not Yet Implemented"

@app.route("/residence-block/<int:block>")
def find_block(block):
	block_records = []
	collection = db.census
	for record in collection.find({"Residence_Census_Block_Code":block})[20]:
		block_records.append(record)
	return dumps(block_records) 

@app.route("/demo")
def demo():
	collection = db.census
	return dumps(collection.fine_one())

@app.route("/")
def hello():
    return "Hello World, again"

if __name__ == "__main__":
	mongo_url = str(os.environ.get('MONGO_URL'))
	client = pymongo.MongoClient(mongo_url,27017)
	db = client.census
	app.run(debug=True)