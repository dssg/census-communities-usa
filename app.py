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

#BAD MONGO URL
#NEVER DO THIS
MONGO_URL = 'mongodb://heroku:29e9df65f428be35535a2f6786508275@shannon.mongohq.com:10038/app16981083'
@app.route("/state/<state>")
def get_state(state):
	if MONGO_URL:
		conn = pymongo.Connection(MONGO_URL)
    	db = conn[urlparse(MONGO_URL).path[1:]]
    	census = db.census
    	return dumps(census.find_one())

@app.route("/year/<year>")
def get_year(year):
	return "you wanted a year"

@app.route("/get-all/<year>/<state>")
def get_year_and_state(year,state):
	return "you wanted both"

@app.route("/")
def hello():
    return "Hello World, again"

if __name__ == "__main__":
	app.run(debug=True)