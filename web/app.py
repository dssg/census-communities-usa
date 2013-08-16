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

MONGO_HOST = os.environ.get('MONGO_HOST')
MONGO_CONN = pymongo.MongoReplicaSetClient(MONGO_HOST, replicaSet='rs0')
MONGO_DB = MONGO_CONN['census']

MONGO_COLLS = {
    'od': 'origin_destination',
    'rac': 'residence_area',
    'wac': 'work_area',
}

AREAS = [
    'st_leg_upper_name',
    'st_leg_lower_name',
    'place_code',
    'county_fips',
    'census_tract_code',
    'st_leg_upper_code',
    'place_name',
    'zcta_name',
    'state_name',
    'zcta_code',
    'cong_dist_name',
    'county_name',
    'state_abrv',
    'census_tract_name',
    'st_leg_lower_code',
    'census_block_code',
    'census_block_name',
    'cong_dist_code',
]

@app.route("/<coll_name>/<geo_area>/<value>")
def query(coll_name, geo_area, value):
    if coll_name not in MONGO_COLLS.keys():
        return make_response('Not a valid collection name', 401)
    if geo_area not in AREAS:
        return make_response('Not a valid geospatial area', 401)
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
    for record in collection.find({"Residence_Census_Block_Code":block}, limit=20):
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
    app.run(debug=True)
