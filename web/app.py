from flask import Flask,jsonify
import flask
import os
import json
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

@app.route("/<coll_name>/<geo_area>/<value>/")
def query(coll_name, geo_area, value):
    if coll_name not in MONGO_COLLS.keys():
        return make_response('Not a valid collection name', 401)
    if geo_area not in AREAS:
        return make_response('Not a valid geospatial area', 401)
    query = {}
    values = value.split('_')
    if coll_name == 'od':
        query = {'home_%s' % geo_area: values[0],  'work_%s' % geo_area: values[1]}
    elif coll_name == 'wac':
        query = {'work_%s' % geo_area: values[0]}
    elif coll_name == 'rac':
        query = {'home_%s' % geo_area: values[0]}
    coll = MONGO_DB[MONGO_COLLS[coll_name]]
    records = json.dumps([r for r in coll.find(query, limit=50)])
    resp = make_response(records)
    resp.headers['Content-Type'] = 'application/json'
    return resp

if __name__ == "__main__":
    app.run(debug=True)
