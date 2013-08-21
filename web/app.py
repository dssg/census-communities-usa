from flask import Flask, make_response, request
import flask
import os
import json
import sys
import pymongo
from urlparse import urlparse
from bson import Binary, Code
from bson import json_util
from operator import itemgetter
from itertools import groupby

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
        if len(values) < 2:
            return make_response('You need to supply two values separated by an underscore to query the OD table', 401)
        else:
            query = {'home_%s' % geo_area: values[0],  'work_%s' % geo_area: values[1]}
    elif coll_name == 'wac':
        query = {'work_%s' % geo_area: values[0]}
    elif coll_name == 'rac':
        query = {'home_%s' % geo_area: values[0]}
    coll = MONGO_DB[MONGO_COLLS[coll_name]]
    limit = request.args.get('limit', 50)
    records = json_util.dumps([r for r in coll.find(query, limit=int(limit))])
    resp = make_response(records)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route('/tract-average/<tract_code>/')
def tract_average(tract_code):
    coll = MONGO_DB['residence_area']
    query = {'home_census_tract_code': tract_code}
    results = [d for d in coll.find(query, limit=50)]
    results = sorted(results, key=itemgetter('data_year'))
    res = []
    for k, g in groupby(results, key=itemgetter('data_year')):
        v = {tract_code: {}}
        for item in group:
            v[tract_code]['SE01'] = sum([i['SE01'] for i in group]) * 1250
            v[tract_code]['SE02'] = sum([i['SE02'] for i in group]) * 2083
            v[tract_code]['SE03'] = sum([i['SE03'] for i in group]) * 3333
            v[tract_code]['S000'] = sum([i['S000'] for i in group])
        res.append({k:v})
    resp = make_response(json_util.dumps(res))
    resp.headers['Content-Type'] = 'application/json'
    return resp

if __name__ == "__main__":
    app.run(debug=True, port=7777)
