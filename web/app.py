from flask import Flask, make_response, request
import flask
import os
import json
import sys
import pymongo
from pymongo.read_preferences import ReadPreference
from urlparse import urlparse
from bson import Binary, Code
from bson import json_util
from operator import itemgetter
from itertools import groupby

app = Flask(__name__)

MONGO_HOST = os.environ.get('MONGO_HOST')
MONGO_CONN = pymongo.MongoReplicaSetClient(MONGO_HOST, replicaSet='rs0')
MONGO_DB = MONGO_CONN['chi_metro']
MONGO_DB.read_preference = ReadPreference.SECONDARY_PREFERRED

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

@app.route('/tract-origin-destination/<tract_code>/')
def tract_origin_destination(tract_code):
    coll = MONGO_DB['origin_destination']
    home = [d for d in coll.find({'home_census_tract_code': tract_code})]
    work = [d for d in coll.find({'work_census_tract_code': tract_code})]
    home_sorted = sorted(home, key=itemgetter('work_census_tract_code'))
    work_sorted = sorted(work, key=itemgetter('home_census_tract_code'))
    results = {tract_code: {'traveling-from': {}, 'traveling-to': {}}}
    for k,g in groupby(home_sorted, key=itemgetter('work_census_tract_code')):
        tract_count = len(list(g))
        if tract_count >= 20:
            results[tract_code]['traveling-to'][k] = tract_count
    for k,g in groupby(work_sorted, key=itemgetter('home_census_tract_code')):
        tract_code = len(list(g))
        if tract_count >= 20:
            results[tract_code]['traveling-from'][k] = tract_count
    resp = make_response(json.dumps(results))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route('/tract-average/<tract_code>/')
def tract_average(tract_code):
    coll = MONGO_DB['origin_destination']
    # query = {'home_census_tract_code': { '$regex': '/%s.*' % tract_code}}
    query = {'home_census_tract_code': tract_code}
    results = [d for d in coll.find(query)]
    results = sorted(results, key=itemgetter('data_year'))
    res = []
    keys = request.args.get('keys',[])
    if keys:
        keys = keys.split(',')
    for k, g in groupby(results, key=itemgetter('data_year')):
        v = {tract_code: {}}
        all_vals = list(g)
        v[tract_code]['S000'] = sum([i['S000'] for i in all_vals])
        for key in keys:
            try:
                v[tract_code][key] = sum([i[key] for i in all_vals])
            except KeyError:
                return make_response('"%s" is not a valid field name' % key, 401)
        res.append({k:v})
    resp = make_response(json_util.dumps(res))
    resp.headers['Content-Type'] = 'application/json'
    return resp

if __name__ == "__main__":
    app.run(debug=True, port=7777)
