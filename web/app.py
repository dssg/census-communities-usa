from flask import Flask, make_response, request
import flask
import os
import json
import sys
import psycopg2
from urlparse import urlparse
from operator import itemgetter
from itertools import groupby

app = Flask(__name__)

DB_HOST = os.environ.get('DB_HOST')

from datetime import timedelta
from flask import make_response, request, current_app
from functools import update_wrapper


def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator

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
@crossdomain(origin="*")
def tract_origin_destination(tract_code):
    conn = psycopg2.connect('host=%s dbname=census user=census' % DB_HOST)
    origin_cursor = conn.cursor()
    dest_cursor = conn.cursor()
    dest_query = """select 
        substring(w_geocode from 1 for 11) as work, 
        sum(s000) as total_jobs from origin_destination 
        where h_geocode like %(like)s group by work order by total_jobs desc limit 20;"""
    dest_cursor.execute(dest_query, {'like': tract_code + '%'})
    dest_results = dest_cursor.fetchall()
    origin_query = """select 
        substring(h_geocode from 1 for 11) as home, 
        sum(s000) as total_jobs from origin_destination 
        where w_geocode like %(like)s group by home order by total_jobs desc limit 20;"""
    origin_cursor.execute(origin_query, {'like': tract_code + '%'})
    origin_results = origin_cursor.fetchall()
    results = {'traveling-to': [{d[0]: d[1]} for d in dest_results if d[1] >= 20]}
    results['traveling-from'] = [{o[0]: o[1]} for o in origin_results if o[1] >= 20]
    resp = make_response(json.dumps(results))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route('/tract-average/<tract_code>/')
def tract_average(tract_code):
    conn = psycopg2.connect('host=%s dbname=census user=census' % DB_HOST)
    cursor = conn.cursor()
    query = """SELECT 
        data_year, sum(earnings_1250_under), sum(earnings_1251_3333), sum(earnings_3333_over) 
        from work_area_detail where geocode like %(like)s group by data_year order by data_year;"""
    cursor.execute(query, {'like': tract_code + '%'})
    results = cursor.fetchall()
    out = {}
    for result in results:
        out[result[0]] = {}
        for k,v in zip(['earnings_1250_under', 'earnings_1251_3333', 'earnings_3333_over'], result[1:]):
            out[result[0]][k] = v
            out[result[0]]['total_jobs'] = sum(result[1:])
    resp = make_response(json.dumps(out))
    resp.headers['Content-Type'] = 'application/json'
    return resp

if __name__ == "__main__":
    app.run(debug=True, port=7777)
