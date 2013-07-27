import requests
import shapefile
import os
import pymongo
import gzip
import zipfile
import csv
from datetime import datetime
from cStringIO import StringIO
from itertools import izip_longest
ENDPOINT = 'http://lehd.ces.census.gov/onthemap/LODES7'
MONGO_HOST = os.environ.get('MONGO_HOST')
if not MONGO_HOST:
    MONGO_HOST = 'localhost'
MONGO_CONN = pymongo.MongoClient(MONGO_HOST)
MONGO_DB = MONGO_CONN['census']

SEGMENTS = {
    'od': ['main', 'aux'],
    'wac': ['S000', 'SA01', 'SA02', 'SA03', 'SE01', 'SE02', 'SE03', 'SI01', 'SI02', 'SI03'],
    'rac': ['S000', 'SA01', 'SA02', 'SA03', 'SE01', 'SE02', 'SE03', 'SI01', 'SI02', 'SI03'],
}

COLLS = {
    'od': 'origin_destination',
    'rac': 'residence_area',
    'wac': 'work_area',
}

def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return izip_longest(*args, fillvalue=fillvalue)

def fetch_load_xwalk(state):
    """ 
    Only use this the first time or drop the 'geo_xwalk'
    collection before executing again
    """
    xwalk = requests.get('%s/%s/%s_xwalk.csv.gz' % (ENDPOINT, state, state))
    if xwalk.status_code != 200:
        print 'Could not find Geographic crosswalk table for %s' % state
        return None
    s = StringIO(xwalk.content)
    coll = MONGO_DB['geo_xwalk']
    with gzip.GzipFile(fileobj=s) as f:
        row_groups = grouper(csv.DictReader(f), 10000)
        for group in row_groups:
            coll.insert([r for r in group if r])
        coll.ensure_index([('stusps', pymongo.DESCENDING)])

def fetch_load_shapes(state):
    coll = MONGO_DB['geo_xwalk']
    st_fips = coll.find_one({'stusps': state.upper()})['st']
    u = 'http://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010'
    url = '%s/tl_2010_%s_tabblock10.zip' % (u, st_fips)
    req = requests.get(url)
    if req.status_code != 200:
        print 'Unable to fetch census block shape data for %s' % state.upper()
    else:
        zf = StringIO(req.content)
        shp = StringIO()
        dbf = StringIO()
        shx = StringIO()
        with zipfile.ZipFile(zf) as f:
            for name in f.namelist():
                if name.endswith('.shp'):
                    shp.write(f.read(name))
                if name.endswith('.shx'):
                    shx.write(f.read(name))
                if name.endswith('.dbf'):
                    dbf.write(f.read(name))
        shape_reader = shapefile.Reader(shp=shp, dbf=dbf, shx=shx)
        records = shape_reader.shapeRecords()
        for record in records:
            geoid = record.record[4]
            geo_xwalk = coll.find_one({'tabblk2010': geoid})['_id']
            coll.update({'_id': geo_xwalk}, {'$set': {'geojson': record.shape.__geo_interface__}})
        coll.ensure_index([('geojson', pymongo.GEOSPHERE)])

def fetch_load(year, state):
    for group in ['od', 'rac', 'wac']:
        coll = MONGO_DB[COLLS[group]]
        for segment in SEGMENTS[group]:
            for job_type in ['JT00', 'JT01', 'JT02', 'JT03', 'JT04', 'JT05']:
                u = '%s/%s/%s/%s_%s_%s_%s_%s.csv.gz' % (ENDPOINT, state, group, state, group, segment, job_type, year)
                req = requests.get(u)
                if req.status_code != 200:
                    print 'No %s data for %s in the year %s of type %s' % (group, state, year, job_type)
                    continue
                s = StringIO(req.content)
                with gzip.GzipFile(fileobj=s) as f:
                    row_groups = grouper(csv.DictReader(f), 10000)
                    for gr in row_groups:
                        rows = []
                        for row in gr:
                            if row:
                                row['createdate'] = datetime.strptime(row['createdate'], '%Y%m%d')
                                row['stusps'] = state.upper()
                                rows.append(row)
                        coll.insert(rows)

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 2:
        states = [sys.argv[1]]
    else:
        states_file = open('50state.txt', 'rb')
        states = [s[:2].lower() for s in states_file]
    for state in states:
        print 'Loading geographic crosswalk table for %s' % state.upper()
        fetch_load_xwalk(state)
        print 'Loading geometries for %s' % state.upper()
        fetch_load_shapes(state)
        for year in range(2002, 2012):
            print 'Loading data from %s for %s' % (year, state.upper())
            fetch_load(year, state)
