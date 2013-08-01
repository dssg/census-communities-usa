import requests
import os
import pymongo
import gzip
import unicodecsv as csv
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
        row_groups = grouper(csv.DictReader(f, encoding="latin-1"), 10000)
        for group in row_groups:
            coll.insert([r for r in group if r])
        coll.ensure_index([('stusps', pymongo.DESCENDING)])

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
        for year in range(2002, 2012):
            print 'Loading data from %s for %s' % (year, state.upper())
            fetch_load(year, state)
