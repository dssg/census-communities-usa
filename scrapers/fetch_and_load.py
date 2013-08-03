import requests
from requests.exceptions import ConnectionError
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

def fetch_load(year, state, attempts=0, **kwargs):
    groups = ['od', 'rac', 'wac']
    job_types = ['JT00', 'JT01', 'JT02', 'JT03', 'JT04', 'JT05']
    if kwargs.get('groups') and if 'all' not in kwargs.get('group'):
        groups = kwargs.get('groups')
    if kwargs.get('job_types') and if 'all' not in kwargs.get('job_types'):
        job_types = kwargs.get('job_types')
    for group in groups:
        coll = MONGO_DB[COLLS[group]]
        if not kwargs.get('segments') or if 'all' in kwargs.get('segments'):
            segments = SEGMENTS[group]
        else:
            segments = kwargs.get('segments')
        for segment in segments:
            for job_type in job_types:
                state = state.lower()
                u = '%s/%s/%s/%s_%s_%s_%s_%s.csv.gz' % (ENDPOINT, state, group, state, group, segment, job_type, year)
                try:
                    req = requests.get(u)
                except ConnectionError:
                    return 'Was unable to load %s' % u
                if req.status_code != 200:
                    print 'No %s data for segment %s in %s in the year %s of type %s' % (group, segmenet, state, year, job_type)
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
                print 'Successfully loaded %s' % u

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--states', type=str, required=True,
        help="""
            Comma separated list of two letter USPS abbreviation for the state 
            you want to load the data for. Provide 'all' to load all states""")
    parser.add_argument('--files', type=str, required=True,
        help="""
            Comma separated list of files you would like to load. Valid values include:
            'od', 'rac','wac' and geo_xwalk. Provide 'all' to load all four.
        """)
    parser.add_argument('--segments', type=str, required=True,
        help="""
          Comma separated list of workforce segments you want to load. In the 
          case of Origin Destination data, this translates into the part (either 'main' or 'aux').
          Provide 'all' to load all segments.
          """)
    parser.add_argument('--job_types', type=str, required=True,
        help="""
            Comma separated list of job types you would like to load.
            Provie 'all' to load all job types.
        """)
    parser.add_argument('--years', type=str, required=True,
        help="""
            Comma separated list of years you would like to load.
            Provie 'all' to load all years between 2002 and 2011.
        """)
    args = parser.parse_args()
    states_file = open('50state.txt', 'rb')
    states_list = [s[:2] for s in states_file]
    states = args.states.split(',')
    if 'all' in states:
        states = states_list
    if not set(states).issubset(set(states_list)):
        return 'The list of states you provided included an invalid value: %s' % args.states
    years = args.years.split(',')
    if 'all' in years:
        years = range(2002, 2012)
    kwargs = {
        'groups': args.files.split(','),
        'job_types': args.job_types.split(','),
        'segments': args.segments.split(',')
    }
    for state in states:
        print 'Loading geographic crosswalk table for %s' % state.upper()
        fetch_load_xwalk(state)
        for year in years:
            print 'Loading data from %s for %s' % (year, state.upper())
            fetch_load(year, state)
