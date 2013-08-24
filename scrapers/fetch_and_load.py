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
WRITE_CONN = pymongo.MongoReplicaSetClient(MONGO_HOST, replicaSet='rs0')
WRITE_DB = WRITE_CONN['chi_metro']

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

JOB_TYPES = {
    'JT00': 'all',
    'JT01': 'primary',
    'JT02': 'private',
    'JT03': 'private primary',
    'JT04': 'federal',
    'JT05': 'federal primary',
}

AREA_SEGMENTS = {
    'S000': 'all', 
    'SA01': 'under 29',
    'SA02': '30 to 54', 
    'SA03': 'over 55',
    'SE01': '$1250/month or less',
    'SE02': '$1251-$3333/month',
    'SE03': 'more than $3333/month',
    'SI01': 'Goods Producing industry sectors',
    'SI02': 'Trade, Transportation, and Utilities industry sectors',
    'SI03': 'All Other Services industry sectors',
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
    coll = WRITE_DB['geo_xwalk']
    with gzip.GzipFile(fileobj=s) as f:
        row_groups = grouper(csv.DictReader(f, encoding="latin-1"), 10000)
        for group in row_groups:
            coll.insert([r for r in group if r])
        coll.ensure_index([('stusps', pymongo.DESCENDING)])
        coll.ensure_index([('cty', pymongo.DESCENDING)])
        coll.ensure_index([('tabblk2010', pymongo.DESCENDING)])

def make_indexes(group, coll, row):
    if group == 'od':
        home_work_fields = [f[5:] for f in row.keys() if f.startswith('home') or f.startswith('work')]
        for field in home_work_fields:
            if 'code' in field:
                coll.ensure_index([
                  ('home_%s' % field, pymongo.DESCENDING),
                  ('work_%s' % field, pymongo.DESCENDING)
                ])
    else:
        for field in row.keys():
            if field.startswith('home') or field.startswith('work') and 'code' in field:
                coll.ensure_index([(field, pymongo.DESCENDING)])

def iter_parts(year, state, **kwargs):
    groups = kwargs.get('groups')
    if 'all' in groups:
        groups = ['od', 'rac', 'wac']
    job_types = kwargs.get('job_types')
    if 'all' in job_types:
        job_types = JOB_TYPES.keys()
    for group in groups:
        coll = WRITE_DB[COLLS[group]]
        save_path = os.path.join(os.environ['HOME'], 'data', state, group)
        try:
            os.makedirs(save_path)
        except os.error:
            pass
        segments = kwargs.get('segments')
        if 'all' in segments:
            segments = SEGMENTS[group]
        for segment in segments:
            for job_type in job_types:
                state = state.lower()
                fname = '%s_%s_%s_%s_%s.csv.gz' % (state, group, segment, job_type, year)
                full_path = os.path.join(save_path, fname)
                url = '%s/%s/%s/%s' % (ENDPOINT, state, group, fname)
                yield full_path, url

def fetch(full_path, url):
    if os.path.exists(full_path):
        return 'Already fetched file: %s' % os.path.basename(full_path)
    else:
        try:
            req = requests.get(url)
        except ConnectionError:
            return 'Was unable to load %s' % url
        if req.status_code != 200:
            return 'Got a %s when trying to fetch %s' % (req.status_code, url)
        f = open(full_path, 'wb')
        f.write(req.content)
        f.close()
        return 'Saved %s' % os.path.basename(full_path)

def load(full_path):
    if not os.path.exists(full_path):
        return 'Have not yet downloaded file: %s' % os.path.basename(full_path)
    else:
        state, group, segment, job_type, year = os.path.basename(full_path)[:-7].split('_')
        coll = WRITE_DB[COLLS[group]]
        with gzip.GzipFile(full_path) as f:
            row_groups = grouper(csv.DictReader(f), 20000)
            for gr in row_groups:
                rows = []
                for row in gr:
                    if row:
                        row['createdate'] = datetime.strptime(row['createdate'], '%Y%m%d')
                        if group !='od':
                            row['segment_code'] = segment
                            row['segment_name'] = AREA_SEGMENTS[segment]
                        row['main_state'] = state.upper()
                        for key in row.keys():
                            if 'geocode' not in key:
                                try:
                                    row[key] = int(row[key])
                                except ValueError:
                                    pass
                                except TypeError:
                                    pass
                        row['data_year'] = year
                        row['job_type'] = JOB_TYPES[job_type]
                        if group in ['od', 'rac']:
                            home_geo = row['h_geocode']
                            row['home_census_bgrp_code'] =  home_geo[:-2]
                            row['home_census_tract_code'] = home_geo[:-3]
                            row['home_county_code'] =       home_geo[:5]
                        if group in ['od', 'wac']:
                            work_geo = row['w_geocode']
                            row['work_census_bgrp_code'] =  work_geo[:-2]
                            row['work_census_tract_code'] = work_geo[:-3]
                            row['work_county_code'] =       work_geo[:5]
                        counties = [
                            '17031', 
                            '17111', 
                            '17037',
                            '17063',
                            '17093',
                            '17197',
                            '18127',
                            '17097', 
                            '17043', 
                            '18111', 
                            '17089', 
                            '18073', 
                            '18089', 
                            '55059'
                        ]
                        home_county = row.get('home_county_code')
                        work_county = row.get('work_county_code')
                        if home_county in counties or work_county in counties:
                            rows.append(row)
                if rows:
                    coll.insert(rows)
        return 'Successfully loaded %s' % os.path.basename(full_path)

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
    parser.add_argument('--skip_geo', action='store_true',
        help="""
            Skips importing the geographic crosswalk info.
        """)
    parser.add_argument('--fetch', action='store_true',
        help="""
            Fetch selected files from Census Bureau.
        """)
    parser.add_argument('--load', action='store_true',
        help="""
            Load selected files from Census Bureau.
        """)
    args = parser.parse_args()
    states_file = open('50state.txt', 'rb')
    states_list = [s[:2] for s in states_file]
    states = args.states.split(',')
    if 'all' in states:
        states = states_list
    if not set(states).issubset(set(states_list)):
        print 'The list of states you provided included an invalid value: %s' % args.states
        sys.exit()
    years = args.years.split(',')
    if 'all' in years:
        years = range(2002, 2012)
    kwargs = {
        'groups': args.files.split(','),
        'job_types': args.job_types.split(','),
        'segments': args.segments.split(',')
    }
    for state in states:
        if not args.skip_geo:
            print 'Loading geographic crosswalk table for %s' % state.upper()
            fetch_load_xwalk(state.lower())
        else:
            print 'Skipping geographic crosswalk table for %s' % state.upper()
        for year in years:
            for full_path, url in iter_parts(year, state, **kwargs):
                if args.fetch:
                    print fetch(full_path, url)
                if args.load:
                    print load(full_path)
