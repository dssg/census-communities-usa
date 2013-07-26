import grequests
import requests
import pymongo
import tarfile
ENDPOINT = 'http://lehd.ces.census.gov/onthemap/LODES7'
MONGO_HOST = os.environ['MONGO_HOST']

def fetch_load_xwalk(state):
    xwalk = requests.get('%s/%s_xwalk.csv.gz' % (ENDPOINT, state))


def gen_origin_destination(year, state):
    for part in ['main', 'aux']:
        for job_type in ['JT00', 'JT01', 'JT02', 'JT03', 'JT04', 'JT05']:
            yield '%s/%s_od_%s_%s_%s.csv.gz' % (ENDPOINT, state, part, job_type, year)


if __name__ == "__main__":
    states_file = open('scrapers/50state.txt', 'rb')
    states = [s[:2].lower() for s in states_file]
    for state in states:
        fetch_load_xwalk(state)
        for year in range(2002, 2012):
            od_reqs = (grequests.get(u) for u in gen_origin_destination(year, state))
            fetch_
